# Tachyon architecture

How the pieces fit, and why they are shaped the way they are. Read
[`api.md`](api.md) first if you only want to use Tachyon.

## Layers

```text
          ┌─────────────────────────────────────────┐
          │ tachyon-server   REST, auth, analytics  │
          └───────────────────┬─────────────────────┘
                              │
          ┌───────────────────▼─────────────────────┐
          │ tachyon-engine   collections, recovery  │
          └────────┬────────────────────┬───────────┘
                   │                    │
   ┌───────────────▼──────┐   ┌─────────▼───────────┐
   │ tachyon-query        │   │ tachyon-storage     │
   │ parse, plan, rank    │   │ WAL, layout, meta   │
   └───────────────┬──────┘   └─────────┬───────────┘
                   │                    │
          ┌────────▼────────────────────▼───────────┐
          │ tachyon-index   tokenizer, postings,    │
          │                 columns, fuzzy matching │
          └───────────────────┬─────────────────────┘
                              │
          ┌───────────────────▼─────────────────────┐
          │ tachyon-core   schema, values, errors   │
          └─────────────────────────────────────────┘
```

Dependencies only point downwards. `tachyon-core` touches no I/O and holds no
state; `tachyon-storage` knows nothing about posting lists; `tachyon-query`
opens no files.

## The write path

PRD §10, and the order matters:

1. **Validate** the document against the schema — outside every lock, because
   parsing is the expensive part and needs nothing but the schema.
2. **Append** to the write-ahead log, and fsync according to the sync policy.
3. **Apply** to the memtable. This step is infallible by construction: every
   fallible check happened in step 1, so a write that reached the log always
   reaches memory.
4. **Acknowledge.**

A batch is one `write` syscall and one fsync regardless of size, because the
fsync is what a bulk load is really paying for.

## Durability and recovery

`state.json` is the commit point. It records which segments are committed and
the WAL sequence number up to which mutations are captured in them. Writing it
is a temp-file-plus-rename, so a reader sees the old contents or the new ones
and never a half-written file.

On open, every WAL record beyond `applied_seq` is replayed in order. Doc ids
are handed out sequentially from `state.next_doc_id`, so replay reconstructs
exactly the ids the previous process had assigned — which is what makes the
tombstone bitmap meaningful across a restart.

Three crash points, and why each is safe:

| Crash | Outcome |
|---|---|
| After the WAL append, before the flush | Replay rebuilds the memtable |
| Part-way through writing a segment | `state.json` still points at the old set; the partial files are ignored |
| After `state.json` is renamed, before the WAL is truncated | Replay skips records at or below `applied_seq` |

A crash mid-append leaves a torn frame. Each frame carries a length and a
CRC32; replay stops at the first frame that is short or fails its checksum and
truncates the file there. A frame that was never fully written was also never
acknowledged, so no client was told it was durable.

The WAL payload is JSON. It costs a few bytes against a binary encoding, but
the log is bounded by the flush threshold, the cost is dwarfed by the fsync it
sits behind, and a log an operator can read with `strings` is worth a lot
during an incident.

## Index structures

**Inverted index.** `term → field → [(doc_id, positions)]`. Grouping by field
before document makes per-field document frequency — the `df` BM25 needs — a
`len()` call rather than a scan. The term dictionary is a `BTreeMap` rather
than a hash map: lookup is barely slower at these sizes, and the ordering gives
prefix scans for autocomplete for free.

Postings are never deleted. A deleted document stays in the lists and is
skipped at query time against the live set, which keeps deletes O(1) instead of
O(terms in the document). The space is reclaimed when the memtable is flushed.

**Numeric columns.** A range filter wants a sorted `(value, doc_id)` array so
it can binary-search both ends. But the memtable is written to constantly and
re-sorting per insert would be quadratic, so a column is a large sorted region
plus a small unsorted tail. Inserts push onto the tail; when it reaches 4096
entries it is sorted and merged in one linear pass. Queries read both. O(log n)
lookups, O(1) amortized inserts.

Integers are stored as `i64`, not `f64`. Storing everything as a float would
silently round values beyond 2^53, which for a database is the kind of bug that
surfaces years later in someone's id column.

**Keyword columns.** `value → roaring bitmap of documents`. The same structure
answers an equality filter (return the bitmap) and a facet count (its
cardinality).

## Segments

A flush turns a memtable into five immutable files — `<id>.terms`, `<id>.ids`,
`<id>.post`, `<id>.col`, `<id>.doc` — under `segments/`, one file per
structure rather than one shared blob, so postings, columns, and the document
store can each use whatever layout suits them without agreeing on a common
framing. `.terms` and `.ids` are `fst::Map`s (term → dense id, and user id →
doc id), streamed directly through `fst::MapBuilder` as terms/ids are
visited in sorted order. `.post`, `.col`, and `.doc` each open with a 12-byte
header (a magic string and the format version) and close with a small
footer — an offset table, essentially — whose own position is named by the
file's last 8 bytes. The footer sits at the *end* rather than the front on
purpose: a writer can then stream payload bytes forward as it produces them
and only needs the directory's contents, never its file position, once
everything before it is already written — which is what lets both a flush
and a merge (see "Merges" below) write a segment in bounded memory, without
ever holding the whole thing assembled first. Nothing about *reading* a
segment cares where the footer sits; mmap makes every byte in the file
equally cheap to reach regardless.

Every file is mmap'd, and every read decodes only what it was asked for:

- **`.post`**: the footer holds per-field corpus stats and a byte offset per
  term. A query resolves a term through `.terms`' FST to a dense id, then
  decodes just that one term's block — nothing else in the file is touched.
  `doc_freq`/`live_doc_freq` go further still: they only ever need a document
  count or a liveness check, never positions, so they walk the block skipping
  position arrays instead of decoding and allocating them — the difference
  matters because autocomplete calls both across every candidate term and
  every source.
- **`.col`**: the footer is a small per-field directory (tag, offset,
  length). A query decodes one field's column — reusing
  `NumericColumn`/`KeywordColumn` verbatim via `from_sorted`/`from_parts` —
  never the whole file.
- **`.doc`**: field lengths and per-field values sit in dense, fixed-width
  arrays indexed directly by `doc_id`, so BM25's `|d|` and a numeric/bool
  field's value cost a bounds-checked read at a computed offset — no decode
  step, no allocation. Text and array values, and the document's source JSON,
  are variable-length and read on demand through a small offset directory;
  the source in particular is parsed only when a matched document actually
  becomes a returned hit, not for every document a query scores.

`IndexSource`'s methods reflect this: `postings`, `numeric_column`,
`keyword_column`, and `value` all return `Cow<'_, T>` rather than `&T`. The
memtable returns `Cow::Borrowed` — its data already lives in memory in the
shape callers want, so nothing about its cost changes. A segment returns
`Cow::Owned`, built fresh from the mapped bytes on every call. That's the
whole trade: a segment pays a small decode per query instead of holding
everything decoded at rest, which is what keeps a segment's resident memory
bounded by how much of it is queried rather than by its size on disk.

Segments still win on two fronts a memtable can't, independent of laziness:
the files survive a restart, so replay is bounded to the WAL generation
opened after the last flush rather than the whole log, and a flush holds
nothing for a document deleted before it — the memtable's own postings and
columns are never pruned until then.

A flush also has to tell apart two reasons a doc id can be missing from a
segment, and conflating them would either resurrect a deleted document or
silently drop a live one:

- **Never written.** A document deleted from the memtable before ever being
  flushed leaves nothing behind — no postings, no column entries, no doc
  record. A segment-local presence bitmap records exactly which ids in its
  `[min_doc_id, max_doc_id]` range were live at flush time, and
  `IndexSource::is_live` answers from that alone.
- **Deleted after commit.** A document deleted, or superseded by an upsert,
  once it already lives in a segment is tombstoned in the collection-wide
  `deleted` bitmap — the same one `state.json` persists — which the executor
  checks separately from `is_live` (see "Reading" below). A segment reader
  never sees these; it only ever answers "was this id written here."

Flushing runs under the same write-lock acquisition as the mutation that
triggered it, start to finish. Splitting it into separate acquisitions would
let a concurrent write's WAL record land at or below the `applied_seq` this
flush is about to commit — silently excluded from every future replay despite
never having reached a segment. Segment files are written before `state.json`
is replaced, so a crash between the two leaves orphaned files at an id nothing
references; `Collection::open` never scans the `segments/` directory, only the
ids `state.json` names, so a later flush reusing that id simply overwrites
them.

## Reading

A search runs over an ordered list of sources — the memtable, then every
committed segment — behind one trait, `IndexSource`, so the executor never
branches on which it is holding. Corpus statistics are summed across sources;
per-source statistics would score the same document differently depending on
which segment it happened to land in.

### The accumulator is flat

A broad query matches a large fraction of the corpus and every match must be
scored. The obvious structure — a map from doc id to a struct holding a vector
per field per token — allocates several times per matched document, and at a
hundred thousand matches that allocation traffic *is* the query.

So the evidence lives in flat vectors addressed arithmetically by
`(slot, field, token)`. A newly matched document appends one block; nothing
else allocates. On a million-document corpus with queries matching 6% of it,
this took p95 from 229 ms to 68 ms.

Two smaller wins in the same place: positions are only collected when something
will read them (proximity needs at least two tokens; a single-token query does
not), and the accumulator's index hashes doc ids with a multiply-xor rather
than SipHash — these are ids we assigned ourselves, not attacker-chosen keys.

### Ranking

Five signals, each normalized to `[0, 1]` before being mixed, because the PRD's
weights only mean something on a shared scale:

```text
score = 0.45·BM25 + 0.25·field_boost + 0.15·proximity + 0.10·typo_penalty + 0.05·popularity
```

BM25 and popularity are unbounded, so they are squashed by `x / (x + half)`.
That is monotonic, has no ceiling to clip against, and — unlike dividing by the
best score in the result set — does not make one document's score depend on
which other documents happened to match.

A document is scored on its **best** field, not the sum of all of them. Summing
double-counts text appearing in both `title` and `description` and lets a long,
repetitive field outrank a precise title match. The best field also supplies
the proximity and typo signals, so all five components describe the same match
rather than a blend of unrelated ones.

### Typo tolerance

Unrestricted Damerau-Levenshtein — insertion, deletion, substitution, and
transposition of characters that need not be adjacent in the original. The
cheaper "optimal string alignment" variant refuses to edit the same region
twice, which makes `cadefghi` → `abcdefghi` cost 3 instead of 2; at the two
edits the typo table permits, that difference is reachable, so the real
algorithm is what is implemented.

Matching a token against a dictionary means running it against many candidates,
so the matcher keeps its scratch buffers across calls and rejects in cost
order: a length gap larger than the budget first (no allocation), then a row
minimum over budget (abandon mid-matrix). Most terms die on the length check.

## Concurrency

Each collection is an `RwLock` over its mutable state. Searches take the read
lock and run concurrently with each other; a write normally holds the write
lock only for the WAL append and the memtable update, except when it also
triggers a flush — that runs to completion under the same acquisition (see
"Segments"), so the occasional write that crosses the memtable threshold
blocks readers for the length of a segment encode and write rather than a WAL
append. A flush that goes on to trigger a merge does *not* extend that
hold any further — see "Merges" below for why, and for what still does.
This is deliberately simple, and a large flush's own lock hold is the
obvious place to look next if write throughput under concurrent search
becomes the bottleneck — an atomically-swapped read snapshot, or streaming
the flush itself the way a merge now streams, would be the next step.

## Merges

A search fans out across the memtable plus every committed segment, and
each segment pays its own decode independently — so segment count, not
corpus size alone, drives query latency up over a collection's life. A merge
folds several small segments into one, keeping that count from growing
without bound: once a collection holds more than `merge_trigger_segments`
segments (default 8), the smallest `merge_fan_in` by document count
(default 4, size-tiered in spirit) are folded into one.

**A merge renumbers documents, it does not preserve their ids.** A segment's
doc-id range is fixed for its life (see "Segments" above), so folding two
already-committed segments together while keeping their original ids would
mean merging two already-*encoded* structures anyway, just without the
freedom to renumber — old ids would have to be preserved gap-for-gap. Since
renumbering was going to happen either way, `tachyon_index::merge_segments`
(`crates/tachyon-index/src/segment/merge.rs`) does the direct merge instead:
a streaming k-way union of the victims' term dictionaries (`fst::map::OpBuilder`)
and postings, columns, and doc-store rows, with every surviving id remapped
as it goes (see the module's own doc comment for the exact rank-based
remapping). Nothing here re-parses a document's source or re-tokenizes its
text — that work already happened once, when each victim was first flushed.
A `Null`/`Bool`/`Int`/`Float` value slot is copied as 13 raw bytes with no
decode at all; a `Str`/`Array` slot's blob bytes and a document's stored
source JSON are copied verbatim, with only offsets rebased to the new
file's positions. Only postings are actually decoded, and only because
block boundaries genuinely shift once dead documents drop out and ids
change.

Memory stays bounded by what one term-field's postings, one field's column,
or one document's blob bytes need — never by corpus size or by
`merge_fan_in × segment size`. Measured directly on a 1M-document
collection (four 250k-document segments, `merge_fan_in=4`): each merge's own
RSS delta averaged **~93 MiB**, peaking at **~110 MiB** — a genuine,
sustained reduction from the pre-streaming design (which held every victim's
worth of live documents decoded in memory at once), not just a shorter spike.
`--merge-fan-in` still trades frequency against average merge cost the same
way it always did; it no longer trades against a memory ceiling that scaled
with the merge itself.

### Running off the write lock

`Collection::run_merge` (`crates/tachyon-engine/src/collection.rs`) splits a
merge into three stages, and only the first and third hold `inner`'s write
lock:

1. **Snapshot** (locked). Pick victims, clone their `Arc<SegmentReader>`s,
   and compute each one's `live` bitmap (its presence minus the collection's
   current tombstones). Reserve everything a concurrent write could
   otherwise race with, right here: the output doc id range, via
   `MemTable::reserve` on the active memtable, and the output segment id,
   by bumping `state.next_segment_id`. Both reservations are simple
   monotonic-counter bumps, so a concurrent flush or another merge attempt
   can never claim the same range or id — see the two doc comments in
   `snapshot_merge_locked` for exactly how each one stays safe across any
   number of intervening flushes.
2. **Build** (unlocked). `tachyon_index::merge_segments` streams the merge
   into five new segment files, using only what the snapshot captured.
   This is the expensive part — the whole reason for the three-way split —
   and it runs with searches and writes proceeding normally.
3. **Swap** (locked). Commit: re-validate against whatever changed while
   the build ran without the lock, then write `state.json` and publish the
   new segment.

**What "whatever changed" means in practice**, and why the swap stage has
to re-derive rather than trust the snapshot:

- A **flush** may have committed a new segment, or advanced
  `state.next_doc_id`, while the build ran. The swap stage clones `inner
  .state` *fresh*, not the clone the snapshot would have taken, so none of
  that is silently discarded, and it takes `next_doc_id` as
  `max(current, merge_base + claimed)` rather than assigning it outright —
  a plain assignment could regress a value a concurrent flush had already
  advanced further.
- A **delete, or an upsert superseding an old copy**, may have landed on a
  document the snapshot already considered live and copied into the merge
  output. That document's old tombstone needs to reappear under its *new*
  id: the swap stage intersects each victim's snapshotted `live` bitmap
  against the tombstones that have landed since, then reproduces
  `merge_segments`'s own rank-based remap for each one that matches.
- A victim segment's own tombstones, once it's retired, are pruned from
  the collection-wide tombstone set by that segment's **presence bitmap**,
  not by its declared `[min_doc_id, max_doc_id]` range. The two used to be
  interchangeable, but off-lock merging broke that: a memtable can now
  have live documents both *before and after* a hole a merge reserved on
  it (`MemTable::reserve`, mid-write rather than always on a guaranteed-
  fresh memtable the way the single-lock design's timing happened to
  guarantee), so the segment that memtable eventually flushes into can
  have a declared range wider than what it actually holds — wide enough to
  numerically overlap an unrelated segment's range. Pruning by that range
  would delete a tombstone that belongs to the *other* segment, silently
  resurrecting whatever document it was protecting. Pruning by presence
  is exact regardless, since a doc id is never reused and so is only ever
  set in the one segment that actually owns it.

At most one merge runs at a time, enforced by `merge_gate`, a plain
`Mutex<()>` outside `inner`'s own lock — an explicit `Collection::merge()`
call blocks on it (a caller reaching for that method wants a merge to have
happened by the time it returns); the automatic post-write check,
`maybe_merge`, uses `try_lock` and simply skips if one is already running,
trusting the next write to check again.

Measured on a 1M-document run (default `merge_trigger_segments`/
`merge_fan_in`, background searches running the whole time): worst-case
concurrent search latency during indexing fell from 3.7 s (before any of
this project) to **233 ms** — an 8× reduction — with what's left dominated
by a large flush's own lock hold (see "Concurrency" above), not by
merging, which no longer holds the lock for anything but a snapshot and a
commit.

## Score-bound pruning

Two independent pruning mechanisms compose on every search: one that never
changes what a query reports, and one that does.

### Tail pruning (exact)

Every matching document is visited and counted — `found`, `matched`, and
facets are always exact under this mechanism alone. What gets skipped, for
documents a cheap upper bound proves cannot reach the final page, is the
*expensive part* of per-document scoring — proximity, the popularity read,
best-field `combine()`. The bound is real BM25 (exact, already known once a
document is resolved) plus the maximum possible value of the other four
`combine()` signals, which are non-negative-weighted and `[0,1]`-normalized,
so the sum is a sound ceiling. This mechanism alone was measured, before true
WAND existed, at a modest win (5M docs, ~9%-broad query: p95 494 ms → 454 ms)
precisely because it never reduces how many documents get visited in the
first place — see below for what does.

### True block-max WAND (postings-level, approximate)

Classical block-max WAND skips documents entirely, jumping ahead in sorted
posting lists using per-block score bounds — incompatible with the exact
counts the mechanism above preserves. This is now built as a genuinely
separate, composed layer: `.post`'s format groups a term's postings into
fixed 128-document blocks, each carrying its own max doc id, max term
frequency, and byte offset, so a query can skip — or jump straight past — a
whole block without decoding a single posting inside it. A lazy
`PostingCursor` (`crates/tachyon-index/src/cursor.rs`, `segment/cursor.rs`)
walks one block at a time; `tachyon-query/src/wand.rs` merges cursors across
sources and candidate terms into one frontier per query token, then runs a
document-at-a-time driver:

- **Disjunctive (`match_mode=any`)**: classic WAND pivot selection — sum
  live frontiers' current-block bounds in ascending doc-id order; the first
  prefix whose bound clears the current top-K threshold names the pivot
  document. When *no* prefix clears it — the case a naive "always pick a
  pivot" design misses, and the one that matters most for a single-token
  query, which has only one frontier to pivot against — every live frontier
  skips its own hopeless block outright.
- **Conjunctive (`match_mode=all`, the default)**: a genuine match needs
  every token, so the bound is the *sum* of every frontier's current-block
  bound, checked *before* searching for agreement. When it clears the
  threshold, exact leapfrog intersection finds the next doc id every
  frontier agrees on — leapfrog's own catch-up never skips a genuine match,
  so it alone never causes approximation. When the bound doesn't clear, the
  intersection search is skipped entirely, which is what makes the default
  mode benefit from this too, not just `any`.

Both drivers still hand every visited document through the exact tail
pruning above. The only source of approximation is a document that's never
visited at all because a block was skipped — signaled per response by
`found_is_exact: false`. Facets inherit the same caveat automatically, since
they count over the same visited set `found` does.

Measured on the same 5M-document HTTP benchmark, broad query (`any` mode,
~9% of the corpus): search p95 went from 454 ms (tail pruning alone) to
**278 ms** — the O(matches) walk itself is now genuinely reduced, not just
its tail. `match_mode=all`, the default, does even better here (p95 **252
ms**) since leapfrog intersection narrows the candidate set before the
bound check ever runs.

## What is not built yet

**Off-lock flushing.** Merging no longer holds the write lock for its own
duration (see "Running off the write lock" under "Merges" above), but a
flush still does: encoding a memtable into a segment and writing it runs
synchronously under the same write-lock hold as the insert or delete that
crossed the threshold, so every search stalls for a large flush's full
duration. `encode_streaming` already streams in bounded memory the same
way a merge's build phase does, so the memory side of this is already
solved — what a flush would need to become off-lock is the same shape a
merge now has: reserve the output segment id and doc range up front,
encode without the lock held, then re-validate and commit. The
complication is different from a merge's, though — a flush's own memtable
is exactly what concurrent writes want to keep inserting into, so an
off-lock flush would need to swap in a fresh memtable for new writes to
land in *before* the encode starts, rather than after. Deferred until the
remaining stall (measured at 233 ms worst-case on a 1M-document
flush-under-load run, down from 3.7 s before any of this work) is shown to
matter in practice.
