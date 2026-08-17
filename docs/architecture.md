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
doc id); `.post`, `.col`, and `.doc` each carry a small eager header — an
offset table, essentially — followed by the data it points into.

Every file is mmap'd, and every read decodes only what it was asked for:

- **`.post`**: the header holds per-field corpus stats and a byte offset per
  term. A query resolves a term through `.terms`' FST to a dense id, then
  decodes just that one term's block — nothing else in the file is touched.
  `doc_freq`/`live_doc_freq` go further still: they only ever need a document
  count or a liveness check, never positions, so they walk the block skipping
  position arrays instead of decoding and allocating them — the difference
  matters because autocomplete calls both across every candidate term and
  every source.
- **`.col`**: the header is a small per-field directory (tag, offset,
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
append. This is deliberately simple, and it is the obvious place to look when
write throughput under concurrent search becomes the bottleneck — the next
step would be an atomically-swapped read snapshot so searches never block at
all.

## Merges

A search fans out across the memtable plus every committed segment, and
each segment pays its own decode independently — so segment count, not
corpus size alone, drives query latency up over a collection's life. A merge
folds several small segments into one, keeping that count from growing
without bound: once a collection holds more than `merge_trigger_segments`
segments (default 8), the flush that crossed that line also picks the
`merge_fan_in` smallest by document count (default 4, size-tiered in
spirit) and merges them, right after its own commit and under the same
write-lock hold. At most one merge per flush, so segment count converges
down over several flushes rather than in one large pause.

**A merge renumbers documents, it does not preserve their ids.** A segment's
doc-id range is fixed for its life (see "Segments" above), so folding two
already-committed segments together while keeping their original ids would
mean merging two already-*encoded* structures — a from-scratch k-way merge of
sorted term dictionaries and postings. Renumbering sidesteps that entirely:
a merge fetches each live, non-tombstoned document's source through
`SegmentReader::get`, runs it back through `ParsedDocument::parse` — the
exact validation and tokenization a fresh insert already does — and feeds
the result into a scratch `MemTable`, then calls the ordinary `encode` on
it. The entire insert/encode pipeline is reused unchanged; the only new
code is picking victims and updating the commit state. Retired ids are
simply never claimed again, and any tombstones for them are pruned from
`state.json`'s `deleted` list as part of the merge's own commit, the same
way a flush's own commit is the point of no return.

The cost of that reuse is a **transient memory spike while a merge is in
flight**: rebuilding `merge_fan_in` segments' worth of live documents into a
scratch memtable before encoding it means the merge briefly holds all of
them decoded at once, proportional to `merge_fan_in × segment size` rather
than to the corpus as a whole. Measured directly: peak RSS during a 1M-doc
run with default settings was ~1.5 GiB captured mid-merge, but memory back
at rest immediately after indexing finished (no merge in flight, no queries
yet run) was ~49 MiB. It's a real, momentary cost worth accounting for when
sizing `merge_fan_in` on a memory-constrained deployment — smaller values
trade a bigger memory ceiling for more frequent, cheaper merges — but it is
not sustained.

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
separate, composed layer: `.post`'s v3 format groups a term's postings into
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

**Streaming merges.** A merge currently rebuilds its input through a scratch
memtable rather than merging the already-encoded structures directly, which
is what causes the transient memory spike above. A direct k-way merge of
sorted term dictionaries, columns, and doc stores across segments would
avoid materializing anything beyond what's being written — real, novel code
this workspace doesn't have yet, deferred until the spike above is shown to
matter in practice.
