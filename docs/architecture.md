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
lock and run concurrently with each other; a write holds the write lock only
for the WAL append and the memtable update. This is deliberately simple, and it
is the obvious place to look when write throughput under concurrent search
becomes the bottleneck — the next step would be an atomically-swapped read
snapshot so searches never block at all.

## What is not built yet

**Segment flush.** The memtable is never written to disk, so memory grows with
the corpus and startup replays the whole log. Everything around it exists: the
commit protocol, WAL generations, tombstone bitmaps, and the `IndexSource`
abstraction segments plug into. This is the most important next piece of work.

**Block-max WAND.** The executor scores every match. Early termination would
let it skip documents that cannot reach the top-K, which is what broad queries
on large corpora need.

**Tiered merges.** Follows segments.
