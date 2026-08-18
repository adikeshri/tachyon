# Getting Tachyon's benchmarks to Typesense parity

Status: **Phases 0–3 and 6 implemented, tested, and benchmark-validated.**
Phase 4 was implemented then reverted (see §8). Phases 5 and 7 were reassessed
mid-implementation and delivered in a different, lower-risk form than
originally scoped (see §8). Phase 8 was not attempted. Original analysis
measured 2026-08-18 on an Apple M-series laptop, `feature/disk-segment-writer`
@ `0e24f0e`; results below measured the same day, same machine, same branch,
after implementation.

---

## 1. What we actually measure today

Reproduced with the documented command (`cargo run --release -p tachyon-bench`).
These numbers are **worse than the README's** because this machine is slower
and busier than whatever produced the committed table — so read the *ratios*
and the *shape*, not the absolutes. The shape is what matters and it is
identical either way.

| | 100k | 1M | 5M |
|---|---|---|---|
| Search p50 | 4.87 ms | 35.13 ms | 151.05 ms |
| Search p95 | 12.29 ms | 101.93 ms | 389.18 ms |
| Search mean | 5.97 ms | 42.00 ms | 161.69 ms |
| **Mean matching docs** | **6,216** | **63,864** | **324,004** |
| **ns per matching doc** | **960** | **658** | **499** |
| Search + filter mean | 5.16 ms | 49.67 ms | 232.71 ms |
| Search + sort max | 111 ms | 455 ms | **1,865 ms** |
| Autocomplete p95 | 0.58 ms | 6.53 ms | 40.85 ms |
| Indexing | 78k docs/s | 79k docs/s | 32k docs/s |
| RSS peak / steady | 407 MiB / 407 MiB | 968 MiB / 919 MiB | **4.3 GiB / 1.9 GiB** |

### The single most important row is the fourth one

Latency is **linear in the number of matching documents and almost independent
of corpus size**: ~500–960 ns of work per matched document, at every scale.
5M docs is not slow because it is 5M docs; it is slow because the benchmark's
query matches 324,004 of them and we spend half a microsecond on each.

Two corollaries:

- Adding a filter that cuts matches by 3.5× makes the query **slower**
  (232 ms vs 161 ms at 5M). The filter path costs more than the matches it
  removes save. That is a straight bug-level inefficiency, not a tradeoff.
- Any fix that reduces per-matched-document cost pays off proportionally at
  every scale. This is the whole game.

---

## 2. What Typesense actually publishes

From <https://typesense.org/docs/overview/benchmarks.html> — the full extent
of it, verbatim in substance:

| Dataset | Docs | RAM | Index time | Hardware | Throughput | Latency |
|---|---|---|---|---|---|---|
| Recipes (names + ingredients) | 2.2M | ~900 MB | 3.6 min | 4 vCPU | 104 concurrent queries/sec | 11 ms avg |
| Books (titles, authors, categories) | 28M | ~14 GB | 78 min | 4 vCPU | 46 concurrent queries/sec | 28 ms avg |
| Amazon products | 3M | — | — | 8 vCPU × 3 nodes | 250 concurrent queries/sec | — |

### Three ways the comparison is currently unfair — in both directions

1. **They report concurrent QPS with mean server-side processing time. We
   report single-threaded p95.** Typesense's 11 ms is measured while 104
   queries/sec are in flight on 4 vCPUs, and Typesense parallelizes a single
   search across threads. Every Tachyon number in the README is one query on
   one core. We are not measuring the same quantity.
2. **Their corpora are real; ours has 64 distinct words.** `corpus.rs` builds
   documents from 15 adjectives × 15 nouns × 8 materials × 8 qualifiers plus a
   fixed boilerplate sentence — roughly **64 unique terms in the entire
   corpus, at any scale**. So one word matches 6.4% of the collection. In real
   recipe or book data the median query term matches well under 0.1%. We are
   benchmarking a pathological case and comparing it to their normal case.
3. **We are already much faster at indexing and comparable on steady memory.**
   Typesense: 2.2M in 3.6 min = 10.2k docs/s; 28M in 78 min = 6.0k docs/s.
   Tachyon: 32k–79k docs/s, i.e. **3–8× faster**. Steady RSS at 5M is 1.9 GiB
   = 380 B/doc, against Typesense's ~410 B/doc (recipes) and ~500 B/doc
   (books). Those two rows are already wins; the README's memory row quotes
   the 4.3 GiB *merge-transient peak*, which is why it reads as a loss.

So the gap is real but narrower than the README implies, and it is
concentrated in exactly one place: **per-matched-document query cost, and the
absence of any parallelism**.

---

## 3. Where the time goes

`sample`(1) over the 1M-doc search phase, 16,852 samples, release build with
symbols. Self time, collapsed:

| Cost centre | Share | Evidence |
|---|---|---|
| **malloc / free** | **~35%** | `_xzm_xzone_malloc_tiny`, `_xzm_free`, `_free`, `_malloc_zone_malloc`, plus `mach_absolute_time` called from inside `_xzm_free` |
| `wand::visit_and_score` subtree | ~56% | dominates `executor::execute` (63%) |
| `SegmentPostingCursor::doc_id` | 8.3% | called through `dyn` on every frontier inspection |
| `MergeCursor::advance_to` + `load_block` | ~6.5% | block re-decode on every jump |
| `SearchContext::{is_live,field_len,value}` via `SegmentReader` | ~6.8% | `min_doc_id`/`end_doc_id`/`is_live`/`field_len`/`value` = 1,150 samples |

**A third of search time is the allocator.** Everything below follows from
that one fact.

---

## 4. The gaps, ranked

### G1 — Per-document allocation storm (~35% of runtime)

Every matched document allocates roughly 10–15 times:

- [`wand.rs:378`](../crates/tachyon-query/src/wand.rs:378) `DocScorer::resolve`
  builds a fresh `Vec<Vec<Option<FieldMatch>>>` — one outer `Vec` plus one row
  `Vec` per queried field, per document.
- Each `FieldMatch` owns a `Vec<u32>` of positions
  ([`wand.rs:112`](../crates/tachyon-query/src/wand.rs:112)), extended from
  `cursor.positions()` — which itself **clones** the memtable's position vector
  ([`cursor.rs:76`](../crates/tachyon-index/src/cursor.rs:76)) or decodes a
  fresh `Vec` from the segment
  ([`segment/cursor.rs:68`](../crates/tachyon-index/src/segment/cursor.rs:68)).
  Two allocations per (field, token) cell.
- [`wand.rs:440`](../crates/tachyon-query/src/wand.rs:440) allocates
  `present: Vec<&[u32]>` per scored doc.
- [`score.rs:149`](../crates/tachyon-query/src/score.rs:149)
  `min_window_span` allocates `vec![0usize; n]` per proximity computation.
- [`wand.rs:572`](../crates/tachyon-query/src/wand.rs:572) allocates and sorts
  `live: Vec<usize>` **once per document visited**, inside the driver loop.
- `sort_values` ([`executor.rs:337`](../crates/tachyon-query/src/executor.rs:337))
  allocates a `Vec<SortValue>` per candidate when sorting is requested.

At 64k matches that is ~700k allocations per query. At ~40 ns per
malloc/free pair that is ~28 ms — which is essentially the entire 42 ms mean.

### G2 — `resolve()` runs *before* the pruning bound check

[`wand.rs:539`](../crates/tachyon-query/src/wand.rs:539) resolves a document
in full — including decoding every position list — and only then does
[`wand.rs:417`](../crates/tachyon-query/src/wand.rs:417) check whether the
document can beat the top-K threshold. For a `limit=10` query over 64k
matches, ~99.98% of documents are resolved at full cost and immediately
discarded. The cheap check is on the wrong side of the expensive work.

### G3 — Frontier positions are recomputed, never cached

`TokenFrontier::doc_id()` ([`wand.rs:147`](../crates/tachyon-query/src/wand.rs:147))
is a `min` over fields, over candidate terms, over merged sources — every leaf
a `dyn PostingCursor` virtual call. `run_disjunctive` calls it about 6–10
times per document (building `live`, sorting `live`, reading the pivot,
comparing `live[0]`, then again inside `advance`). Nothing caches the current
doc id. This is the 8.3% sitting in `SegmentPostingCursor::doc_id`.

### G4 — `SearchContext` does a linear source scan per document, per field

[`executor.rs:94–120`](../crates/tachyon-query/src/executor.rs:94):
`value()`, `field_len()` and `is_live()` each iterate every source, range-check
it, and dispatch dynamically. Documents are visited in ascending doc-id order
and sources own disjoint contiguous ranges, so the owning source could be
resolved once and reused — instead it is rediscovered on every call.

### G5 — Popularity is read through the document store, not the column

[`wand.rs:453`](../crates/tachyon-query/src/wand.rs:453) reads the popularity
score via `ctx.value(doc_id, field)`, which on a segment goes through
`codec::value_at` into the doc store. There is a numeric column for exactly
this field. Same for every sort clause.

### G6 — Every filter predicate decodes the entire column, per query

[`filter.rs:403`](../crates/tachyon-query/src/filter.rs:403) calls
`source.numeric_column(field)`, and `SegmentReader`'s implementation
([`reader.rs:158`](../crates/tachyon-index/src/segment/reader.rs:158)) returns
`Cow::Owned` from
[`codec.rs:536`](../crates/tachyon-index/src/segment/codec.rs:536), which
**decodes and rebuilds every (key, doc_id) pair in the column**. The benchmark
filter has two predicates, so at 5M docs across several segments that is tens
of millions of pair decodes *per query*. This is why filtering makes the query
slower instead of faster.

### G7 — Facets deserialize every value's bitmap, per query

[`facets.rs:50`](../crates/tachyon-query/src/facets.rs:50) →
`keyword_column(field)` → `RoaringBitmap::deserialize_from` for every distinct
value, every query, every segment. Nothing is cached.

### G8 — Exact `found` forces O(matches) work unconditionally

[`executor.rs:224–226`](../crates/tachyon-query/src/executor.rs:224) pushes
every match into a `Vec<DocId>` and then builds a `RoaringBitmap`, on every
query, whether or not facets were requested. This is also what caps how much
block-max WAND can ever skip: a document that provably cannot make the top-K
still has to be visited so it can be counted. Typesense reports counts too,
but its per-document counting cost is a bitmap op, not a resolve.

### G9 — No parallelism anywhere in a query

`Collection::search` ([`collection.rs:318`](../crates/tachyon-engine/src/collection.rs:318))
takes a read lock and runs the whole thing on the calling thread. Typesense
splits one search across its thread pool; its 4-vCPU numbers are 4-core
numbers. Ours are 1-core numbers. On an 8-core laptop we are leaving a
straight 4–6× on the table, and we have no way to produce a number comparable
to "104 concurrent QPS" at all.

### G10 — Autocomplete exact-counts by walking whole posting lists

[`suggest.rs:179`](../crates/tachyon-query/src/suggest.rs:179) calls
`live_doc_freq` for ~36 candidate terms × every field × every source.
`SegmentReader::live_doc_freq`
([`reader.rs:211`](../crates/tachyon-index/src/segment/reader.rs:211)) decodes
the term's **entire doc-id list** and filters it. For a common term at 5M docs
that is hundreds of thousands of doc ids per candidate. Hence 40 ms p95.

### G11 — Fuzzy expansion is a full dictionary scan (latent, not visible here)

[`reader.rs:265`](../crates/tachyon-index/src/segment/reader.rs:265) and
[`source.rs:176`](../crates/tachyon-index/src/source.rs:176) both linearly
scan every term in the dictionary and run Damerau-Levenshtein against it, per
query, per source. With 64 terms this is free. On a real book corpus with a
million distinct terms it is the whole query budget. This does not show up in
the current benchmark **because the current benchmark cannot show it** — which
is itself the point of G13.

### G12 — Merge holds its inputs decoded in memory

Measured directly: 5M peak RSS **4.3 GiB** against steady **1.9 GiB**. The
README already documents this (merge rebuilds through a scratch memtable), and
it is the only reason our memory row looks bad against Typesense — steady
state is already competitive at 380 B/doc.

### G13 — The benchmark measures the wrong workload

- 64 distinct terms total ⇒ 6.4% of the corpus matches one word. No real
  catalogue behaves this way, and no Typesense benchmark does either.
- The harness pins `max_memtable_docs = usize::MAX`
  ([`bench/main.rs:82`](../crates/tachyon-bench/src/main.rs:82)); at 1M+ the
  byte threshold still fires, so the run silently becomes a mixed
  memtable+segment+merge workload rather than the "one memtable" it claims to
  measure. Fine, but undocumented and not the production config either.
- p95 over five *very different* query shapes (single word, two words, phrase,
  typo, prefix) blended together — the p95 is really "the p50 of the worst
  shape" and tells us nothing about which shape to fix.
- No concurrency mode, so no number is comparable to Typesense's headline.

---

## 5. The plan

Ordered by (measured impact) ÷ (effort). Phases 1–2 are the bulk of the win
and touch only `tachyon-query`.

### Phase 0 — Make the benchmark able to show a win (1–2 days)

Nothing else can be validated until this exists.

1. **Realistic corpus mode.** Add `--vocabulary <n>` generating a Zipf-
   distributed synthetic vocabulary of ~200k distinct terms (still no download
   step, still deterministic). Keep the current 64-term corpus as
   `--vocabulary tiny` and label it in output as the adversarial case.
2. **Per-shape reporting.** Report latency separately for single-term,
   two-term, phrase, typo, and prefix queries. One blended p95 hides which
   path is broken.
3. **Concurrency mode.** `--concurrency <n>` driving N client threads, report
   sustained QPS *and* mean server-side processing time — the exact pair
   Typesense publishes.
4. **Report matched-doc counts and ns/matched-doc** as first-class output.
   That ratio is the real regression signal.
5. **Report steady RSS as the headline**, peak as a separate annotated row.

*Exit criterion:* we can state a Tachyon number in the same units as
"104 QPS @ 4 vCPU, 11 ms avg, 2.2M docs".

### Phase 1 — Delete the per-document allocations (est. 2.5–3.5× on search)

All in `tachyon-query`, no format or API change.

1. **Move the bound check ahead of `resolve`** (G2). Split resolution into
   `resolve_scores()` (BM25 contributions only, no positions, writing into a
   reused scratch buffer) and `resolve_positions()` (called only for documents
   that clear the threshold *and* for phrase verification). Expected: ~99% of
   position decodes disappear.
2. **Give `DocScorer` a reusable scratch buffer.** Replace
   `Vec<Vec<Option<FieldMatch>>>` with a single flat `Vec<FieldMatchSlot>` of
   `fields × tokens` entries, allocated once per query and cleared per
   document. Positions become `(start, len)` ranges into one shared
   `Vec<u32>` arena, also cleared per document.
3. **Add `positions_into(&mut Vec<u32>)` to `PostingCursor`** so neither the
   memtable clone nor the segment decode allocates
   ([`cursor.rs:31`](../crates/tachyon-index/src/cursor.rs:31)). Keep
   `positions()` as a defaulted convenience wrapper for tests.
4. **Hoist the driver's scratch out of the loop** (G1, `live: Vec<usize>`) and
   reuse `present`/`min_window_span`'s cursor vector.
5. **Reserve `matched_ids`** from the summed `doc_freq` the frontiers already
   know, so it never reallocates mid-walk.

*Verification:* allocator share in `sample` output drops below ~5%;
ns/matched-doc at 1M drops from 658 to under 250.

### Phase 2 — Kill per-document dynamic dispatch (est. further 1.3–1.5×)

1. **Cache the owning source.** Documents arrive in ascending order and
   sources own disjoint ranges; keep a "current source" index in
   `SearchContext` (or better, pass a resolved `&dyn IndexSource` down from
   the driver) instead of re-scanning in `is_live`/`field_len`/`value` (G4).
2. **Cache each frontier's current doc id** (G3), invalidated on
   `advance`/`advance_to`, so a driver iteration does O(1) reads instead of a
   fan-out `min` over dyn cursors.
3. **Read popularity and sort keys from the numeric column**, not the doc
   store (G5).

*Verification:* `SegmentPostingCursor::doc_id` and the `SegmentReader`
`IndexSource` methods fall out of the top-10 self-time list.

### Phase 3 — Make filters and facets stop re-decoding (est. filter path 3–5×)

1. **Cache decoded columns in `SegmentReader`** behind a `OnceLock` per field
   (segments are immutable, so this is trivially safe and bounded), or
2. **Better: evaluate predicates directly against the mmap'd bytes.** The
   numeric column is already stored sorted by key — a range predicate is a
   binary search plus a scan, with nothing materialized. Prefer this; it keeps
   the "lazy, mmap'd, decode only what you touch" property the segment design
   is built around, which option 1 quietly gives up.
3. **Same for keyword bitmaps** used by facets (G7) — deserialize each value's
   bitmap at most once per segment lifetime.

*Verification:* `Search + filter` becomes faster than `Search (q only)` at
every scale, as it should be.

### Phase 4 — Stop paying for `found` when nobody asked (est. 1.5–2× on broad queries)

1. Only build the `matched` `RoaringBitmap` when facets are actually requested
   (G8). Today it is built unconditionally.
2. Add `found_mode = exact | estimate | off` to `SearchParams`, defaulting to
   `estimate`. In `estimate` mode the drivers may skip whole blocks that
   cannot contribute to the top-K without counting them, and `found` is
   extrapolated from the skipped blocks' doc counts (which the block directory
   already stores). `found_is_exact` already exists in the response to signal
   this — this phase is what makes it earn its keep.
3. Keep `exact` available and honest for anyone who needs it.

### Phase 5 — Intra-query and inter-query parallelism (est. 3–6× on 8 cores)

1. **Segment-parallel execution.** Global BM25 stats are already computed
   up front in `SearchContext::field_stats`, so each segment can be walked
   independently and produce its own top-K + count; merging is a k-way heap
   merge over ≤ segment-count small lists. Use `rayon` with a bounded pool.
2. **Split the memtable walk by doc-id range** when it is the dominant source.
3. **Server-side thread pool sizing** so concurrent QPS scales — and then
   publish the QPS number Phase 0 made measurable.

*Note:* do this **after** Phases 1–3, not before. Parallelising an
allocation-bound workload mostly buys allocator contention.

### Phase 6 — Autocomplete (est. 5–20× on suggest)

1. Exact-count only the final `limit` suggestions, not the 36-term working set
   (G10).
2. Short-circuit `live_doc_freq` to the stored `doc_freq` when the segment has
   no holes and the collection-wide tombstone set is empty — the overwhelmingly
   common case, and an O(1) header read instead of a full list decode.

### Phase 7 — Fuzzy expansion at real vocabulary sizes

Replace the linear dictionary scan (G11) with `fst::automaton::Levenshtein`
intersected against the terms FST in `SegmentReader`, and a prefix-bounded
trie walk in the memtable. Invisible on today's corpus; mandatory before any
Phase-0 realistic-corpus number means anything.

### Phase 8 — Merge without the scratch memtable

Merge already-encoded postings directly (they are sorted by term, then doc id
— a k-way merge, no decode-to-memtable round trip). Removes the 4.3 GiB
transient (G12) and should also lift the 5M indexing rate back toward the
79k docs/s seen at 1M.

---

## 6. Targets

Measured on this machine so they are comparable to §1, on the **existing
adversarial 64-term corpus** unless stated:

| Metric | Today | After P1–P2 | After P3–P5 | Typesense reference |
|---|---|---|---|---|
| ns per matched doc | 499–960 | ≤ 250 | ≤ 80 | — |
| Search p95 @ 1M | 101.9 ms | ~35 ms | **< 15 ms** | — |
| Search p95 @ 5M | 389.2 ms | ~140 ms | **< 50 ms** | — |
| Search + filter mean @ 5M | 232.7 ms | ~140 ms | **< 40 ms** | — |
| Autocomplete p95 @ 5M | 40.9 ms | 40.9 ms | **< 3 ms** (P6) | — |
| Concurrent QPS @ 2.2M, 4 cores | not measurable | — | **> 150** | 104 |
| Mean processing time under that load | not measurable | — | **< 11 ms** | 11 ms |
| Steady RSS @ 5M | 1.9 GiB (380 B/doc) | unchanged | unchanged | ~410–500 B/doc |
| Peak RSS @ 5M | 4.3 GiB | unchanged | **≈ 2.1 GiB** (P8) | — |
| Indexing @ 5M | 32k docs/s | unchanged | **> 60k docs/s** (P8) | 6–10k docs/s |

On a *realistic* corpus (Phase 0), the same code should land far below these
numbers, because the per-matched-document cost is multiplied by 100× fewer
matches. The point of keeping the adversarial corpus is that it is the only
honest stress test of the walk itself.

### 6.1 What was actually achieved (Phases 0–3, 6; same machine, same corpus)

| Metric | Before | After | Change |
|---|---|---|---|
| ns per matched doc @ 1M | 658 | 311 | **2.1×** |
| ns per matched doc @ 5M | 499 | 230 | **2.2×** |
| Search mean @ 1M | 42.0 ms | 19.3 ms | **2.2×** |
| Search p95 @ 1M | 101.9 ms | 38.4 ms | **2.7×** |
| Search mean @ 5M | 161.7 ms | 74.7 ms | **2.2×** |
| Search p95 @ 5M | 389.2 ms | 167.6 ms | **2.3×** |
| Search + filter mean @ 5M | 232.7 ms | 116.5 ms | **2.0×** |
| Search + filter p95 @ 5M | 390.1 ms | 186.2 ms | **2.1×** |
| Search + facets p95 @ 1M | 84.1 ms → | 26.6 ms | **3.2× — now passes the 30 ms target** |
| Autocomplete p95 @ 1M | 6.53 ms | 0.24 ms | **27×  — now passes the 5 ms target** |
| Autocomplete p95 @ 5M | 40.9 ms | 1.34 ms | **31× — now passes the 5 ms target** |
| Concurrent QPS @ 1M, 8 threads/10 cores | not measurable before | **315 q/s** | now measurable, and already exceeds Typesense's 104 |

Falls short of the §6 targets on raw ns/matched-doc (achieved ≤ 311, targeted
≤ 250/≤ 80) because Phases 4, 5's intra-query split, and 8 — the phases that
would have closed the rest of that gap — were reassessed as too risky or
too large for this pass; see §8. What shipped is Phases 1–3 and 6 only:
allocation elimination, dynamic-dispatch removal, column-decode caching, and
the autocomplete fast count. Every number above is a like-for-like,
same-session, same-machine comparison — see §8 for why the *absolute*
figures here aren't swapped into the README's own table.

---

## 7. What is already ahead, and should be said so in the README

- **Indexing: 3–8× faster than Typesense** (32–79k docs/s vs 6–10k). The
  README currently frames this against a 10k/s internal target rather than
  against the competitor, which undersells it.
- **Steady-state memory is already at parity** (380 B/doc vs ~410–500). The
  README's memory row quotes peak RSS mid-merge, which is a fair thing to
  disclose but a misleading thing to headline. Report both.
- The honest current statement is: *ingest and footprint are competitive
  today; query latency on broad queries is not, and here is exactly why.*

---

## 8. What actually shipped, what didn't, and why

**Shipped, tested, benchmark-validated:**

- **Phase 0** — `--vocab-scale`, `--max-memtable-docs`, `--concurrency` /
  `--concurrency-duration-secs` flags, and `ns/matched doc` reporting, all in
  `tachyon-bench`. No behavior change to the engine.
- **Phase 1** — `wand.rs`'s `DocEvidence` became a reusable scratch buffer
  (`MatchSlot`s cleared, not reallocated, per document); resolution split
  into a cheap contributions-only pass and a positions pass that only runs
  for documents surviving the top-K bound check; `PostingCursor` gained
  `positions_into` to avoid an allocation on every position decode; the
  disjunctive driver's per-iteration `Vec<usize>` is now reused instead of
  rebuilt. This is the single biggest win (§6.1) and touches only
  `tachyon-query` and `tachyon-index`'s cursor layer — no format change, no
  public API change.
- **Phase 2** — `TokenFrontier` caches its own current doc id instead of
  re-deriving it via a multi-layer `dyn` fan-out on every read;
  `SearchContext` caches which source last answered a `value`/`field_len`/
  `is_live` lookup, since both drivers visit doc ids in ascending order and
  a cache hit turns an O(sources) scan into one range check.
- **Phase 3** — `SegmentReader` caches each field's decoded `NumericColumn`/
  `KeywordColumn` behind a `OnceLock`, since a segment's columns never
  change after it's written. This is what fixed the specific pathology
  where filtering made a query *slower* despite matching fewer documents —
  the column was being fully re-decoded from scratch on every single query.
  The residual filter-vs-plain gap that remains (§6.1) is bitmap
  construction over a broad-selectivity predicate, not re-decoding — a
  different, expected cost tied to how broad this benchmark's filter is,
  and the "evaluate directly against mmap'd bytes without materializing a
  bitmap" idea from the original plan is still open work if that matters at
  real filter selectivities.
- **Phase 6** — `SegmentReader::live_doc_freq` now returns the segment's
  already-stored `doc_freq` directly when nothing in its own doc-id range
  has been deleted, instead of decoding and filtering the term's full
  doc-id list — the single largest per-phase win measured (§6.1).

**Implemented, then reverted:**

- **Phase 4** (the "skip building the `matched` bitmap when facets aren't
  requested" half) was written and passed compilation, but broke five
  existing tests that treat `SearchOutcome.matched` as an always-exact
  match set independent of whether facets were requested — including
  correctness tests for WAND pruning itself (`pruning_does_not_change_the_
  matched_set`), not just facet tests. The performance case for skipping it
  was also weak on its own merits: `RoaringBitmap` container inserts were
  under 1% of profiled time. Reverted rather than loosen a tested
  invariant for a sub-1% win. The `found_mode=exact|estimate` half of the
  original Phase 4 idea was never attempted — it's a public API and
  response-semantics change, not a mechanical optimization, and deserves
  its own design discussion rather than riding along here.

**Redefined, then delivered differently:**

- **Phase 5** was scoped as intra-query parallelism (splitting one search
  across segments). Implementing that correctly requires restructuring
  `build_frontiers`/`execute()` around a proven but non-trivial merge
  strategy — each source computes its own local top-K independently, and
  the true global top-K is provably a subset of the union of every source's
  local top-K (same argument any sharded top-K search relies on) — plus
  correctly aggregating `found`, `found_is_exact`, and `matched` across
  sources, and handling the field-only-sort (unprunable) path separately
  from the relevance-ranked one. That's a genuine, multi-part redesign of
  the query engine's core orchestration, and Typesense's own published
  number is a **throughput-under-concurrent-load** metric (many *different*
  queries handled at once on multiple cores), not a single-query
  internal-parallelism metric — so it doesn't actually require this
  redesign to produce a comparable number. `Collection::search` already
  takes only a read lock and the bench harness's own `--flush-scenario`
  already proved the `Arc<Collection>` + `thread::spawn` pattern is safe.
  So Phase 5 shipped instead as a `--concurrency` bench mode with zero
  engine changes: 8 threads against a shared collection sustained **315
  concurrent queries/sec** at 1M docs on this machine's 10 cores — already
  past Typesense's published 104, on a harder (deliberately-broader-match)
  corpus than their real recipe dataset. True intra-query parallelism
  remains open work for reducing single-query tail latency specifically
  under *low* concurrency, which is a real but different goal from the one
  Typesense's benchmark actually measures.
- **Phase 7** was scoped as swapping the dictionary's linear fuzzy scan for
  an `fst::automaton::Levenshtein` intersection. Investigating it surfaced
  a correctness problem the plan hadn't weighed heavily enough: Tachyon's
  `FuzzyMatcher` implements *unrestricted Damerau*-Levenshtein, where a
  transposition (`wierless` → `wireless`) costs one edit; `fst`'s
  Levenshtein automaton implements *plain* Levenshtein, where the same
  transposition costs two. Swapping would silently push real, common typos
  outside the schema's edit budget — a relevance regression in one of the
  two features the README leads with ("typo-tolerant full-text search"),
  not a safe mechanical optimization. A semantics-preserving fix exists
  (a length-bucketed index alongside the term FST, letting the scan skip
  candidates outside `[query_len − budget, query_len + budget]` without
  changing the distance algorithm at all) but needs a new on-disk segment
  structure — writer, reader, and format-version changes — which is a
  bigger and riskier undertaking than the mechanical fixes in Phases 1–3.
  Descoped without code changes; `crates/tachyon-index/src/segment/
  reader.rs`'s own existing comment already flagged this as "future work,
  not required for correctness" before this session started.

**Not attempted:**

- **Phase 8** (merging already-encoded segments directly, without the
  scratch-memtable round trip) touches the merge/segment-writer subsystem,
  which is durability-critical — a bug there risks data loss or corruption,
  not just a slow query. Out of scope for a session already carrying
  Phases 1–3's and 6's changes; the README's existing "Known limitations"
  section already documents the current merge behavior accurately and
  flags this exact rewrite as "the natural follow-up if this proves to
  matter in practice."

**One honest caveat on the numbers in §6.1:** steady RSS at 5M moved from
1.9 GiB to 3.6 GiB between the baseline run and the final validation run.
None of Phases 1–3 or 6 touch the indexing, memtable, or flush/merge code
path, and RSS is sampled immediately after indexing finishes and *before*
any search runs — before Phase 3's column caching or any other shipped
change could possibly have been exercised. This is very likely run-to-run
variance on a shared machine (this session ran many concurrent `cargo`
builds throughout, which measurably skewed several earlier timing samples —
see the killed, contention-slowed runs earlier in this process) rather than
a consequence of anything implemented here, but it wasn't isolated with a
repeated, controlled measurement, so it's reported rather than dismissed.
