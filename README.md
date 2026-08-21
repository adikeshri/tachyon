<p align="center">
  <img src="assets/logo.png" alt="Tachyon logo" width="200">
</p>

# Tachyon

**Open-source, typo-tolerant full-text search in a single binary.**

Tachyon adds production-grade text search — BM25 relevance, typo tolerance,
filters, facets, sorting, and autocomplete — to your application in under five
minutes. It is not a vector database and not a RAG engine; it does one thing.

```bash
docker run -p 8108:8108 ghcr.io/tachyon-search/tachyon:latest
```

> See [Known limitations](#known-limitations) before you rely on it.

---

## Quickstart

Create a collection:

```bash
curl -X POST localhost:8108/collections \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "products",
    "fields": [
      {"name": "title",       "type": "text"},
      {"name": "description", "type": "text"},
      {"name": "brand",       "type": "keyword", "facet": true},
      {"name": "price",       "type": "int",     "filter": true, "sort": true}
    ]
  }'
```

Index documents:

```bash
curl -X POST localhost:8108/collections/products/documents \
  -H 'Content-Type: application/json' \
  -d '[
    {"id": "1", "title": "Wireless Mouse", "brand": "Logitech", "price": 2999},
    {"id": "2", "title": "Mechanical Keyboard", "brand": "Razer", "price": 8999}
  ]'
```

Search:

```bash
curl 'localhost:8108/collections/products/search?q=wireless+mouse'
```

```json
{
  "found": 1,
  "found_is_exact": true,
  "search_time_ms": 0,
  "hits": [
    { "document": { "id": "1", "title": "Wireless Mouse", "brand": "Logitech", "price": 2999 },
      "text_match": 554.788 }
  ]
}
```

Misspell it and it still works:

```bash
curl 'localhost:8108/collections/products/search?q=wirelss+mouse'
```

---

## What it does

| | |
|---|---|
| **Relevance** | BM25, per-field boosts, phrase matching, term proximity |
| **Typo tolerance** | Damerau-Levenshtein, budget scaled to token length |
| **Filters** | `=`, `!=`, `<`, `<=`, `>`, `>=`, ranges, set membership, `&&`, `\|\|`, parentheses |
| **Facets** | Counted over the whole result set, not the page |
| **Sorting** | Any numeric field plus `_text_match`, multi-clause |
| **Autocomplete** | Prefix + typo tolerant, ordered by popularity |
| **Analytics** | Top queries, zero-result queries, latency percentiles |
| **Operations** | Prometheus metrics, API key auth, crash-safe writes |

Full API reference: [`docs/api.md`](docs/api.md).
Design and internals: [`docs/architecture.md`](docs/architecture.md).

---

## Measured performance

From `cargo run --release -p tachyon-bench`, on an Apple M-series laptop, over a
synthetic catalogue where a one-word query matches **6% of the corpus** — far
broader than real traffic, and deliberately so, because it is the expensive case.

| | 100k documents | 1M documents | 5M documents | Target |
|---|---|---|---|---|
| Search p95 | **3.4 ms** | 34.4 ms | 175.1 ms | < 30 ms |
| Search p99 | **4.0 ms** | 35.5 ms | 181.7 ms | < 60 ms |
| Autocomplete p95 | **0.29 ms** | 0.23 ms | 1.8 ms | < 5 ms |
| Indexing | **196k docs/sec** | 145k docs/sec | 88k docs/sec | 10k docs/sec |
| Memory (steady RSS) | 405 MiB | 606 MiB | 895 MiB | — |
| Memory (peak RSS) | 405 MiB | 646 MiB | 1.5 GiB | — |

Both measured right after indexing finishes, before any searches run. Steady
is current RSS at that point — what's resident most of the time. Peak is
`getrusage`'s kernel-tracked high-water mark since process start, which also
catches any transient spike indexing passed through on the way there; at 100k
documents the whole corpus stays in one memtable (no segment ever gets
written), so steady and peak are identical there by construction, not
coincidence.

Reproduce:

```bash
cargo run --release -p tachyon-bench -- --documents 1000000 --queries 2000
```

Indexing throughput beats the target by nearly an order of magnitude at 5M
documents and more at smaller scales, and autocomplete clears its target at
every scale measured. Search meets the latency target at 100k documents; at
1M and 5M it misses **on this corpus** — see
[Known limitations](#known-limitations) — but memory no longer moves in
lockstep with corpus size the way it used to: peak RSS at 5M documents fell
from 5.2 GiB to 1.5 GiB after the streaming segment writer and merge
described below replaced a design that rebuilt each merge's input through a
scratch in-memory index before re-encoding it.

---

## Known limitations

Read this before choosing Tachyon.

**Flushes and merges both stream in bounded memory, off the write lock.**
Writes are durable — they go to a write-ahead log and are replayed on
startup — and once a memtable crosses `--max-memtable-docs`/
`--max-memtable-bytes` it is flushed into an immutable, mmap'd on-disk
segment: postings, columns, and document values are all read lazily and
decoded only for what a query actually touches, which is what keeps memory
bounded by how much of the corpus is hot rather than by corpus size. A
query fans out across the memtable plus every committed segment, so
segment count on its own drives latency up over time; once a collection
holds more than `--merge-trigger-segments` (default 8), the smallest
`--merge-fan-in` (default 4) are folded into one via a streaming k-way
merge of the victims' already-encoded term dictionaries, postings, columns,
and doc stores — no document is ever re-parsed or re-tokenized, and nothing
beyond one term's or one field's worth of data is held in memory at a time.
Measured on a 1M-document, four-segment merge, its own RSS delta averaged
~93 MiB and peaked at ~110 MiB (a 5M-document run's overall peak RSS fell
from 5.2 GiB to roughly 1.5–1.9 GiB, depending on the run). Both a flush and
a merge hold the collection's write lock only for a brief snapshot (or
seal) before their own encode and an equally brief commit after it — never
for the encode itself — so a search or a write is blocked only for that
snapshot/commit, not for however long a large flush or merge happens to
take. Measured on a 1M-document flush-under-load run with continuous
concurrent search: worst-case search latency fell from 3.7 s (before any of
this work) to 233 ms once merging alone moved off the lock, then to
**106 ms** once flushing did too — and, since the lock is now held only for
a brief moment rather than for the length of an encode, that worst case is
also markedly less noisy run to run than the numbers it replaced.

**Broad queries no longer visit every match unconditionally.** A block-level
score bound (true block-max WAND, `.post`'s v3 format) now skips whole
regions of a term's postings — and, when the bound can't clear the current
top-K threshold, whole documents — without decoding them at all, for both
`match_mode=any` and the default `match_mode=all`. `found`/facets stay exact
only when nothing was skipped, signaled per response by `found_is_exact`;
when pruning does engage, they become a lower bound, not a false one.
Measured on a 5M-doc, ~9%-broad-query benchmark: search p95 went 494 ms
(before any of this pruning existed) → 454 ms (exact tail-only pruning) →
**278 ms** (`any` mode) / **252 ms** (`all` mode, the default) with true
WAND. Real catalogues are far more selective than this benchmark's
deliberately-broad query, and adding a filter still reduces cost further on
top of this.

**Not yet built:** distributed clustering, replication, synonyms, stemming, stop
words, highlighting, geo search, and nested documents. All are explicit
non-goals for v1.

**Analytics are not durable.** They are an operational signal and reset on
restart.

---

## Configuration

Every flag has an environment variable equivalent.

| Flag | Environment | Default | Meaning |
|---|---|---|---|
| `--listen` | `TACHYON_LISTEN` | `0.0.0.0:8108` | Listen address |
| `--data-dir` | `TACHYON_DATA_DIR` | `./data` | Collections, WAL, segments |
| `--sync-interval-ms` | `TACHYON_SYNC_INTERVAL_MS` | `0` | `0` fsyncs every write; higher trades durability for throughput |
| `--max-memtable-docs` | `TACHYON_MAX_MEMTABLE_DOCS` | `100000` | Flush threshold |
| `--merge-trigger-segments` | `TACHYON_MERGE_TRIGGER_SEGMENTS` | `8` | Merge once a collection holds more segments than this |
| `--merge-fan-in` | `TACHYON_MERGE_FAN_IN` | `4` | Segments merged at once — smallest by document count |
| `--admin-key` | `TACHYON_ADMIN_KEY` | unset | Read/write API key |
| `--search-key` | `TACHYON_SEARCH_KEY` | unset | Read-only API key |
| `--log` | `TACHYON_LOG` | `info` | `tracing` filter |

**With no keys set, every endpoint is open.** That is right for local
development and wrong for anything reachable from a network; set `--admin-key`
before you expose it.

---

## Building from source

Needs Rust 1.85 or newer.

```bash
cargo build --release        # binary at target/release/tachyon
cargo test                   # 350 tests
cargo clippy --all-targets
```

The workspace is layered so each crate depends only on the ones below it:

```text
tachyon-server   REST API, auth, analytics, metrics, the binary
tachyon-engine   collection lifecycle, write path, recovery
tachyon-query    parsing, planning, scoring, ranking
tachyon-index    tokenizer, inverted index, columns, fuzzy matching, segments
tachyon-storage  write-ahead log, on-disk layout, metadata
tachyon-core     schema, values, documents, errors
```

---

## Contributing

Issues and pull requests are welcome. Substantial changes should start as an
RFC issue so the design can be discussed before the code is written. See
[`CONTRIBUTING.md`](CONTRIBUTING.md).

## License

Apache 2.0. See [`LICENSE`](LICENSE).
