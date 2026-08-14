# Tachyon

**Open-source, typo-tolerant full-text search in a single binary.**

Tachyon adds production-grade text search — BM25 relevance, typo tolerance,
filters, facets, sorting, and autocomplete — to your application in under five
minutes. It is not a vector database and not a RAG engine; it does one thing.

```bash
docker run -p 8108:8108 ghcr.io/tachyon-search/tachyon:latest
```

> **Status: alpha.** The API is stable enough to build against and every
> feature below is tested end to end, but this has not run in production
> anywhere. See [Known limitations](#known-limitations) before you rely on it —
> in particular, **all data currently lives in memory**.

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

| | 100k documents | 1M documents | Target |
|---|---|---|---|
| Search p95 | **3.6 ms** | 67.6 ms | < 30 ms |
| Search p99 | **4.5 ms** | 68.5 ms | < 60 ms |
| Autocomplete p95 | **0.09 ms** | 0.1 ms | < 5 ms |
| Indexing | **210k docs/sec** | 161k docs/sec | 10k docs/sec |
| Memory | 104 MiB | 1.0 GiB | — |

Reproduce:

```bash
cargo run --release -p tachyon-bench -- --documents 1000000 --queries 1000
```

Indexing throughput beats the target by more than an order of magnitude.
Search meets the latency target at 100k documents and misses it at 1M **on this
corpus**; see [Known limitations](#known-limitations).

---

## Known limitations

Read this before choosing Tachyon.

**Everything is held in memory.** Writes are durable — they go to a write-ahead
log and are replayed on startup — but the memtable is never flushed into an
on-disk segment, so RAM use grows with the corpus and startup replays the whole
log. At roughly **1.1 KiB per document**, 5M documents needs about 5 GiB, above
the 2.5 GiB the design targets. The segment writer is the single most important
next piece of work: the query engine already reads through a source abstraction
that segments plug into, and the commit protocol (`state.json`, WAL generations,
tombstone bitmaps) is built and tested around them.

**Broad queries scale linearly.** Every matching document is scored. At 1M
documents a query matching 6% of the corpus costs ~68 ms at p95. Real catalogues
are far more selective — and adding a filter already halves it — but the fix is
block-max WAND early termination, which would let the executor skip documents
that cannot reach the top-K.

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
cargo test                   # 318 tests
cargo clippy --all-targets
```

The workspace is layered so each crate depends only on the ones below it:

```text
tachyon-server   REST API, auth, analytics, metrics, the binary
tachyon-engine   collection lifecycle, write path, recovery
tachyon-query    parsing, planning, scoring, ranking
tachyon-index    tokenizer, inverted index, columns, fuzzy matching
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
