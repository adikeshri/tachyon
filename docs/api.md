# Tachyon API reference

Everything is JSON over HTTP. The server listens on `8108` by default.

## Contents

- [Errors](#errors)
- [Authentication](#authentication)
- [Collections](#collections)
- [Documents](#documents)
- [Search](#search)
- [Filters](#filters)
- [Sorting](#sorting)
- [Facets](#facets)
- [Autocomplete](#autocomplete)
- [Analytics](#analytics)
- [Operations](#operations)

---

## Errors

Every failure has the same shape and carries a stable `code` so clients can
branch without matching on prose.

```json
{ "error": { "code": "collection_not_found", "message": "collection `products` not found" } }
```

| Code | Status | Meaning |
|---|---|---|
| `invalid_schema` | 400 | The submitted schema is not legal |
| `invalid_document` | 400 | A document does not match the schema |
| `invalid_query` | 400 | A search request could not be parsed or planned |
| `invalid_json` | 400 | The request body is not valid JSON |
| `unauthorized` | 401 | Missing or wrong API key |
| `forbidden` | 403 | A search key attempted a write |
| `collection_not_found` | 404 | No such collection |
| `document_not_found` | 404 | No such document |
| `collection_exists` | 409 | That collection name is taken |
| `corrupt_data` | 500 | On-disk state failed an integrity check |
| `io_error`, `internal_error` | 500 | Something broke on our side |

---

## Authentication

Send the key in `X-TACHYON-API-KEY`.

```bash
curl -H 'X-TACHYON-API-KEY: your-admin-key' localhost:8108/collections
```

- **Admin key** (`--admin-key`) — reads and writes.
- **Search key** (`--search-key`) — `GET` only. Safe to ship to a browser.
- `/health` is always reachable without a key, so load balancers can probe it.
- With neither key set, every endpoint is open.

---

## Collections

### `POST /collections`

```json
{
  "name": "products",
  "fields": [
    {"name": "title",       "type": "text"},
    {"name": "brand",       "type": "keyword", "facet": true},
    {"name": "price",       "type": "int",     "filter": true, "sort": true},
    {"name": "description", "type": "text"}
  ]
}
```

Returns `201` with the stored schema. Field types are immutable after creation.

**Field types:** `text` (tokenized, searchable), `keyword` (exact, for facets
and filters), `int`, `float`, `bool`, `date` (RFC 3339 or epoch milliseconds).

**Field attributes:**

| Attribute | Default | Meaning |
|---|---|---|
| `facet` | `false` | Build a facet column. Implies filterable. |
| `filter` | `false` | Allow filter expressions on this field |
| `sort` | `false` | Allow sorting on this field |
| `index` | `true` | Include `text` content in the inverted index |
| `optional` | `true` | Allow documents that omit the field |
| `boost` | see below | Per-field relevance multiplier |

`boost` defaults to `10` for a field named `title`, `6` for `brand`, `2` for
`description`, and `1` otherwise, following the PRD's table. Set it explicitly
to override.

**Collection-level options:**

```json
{
  "typo_tolerance": {
    "enabled": true,
    "one_typo_min_len": 4,
    "two_typo_min_len": 8,
    "max_typos": 2
  },
  "default_sorting_field": "popularity"
}
```

`id` is reserved: every document has one implicitly and it is always a string.
Names starting with `_` are reserved.

### `GET /collections`
### `GET /collections/{name}`

The schema plus `num_documents` and `num_segments`.

### `DELETE /collections/{name}`

`204`. Removes the collection and all its data.

---

## Documents

### `POST /collections/{name}/documents`

Accepts an array (or a single object). Documents are upserted by `id`.

```json
[
  {"id": "1", "title": "Wireless Mouse", "brand": "Logitech", "price": 2999},
  {"id": "2", "title": "Mechanical Keyboard", "brand": "Razer", "price": 8999}
]
```

Always `200` when the request itself is well-formed. Individual documents can
fail without failing their neighbours:

```json
{
  "num_indexed": 1,
  "num_failed": 1,
  "results": [
    {"success": true, "id": "1"},
    {"success": false, "code": "invalid_document",
     "error": "field `price`: expected an integer, got a string"}
  ]
}
```

Fields not declared in the schema are stored and returned in search hits, but
are not indexed, filterable, or sortable.

### `GET /collections/{name}/documents/{id}`
### `DELETE /collections/{name}/documents/{id}`

---

## Search

### `GET /collections/{name}/search`

| Parameter | Default | Meaning |
|---|---|---|
| `q` | `""` | Query text. Empty matches everything. |
| `query_by` | all `text` fields | Comma-separated fields to search |
| `filter` | none | Filter expression |
| `sort` | relevance | Sort expression |
| `facet` | none | Comma-separated fields to count |
| `limit` | `10` | Page size, max 250 |
| `offset` | `0` | `offset + limit` must not exceed 10,000 |
| `prefix` | `true` | Prefix-match the final token |
| `typo_tolerance` | collection setting | Allow typo correction |
| `match_mode` | `all` | `all` requires every token, `any` requires one |

**Response:**

```json
{
  "found": 1240,
  "search_time_ms": 12,
  "hits": [ { "document": { }, "text_match": 123.4 } ],
  "facets": { "brand": { "Logitech": 1240, "Razer": 830 } }
}
```

`found` is the total number of matching documents, not the page size.

**Phrases.** Double quotes require adjacency: `wireless "mouse pad"` needs
`mouse` immediately followed by `pad`, within a single field. Phrases are never
prefix-expanded or typo-corrected.

**Typo tolerance** (PRD §7.4), by the length of the token typed:

| Token length | Typos allowed |
|---|---|
| 1–3 | 0 |
| 4–7 | 1 |
| 8+ | 2 |

Distance is unrestricted Damerau-Levenshtein, so a transposition costs one
edit. Exact matches always outrank corrected ones.

**Ranking** combines five normalized signals (PRD §12):

```text
score = 0.45·BM25 + 0.25·field_boost + 0.15·proximity + 0.10·typo_penalty + 0.05·popularity
```

`popularity` reads a field literally named `popularity` if the schema declares
one, and is `0` otherwise. A document is scored on its single best-matching
field, not the sum of all of them.

---

## Filters

```text
brand:=Logitech && price:<5000
(brand:=Logitech || brand:=Razer) && price:[1000..5000]
brand:=[Logitech,Razer] && in_stock:=true
released:>=2024-01-01T00:00:00Z
```

| Operator | Meaning |
|---|---|
| `:=` or `:` | Equal |
| `:!=` | Not equal |
| `:>` `:>=` `:<` `:<=` | Numeric comparison |
| `:[a..b]` | Range, inclusive both ends |
| `:=[a,b,c]` | In the set |
| `:!=[a,b,c]` | Not in the set |
| `&&` `\|\|` | And, or — `&&` binds tighter |
| `( )` | Grouping |

Values containing spaces or punctuation go in single or double quotes:
`brand:="Logitech G Pro"`.

Negation only returns documents that *have* the field: `brand:!=Razer` will not
return a document with no brand, because "unknown" is not "not Razer".

---

## Sorting

```text
sort=_text_match:desc,price:asc
```

Clauses apply left to right; the first that distinguishes two documents wins.
`_text_match` is relevance. Direction is required. Documents missing the sort
field always sort last, in both directions. Doc id is the final tie-break, so
pagination never skips or repeats a document.

---

## Facets

```bash
curl 'localhost:8108/collections/products/search?q=mouse&facet=brand,year'
```

```json
{ "facets": { "brand": { "Logitech": 3, "Razer": 1 }, "year": { "2024": 3, "2023": 1 } } }
```

Counts cover every matching document, not the returned page, and reflect any
`filter` applied. At most 100 values per field, most common first. Faceting
needs `facet: true` on the field.

---

## Autocomplete

### `GET /collections/{name}/suggest`

| Parameter | Default | Meaning |
|---|---|---|
| `q` | — | Text being typed; only the final token is completed |
| `query_by` | all `text` fields | Fields whose terms may be suggested |
| `limit` | `5` | Suggestions to return, max 50 |
| `typo_tolerance` | collection setting | Also suggest corrections |

```bash
curl 'localhost:8108/collections/products/suggest?q=wir'
```

```json
{
  "suggestions": [
    {"text": "wireless", "count": 3, "typos": 0},
    {"text": "wired",    "count": 2, "typos": 0}
  ],
  "search_time_ms": 0
}
```

`count` is the number of live documents behind the suggestion, so a suggestion
never leads to an empty result page. Plain completions always rank above
typo-corrected ones.

---

## Analytics

Searches are recorded automatically. Autocomplete requests are not — a
keystroke is not a search.

### `GET /analytics/top`
### `GET /analytics/zero-results`

Both take `collection` and `limit` (default 20, max 500).

```json
{
  "queries": [
    {"query": "wireless mouse", "collection": "products", "count": 3,
     "zero_result_count": 0, "last_result_count": 12,
     "avg_latency_ms": 1.8, "last_seen": 1786625778150}
  ],
  "tracked_queries": 1,
  "dropped_queries": 0
}
```

`zero-results` ranks by how often a query came back empty, not by whether the
last run did.

### `GET /analytics/latency`

```json
{
  "count": 20, "mean_ms": 1.9, "p50_ms": 2.0, "p95_ms": 4.0,
  "p99_ms": 4.0, "max_ms": 3.4,
  "total_searches": 20, "uptime_seconds": 61, "queries_per_second": 0.33
}
```

Analytics live in memory and reset on restart. At most 10,000 distinct query
strings are tracked; when full, the least-asked half is dropped and counted in
`dropped_queries`.

---

## Operations

### `GET /health`

```json
{"ok": true, "version": "0.1.0", "uptime_seconds": 61, "num_collections": 1}
```

Always reachable without an API key.

### `GET /metrics`

Prometheus exposition format.

| Metric | Type | Labels |
|---|---|---|
| `tachyon_uptime_seconds` | gauge | |
| `tachyon_search_requests_total` | counter | |
| `tachyon_search_queries_per_second` | gauge | |
| `tachyon_search_latency_seconds` | summary | `quantile` |
| `tachyon_analytics_tracked_queries` | gauge | |
| `tachyon_collections` | gauge | |
| `tachyon_collection_documents` | gauge | `collection` |
| `tachyon_collection_segments` | gauge | `collection` |
| `tachyon_collection_memtable_bytes` | gauge | `collection` |
| `tachyon_collection_wal_bytes` | gauge | `collection` |
