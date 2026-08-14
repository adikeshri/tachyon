# Contributing to Tachyon

Thanks for wanting to help.

## Getting set up

Rust 1.85 or newer.

```bash
cargo test                          # the whole suite
cargo clippy --all-targets -- -D warnings
cargo fmt --all
cargo run --release -p tachyon-bench # before and after a performance change
```

CI runs all of the above plus a Docker build and a container smoke test.

## Where things live

| Crate | Responsibility |
|---|---|
| `tachyon-core` | Schema, values, documents, errors. No I/O, no state. |
| `tachyon-storage` | Write-ahead log, on-disk layout, metadata. Knows nothing about posting lists. |
| `tachyon-index` | Tokenizer, inverted index, columns, fuzzy matching. |
| `tachyon-query` | Parsing, planning, scoring, ranking. Opens no files. |
| `tachyon-engine` | Collection lifecycle, write path, recovery. |
| `tachyon-server` | REST API, auth, analytics, metrics, the binary. |

Dependencies point downwards only. If a change needs an arrow the other way,
that is usually a sign the responsibility is in the wrong crate — worth raising
in the issue before writing it.

[`docs/architecture.md`](docs/architecture.md) explains why the main structures
are shaped the way they are. Read it before changing the write path, the
accumulator, or anything touching the on-disk format.

## RFCs

Anything that changes the on-disk format, the API contract, or the ranking
formula should start as an issue labelled `rfc` describing the problem, the
options, and the trade-off you are choosing. Small fixes do not need one.

## Tests

Every change needs tests, and the bar is what a reviewer needs to believe the
code works:

- Test behaviour, not implementation. A test that breaks when you rename a
  private function is a maintenance cost with no benefit.
- Cover the edges the code actually has: empty input, a single element, values
  at a boundary, absent fields, deleted documents.
- Name the test after the property it establishes —
  `deleted_documents_are_invisible`, not `test_search_2`.
- Failure messages should say what went wrong. `assert_eq!(a, b, "not
  descending: {scores:?}")` beats a bare comparison.
- API changes get an end-to-end test in `crates/tachyon-server/tests/`, driving
  the router in-process. No sockets, no ports, no sleeping.

## Changing the on-disk format

`STORE_FORMAT_VERSION` and `WAL_FORMAT_VERSION` exist so an old binary refuses
to misread new files instead of corrupting them. Bump the relevant one in the
same commit as the format change, and say in the pull request what happens to
an existing data directory.

## Performance work

Bring numbers. `cargo run --release -p tachyon-bench` before and after, with
the document and query counts you used — a claim about a hot path without a
measurement is a hypothesis.

## Commits and pull requests

Explain *why* in the message, not just what; the diff already says what.
Keep unrelated changes in separate commits.
