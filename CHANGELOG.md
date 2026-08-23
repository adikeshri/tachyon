# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

Tachyon is pre-1.0 (crate version stays `0.1.0`); the versions below are the
Docker image tags on [Docker Hub](https://hub.docker.com/r/adikeshri/tachyon)
that `adikeshri/tachyon:latest` has pointed to, not semver crate releases.

## [4.2.1] — 2026-08-23

### Changed

- Runtime base image bumped `alpine:3.21` → `3.24`.

### Fixed

- `docker run` command corrected in the README quickstart.

## [4.2.0] — 2026-08-22

### Performance

- Flushes now stream to disk off the collection write lock, the same as
  merges since 4.1.0 — worst-case search latency under concurrent
  flush-under-load fell from 3.7 s to 233 ms (merge off-lock) to **106 ms**
  (flush off-lock too).

## [4.1.0] — 2026-08-20

### Performance

- Segment merges run off the collection write lock: a search or write is now
  blocked only for a brief snapshot/commit, not for the length of a merge.

## [4.0.0] — 2026-08-20

### Changed

- On-disk segment format bumped to v4: flush and merge now stream to bounded
  memory instead of rebuilding a merge's input through a scratch in-memory
  index, cutting peak RSS at 5M documents from 5.2 GiB to roughly 1.5–1.9
  GiB.

## [3.0.0] — 2026-08-18

### Performance

- Query latency cut 2–2.7x by removing per-document allocation and dispatch
  overhead on the hot search path.

## [2.1.0] — 2026-08-17

### Added

- True block-max WAND query pruning: skips whole regions of a term's
  postings, and whole documents, without decoding them. `found` and facets
  stay exact only when nothing was skipped, signaled via `found_is_exact`.

## [2.0.0] — 2026-08-16

### Added

- Tiered segment merges, triggered once a collection holds more than
  `--merge-trigger-segments` (default 8) segments.
- Segment writer with lazy, mmap'd reads: postings, columns, and document
  values are decoded only for what a query actually touches.

## [1.1.0] — 2026-08-14

### Fixed

- Reliability fixes across the query and storage layers.

### Changed

- Hot-path optimizations across query and storage.
- Runtime Docker image no longer carries `wget`; healthcheck uses the
  Tachyon binary directly.

## [1.0.0] — 2026-08-14

Initial public release: BM25 relevance with per-field boosts, phrase
matching and term proximity; typo-tolerant search and autocomplete via
Damerau-Levenshtein matching; filters (`=`, `!=`, `<`, `<=`, `>`, `>=`,
ranges, set membership, `&&`, `||`); facets; multi-clause sorting; query
analytics; Prometheus metrics; API key auth; a crash-safe write path backed
by a write-ahead log; and a single-binary Docker image.

[4.2.1]: https://github.com/adikeshri/tachyon/compare/v4.2.0...v4.2.1
[4.2.0]: https://github.com/adikeshri/tachyon/compare/v4.1.0...v4.2.0
[4.1.0]: https://github.com/adikeshri/tachyon/compare/v4.0.0...v4.1.0
[4.0.0]: https://github.com/adikeshri/tachyon/compare/v3.0.0...v4.0.0
[3.0.0]: https://github.com/adikeshri/tachyon/compare/v2.1.0...v3.0.0
[2.1.0]: https://github.com/adikeshri/tachyon/compare/v2.0.0...v2.1.0
[2.0.0]: https://github.com/adikeshri/tachyon/compare/v1.1.0...v2.0.0
[1.1.0]: https://github.com/adikeshri/tachyon/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/adikeshri/tachyon/releases/tag/v1.0.0
