## What and why

<!-- What changes, and why. If this closes an issue, write "Closes #123". -->

## Tests

<!-- What did you add or update? What would a reviewer need to see to believe
     this works? -->

## Checklist

- [ ] `cargo test` passes
- [ ] `cargo clippy --all-targets -- -D warnings` is clean
- [ ] `cargo fmt --all` applied
- [ ] If this changes the on-disk format: `STORE_FORMAT_VERSION` or
      `WAL_FORMAT_VERSION` is bumped, and this description says what happens
      to an existing data directory
- [ ] If this changes the API contract or ranking formula: linked to the RFC
      issue it was discussed in
- [ ] If this is a performance change: before/after numbers from
      `cargo run --release -p tachyon-bench` are included
