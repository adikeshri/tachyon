//! Index structures for Tachyon.
//!
//! The mutable side lives in [`memtable`]; immutable segments, the inverted
//! index, and the columnar stores are layered on top of it.

pub mod columns;
pub mod fuzzy;
pub mod inverted;
pub mod memtable;
pub mod segment;
pub mod source;
pub mod tokenizer;

pub use columns::{Columns, KeywordColumn, NumKey, NumericColumn};
pub use fuzzy::{distance_within, FuzzyMatcher};
pub use inverted::{DocPosting, FieldPostings, InvertedIndex};
pub use memtable::{MemTable, StoredDoc};
pub use segment::{encode, EncodedSegment, SegmentFilePaths, SegmentReader};
pub use source::IndexSource;
pub use tokenizer::{tokenize, Token};
