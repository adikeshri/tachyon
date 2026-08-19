//! On-disk segments: encode a flushed [`MemTable`](crate::memtable::MemTable)
//! into immutable segment files, and read them back — lazily — into an
//! [`IndexSource`].
//!
//! Five files per segment id (`.terms`, `.ids`, `.post`, `.col`, `.doc`)
//! under the `segments/<id>.<ext>` convention `tachyon-storage::Layout`
//! already provides. Writing the bytes to disk and committing them into
//! `state.json` is `tachyon-engine`'s job — [`codec`] only turns a memtable
//! into bytes and back; [`reader`] is what maps those bytes into memory and
//! decodes them on demand.

mod codec;
mod cursor;
mod format;
mod merge;
mod reader;

pub use codec::{encode, encode_streaming, EncodedSegment};
pub use merge::{merge_segments, MergeInput, MergeStats};
pub use reader::{SegmentFilePaths, SegmentReader};
