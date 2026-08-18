//! A [`PostingCursor`] over one (term, field)'s postings in a segment.
//!
//! Only the currently active block's skeleton — doc ids and term
//! frequencies, positions skipped — is ever decoded, from the segment's
//! mmap'd `.post` bytes; a fresh decode happens only when the cursor
//! actually crosses into a new block, whether by plain `advance` or by an
//! `advance_to` jump that lands elsewhere in the directory.

use tachyon_core::DocId;

use crate::cursor::PostingCursor;

use super::codec::{self, BlockMeta, BlockSkeleton, TermFieldBlocks};

pub(crate) struct SegmentPostingCursor<'a> {
    bytes: &'a [u8],
    blocks: Vec<BlockMeta>,
    /// Always `<= blocks.len()`; `blocks.len()` means exhausted.
    block_idx: usize,
    /// `None` when `block_idx` is out of range, or a block failed to
    /// decode — corrupt segment bytes surface as an exhausted cursor rather
    /// than a panic, the same policy every other segment reader in this
    /// crate follows.
    skeleton: Option<BlockSkeleton>,
    doc_idx: usize,
}

impl<'a> SegmentPostingCursor<'a> {
    /// `term_field`'s block directory is consumed into the cursor; nothing
    /// beyond the first block's skeleton is decoded yet.
    pub(crate) fn new(bytes: &'a [u8], term_field: TermFieldBlocks) -> SegmentPostingCursor<'a> {
        let mut cursor = SegmentPostingCursor {
            bytes,
            blocks: term_field.blocks,
            block_idx: 0,
            skeleton: None,
            doc_idx: 0,
        };
        cursor.load_block(0);
        cursor
    }

    fn load_block(&mut self, idx: usize) {
        self.block_idx = idx.min(self.blocks.len());
        self.doc_idx = 0;
        self.skeleton = match self.blocks.get(self.block_idx) {
            Some(meta) => match codec::decode_block_skeleton(self.bytes, meta) {
                Ok(skeleton) => Some(skeleton),
                Err(e) => {
                    tracing::warn!(error = %e, "segment: failed to decode a posting block");
                    None
                }
            },
            None => None,
        };
    }
}

impl PostingCursor for SegmentPostingCursor<'_> {
    fn doc_id(&self) -> Option<DocId> {
        self.skeleton.as_ref()?.doc_ids.get(self.doc_idx).copied()
    }

    fn term_freq(&self) -> u32 {
        self.skeleton.as_ref().and_then(|s| s.tfs.get(self.doc_idx)).copied().unwrap_or(0)
    }

    fn positions(&self) -> Vec<u32> {
        let Some(skeleton) = &self.skeleton else { return Vec::new() };
        let (Some(&offset), Some(&tf)) =
            (skeleton.positions_offsets.get(self.doc_idx), skeleton.tfs.get(self.doc_idx))
        else {
            return Vec::new();
        };
        codec::decode_positions_at(self.bytes, offset, tf).unwrap_or_else(|e| {
            tracing::warn!(error = %e, "segment: failed to decode posting positions");
            Vec::new()
        })
    }

    fn positions_into(&self, out: &mut Vec<u32>) {
        let Some(skeleton) = &self.skeleton else { return };
        let (Some(&offset), Some(&tf)) =
            (skeleton.positions_offsets.get(self.doc_idx), skeleton.tfs.get(self.doc_idx))
        else {
            return;
        };
        if let Err(e) = codec::decode_positions_into(self.bytes, offset, tf, out) {
            tracing::warn!(error = %e, "segment: failed to decode posting positions");
        }
    }

    fn advance(&mut self) -> Option<DocId> {
        let len = self.skeleton.as_ref().map_or(0, |s| s.doc_ids.len());
        if self.doc_idx + 1 < len {
            self.doc_idx += 1;
        } else {
            self.load_block(self.block_idx + 1);
        }
        self.doc_id()
    }

    fn advance_to(&mut self, target: DocId) -> Option<DocId> {
        // Inside the currently loaded block: a plain binary search.
        if let Some(skeleton) = &self.skeleton {
            if skeleton.doc_ids.last().is_some_and(|&last| last >= target) {
                self.doc_idx += skeleton.doc_ids[self.doc_idx..].partition_point(|&d| d < target);
                return self.doc_id();
            }
        }

        // Otherwise, binary-search the block directory by `last_doc_id` and
        // jump straight to the one block that could hold `target`, without
        // ever decoding the blocks in between.
        let jump = self.blocks[self.block_idx..].partition_point(|b| b.last_doc_id < target);
        self.load_block(self.block_idx + jump);
        if let Some(skeleton) = &self.skeleton {
            self.doc_idx = skeleton.doc_ids.partition_point(|&d| d < target);
        }
        self.doc_id()
    }

    fn max_remaining_tf(&self) -> u32 {
        self.blocks.get(self.block_idx).map_or(0, |b| b.max_tf)
    }

    fn current_block_last_doc_id(&self) -> Option<DocId> {
        self.blocks.get(self.block_idx).map(|b| b.last_doc_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tachyon_core::{CollectionSchema, FieldSchema, FieldType, ParsedDocument};

    use crate::memtable::MemTable;
    use crate::segment::format::TERMS_MAGIC;

    use codec::{decode_post_header, POSTING_BLOCK_SIZE};

    fn schema() -> CollectionSchema {
        CollectionSchema::new(
            "products",
            vec![
                FieldSchema::new("title", FieldType::Text),
                FieldSchema::new("tags", FieldType::Text),
            ],
        )
    }

    /// `n` documents; doc `i` matches "mouse" in `title` only when `i` is
    /// even, at position 0 — a sparse posting list with real gaps in doc id,
    /// so `advance_to` has genuinely absent ids to be tested against, not
    /// just present ones.
    fn sparse_fixture(n: usize) -> (MemTable, CollectionSchema) {
        let schema = schema();
        let mut m = MemTable::new(0, &schema);
        for i in 0..n {
            let title = if i % 2 == 0 { "mouse" } else { "keyboard" };
            m.insert(
                ParsedDocument::parse(
                    json!({ "id": i.to_string(), "title": title, "tags": [] }),
                    &schema,
                )
                .unwrap(),
            );
        }
        (m, schema)
    }

    fn open_cursor<'a>(
        post: &'a [u8],
        terms: &fst::Map<Vec<u8>>,
        term: &str,
        field: u16,
    ) -> SegmentPostingCursor<'a> {
        let header = decode_post_header(post).unwrap();
        let term_id = terms.get(term).unwrap();
        let term_field =
            codec::decode_term_field_blocks(post, &header, term_id, field).unwrap().unwrap();
        SegmentPostingCursor::new(post, term_field)
    }

    fn load_fst(bytes: &[u8]) -> fst::Map<Vec<u8>> {
        let start = codec::validate_header(bytes, TERMS_MAGIC, "test fst").unwrap();
        fst::Map::new(bytes[start..].to_vec()).unwrap()
    }

    #[test]
    fn agrees_with_a_plain_linear_reference_walk_across_multiple_blocks() {
        let n = POSTING_BLOCK_SIZE * 4; // 256 "mouse" postings: doc ids 0, 2, .., 510
        let (m, schema) = sparse_fixture(n);
        let encoded = super::super::encode(&m, &schema).unwrap();
        let terms = load_fst(&encoded.terms);

        let expected: Vec<(DocId, u32, Vec<u32>)> = m
            .index()
            .postings("mouse", 0)
            .unwrap()
            .docs
            .iter()
            .map(|d| (d.doc_id, d.tf(), d.positions.clone()))
            .collect();
        assert_eq!(expected.len(), n / 2, "sanity: every even doc contains \"mouse\"");

        let mut cursor = open_cursor(&encoded.post, &terms, "mouse", 0);
        let mut got = Vec::new();
        while let Some(doc_id) = cursor.doc_id() {
            got.push((doc_id, cursor.term_freq(), cursor.positions()));
            cursor.advance();
        }
        assert_eq!(got, expected);
    }

    #[test]
    fn advance_to_is_correct_at_and_around_a_block_boundary() {
        let n = POSTING_BLOCK_SIZE * 4;
        let (m, schema) = sparse_fixture(n);
        let encoded = super::super::encode(&m, &schema).unwrap();
        let terms = load_fst(&encoded.terms);
        let header = decode_post_header(&encoded.post).unwrap();
        let term_id = terms.get("mouse").unwrap();
        let term_field =
            codec::decode_term_field_blocks(&encoded.post, &header, term_id, 0).unwrap().unwrap();
        assert_eq!(term_field.blocks.len(), 2, "256 postings at block size 128 is two full blocks");
        let block0_last = term_field.blocks[0].last_doc_id; // 254 (128th even id, 0-indexed)

        // Exact last doc id of block 0.
        let mut cursor = open_cursor(&encoded.post, &terms, "mouse", 0);
        assert_eq!(cursor.advance_to(block0_last), Some(block0_last));
        assert_eq!(cursor.current_block_last_doc_id(), Some(block0_last), "still inside block 0");

        // An id absent from both blocks (odd, between the two boundaries)
        // must land on the next real doc, which is block 1's first.
        let mut cursor = open_cursor(&encoded.post, &terms, "mouse", 0);
        let landed = cursor.advance_to(block0_last + 1).unwrap();
        assert_eq!(landed, block0_last + 2, "the next even doc id, now inside block 1");
        assert_eq!(cursor.current_block_last_doc_id(), Some(term_field.blocks[1].last_doc_id));

        // Exact first doc id of block 1.
        let mut cursor = open_cursor(&encoded.post, &terms, "mouse", 0);
        assert_eq!(cursor.advance_to(block0_last + 2), Some(block0_last + 2));

        // A target inside a block's own range but absent (odd id) lands on
        // the next present doc within that same block.
        let mut cursor = open_cursor(&encoded.post, &terms, "mouse", 0);
        assert_eq!(cursor.advance_to(101), Some(102));

        // Past every doc id: exhausts without panicking.
        let mut cursor = open_cursor(&encoded.post, &terms, "mouse", 0);
        assert_eq!(cursor.advance_to(n as DocId + 1000), None);
        assert_eq!(cursor.advance_to(0), None, "stays exhausted");
        assert_eq!(cursor.advance(), None, "and never panics on a further advance");
    }
}
