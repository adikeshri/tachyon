//! Lazy, doc-at-a-time postings iteration.
//!
//! A pruning search needs to ask a term's postings "what's your next doc?"
//! and "skip ahead to here" without paying to decode or hold every posting
//! in between — [`PostingCursor`] is that interface. [`MemTablePostingCursor`]
//! is one concrete shape (a borrowed, already-sorted, already-resident
//! slice); `segment::cursor::SegmentPostingCursor` (block-structured, lazily
//! decoded from an mmap) is the other. [`MergeCursor`] folds several
//! same-term-and-field cursors — the memtable plus every committed segment —
//! into one logical cursor over the whole collection.

use tachyon_core::DocId;

use crate::inverted::DocPosting;

/// Doc-at-a-time access to one term's postings in one field, from one
/// source.
///
/// A cursor starts positioned at its first posting, if any, and exposes the
/// *current* doc without consuming it — what lets a pruning driver inspect a
/// bound before deciding whether a document is even worth visiting.
pub trait PostingCursor {
    /// The doc id currently under the cursor, or `None` once exhausted.
    fn doc_id(&self) -> Option<DocId>;

    /// Term frequency at the current doc id. `0` once exhausted.
    fn term_freq(&self) -> u32;

    /// Positions at the current doc id, decoded only now — a document a
    /// bound proves unnecessary to visit never pays this cost.
    fn positions(&self) -> Vec<u32>;

    /// Move to the next doc id.
    fn advance(&mut self) -> Option<DocId>;

    /// Move forward to the first doc id `>= target`. A no-op if the cursor
    /// is already there or past it — this never moves backward.
    fn advance_to(&mut self, target: DocId) -> Option<DocId>;

    /// Upper bound on `term_freq` for any doc in the cursor's CURRENT block.
    fn max_remaining_tf(&self) -> u32;

    /// The current block's last doc id, for skip-ahead when even this
    /// block's own bound can't clear a threshold. `None` for sources with
    /// no block structure (the memtable).
    fn current_block_last_doc_id(&self) -> Option<DocId>;
}

/// A cursor over a memtable's already-sorted, already-resident posting list.
/// Every read is a slice index — there is nothing to decode.
pub struct MemTablePostingCursor<'a> {
    docs: &'a [DocPosting],
    pos: usize,
    /// Whole-list maximum, computed once at construction — sound but not
    /// block-tight. Fine: the memtable is small and RAM-resident, so a loose
    /// bound here doesn't cost what it would on a large segment.
    max_tf: u32,
}

impl<'a> MemTablePostingCursor<'a> {
    pub fn new(docs: &'a [DocPosting]) -> MemTablePostingCursor<'a> {
        let max_tf = docs.iter().map(DocPosting::tf).max().unwrap_or(0);
        MemTablePostingCursor { docs, pos: 0, max_tf }
    }
}

impl PostingCursor for MemTablePostingCursor<'_> {
    fn doc_id(&self) -> Option<DocId> {
        self.docs.get(self.pos).map(|d| d.doc_id)
    }

    fn term_freq(&self) -> u32 {
        self.docs.get(self.pos).map_or(0, DocPosting::tf)
    }

    fn positions(&self) -> Vec<u32> {
        self.docs.get(self.pos).map_or_else(Vec::new, |d| d.positions.clone())
    }

    fn advance(&mut self) -> Option<DocId> {
        self.pos += 1;
        self.doc_id()
    }

    fn advance_to(&mut self, target: DocId) -> Option<DocId> {
        self.pos += self.docs[self.pos.min(self.docs.len())..]
            .partition_point(|d| d.doc_id < target);
        self.doc_id()
    }

    fn max_remaining_tf(&self) -> u32 {
        self.max_tf
    }

    fn current_block_last_doc_id(&self) -> Option<DocId> {
        None
    }
}

/// A k-way union merge of one term-field's cursors across every source —
/// the memtable plus every committed segment.
///
/// Doc id ranges are disjoint across sources (each source owns a
/// contiguous, non-overlapping range of ids), so at most one child ever
/// holds any particular doc id — but several children can be simultaneously
/// live, each pointing at its own smallest not-yet-visited doc in its own
/// range. Finding whichever is smallest is a linear scan over the live
/// children, which isn't worth a heap at this fan-in (memtable plus a
/// handful of segments, not dozens).
pub struct MergeCursor<'a> {
    children: Vec<Box<dyn PostingCursor + 'a>>,
    /// Index into `children` of whichever cursor currently holds the
    /// smallest live doc id — recomputed whenever a child moves.
    current: Option<usize>,
}

impl<'a> MergeCursor<'a> {
    pub fn new(children: Vec<Box<dyn PostingCursor + 'a>>) -> MergeCursor<'a> {
        let mut merged = MergeCursor { children, current: None };
        merged.find_min();
        merged
    }

    fn find_min(&mut self) {
        self.current = self
            .children
            .iter()
            .enumerate()
            .filter_map(|(i, c)| c.doc_id().map(|d| (i, d)))
            .min_by_key(|&(_, d)| d)
            .map(|(i, _)| i);
    }
}

impl PostingCursor for MergeCursor<'_> {
    fn doc_id(&self) -> Option<DocId> {
        self.current.and_then(|i| self.children[i].doc_id())
    }

    fn term_freq(&self) -> u32 {
        self.current.map_or(0, |i| self.children[i].term_freq())
    }

    fn positions(&self) -> Vec<u32> {
        self.current.map_or_else(Vec::new, |i| self.children[i].positions())
    }

    fn advance(&mut self) -> Option<DocId> {
        if let Some(i) = self.current {
            self.children[i].advance();
        }
        self.find_min();
        self.doc_id()
    }

    fn advance_to(&mut self, target: DocId) -> Option<DocId> {
        for child in &mut self.children {
            if child.doc_id().is_some_and(|d| d < target) {
                child.advance_to(target);
            }
        }
        self.find_min();
        self.doc_id()
    }

    /// Only the current child's doc id is reachable next — the score bound
    /// for a *specific* doc id can only ever come from whichever child owns
    /// that id, never from a different child sitting at some other, larger
    /// doc id.
    fn max_remaining_tf(&self) -> u32 {
        self.current.map_or(0, |i| self.children[i].max_remaining_tf())
    }

    /// The min across every LIVE child's own block boundary, not just the
    /// current one: several children can be simultaneously live at
    /// different doc ids, and skipping ahead based only on the current
    /// child's (possibly wider) block would risk jumping straight over a
    /// real match another child is already sitting on. `None` if any live
    /// child has no block info (a memtable child) — an unbounded live child
    /// makes any skip-ahead here unsound, same as the single-cursor case.
    fn current_block_last_doc_id(&self) -> Option<DocId> {
        let mut min: Option<DocId> = None;
        for child in &self.children {
            if child.doc_id().is_none() {
                continue;
            }
            let last = child.current_block_last_doc_id()?;
            min = Some(min.map_or(last, |m| m.min(last)));
        }
        min
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn postings(pairs: &[(DocId, &[u32])]) -> Vec<DocPosting> {
        pairs.iter().map(|&(doc_id, p)| DocPosting { doc_id, positions: p.to_vec() }).collect()
    }

    #[test]
    fn memtable_cursor_walks_in_order() {
        let docs = postings(&[(1, &[0]), (3, &[0, 5]), (7, &[2])]);
        let mut cur = MemTablePostingCursor::new(&docs);

        assert_eq!(cur.doc_id(), Some(1));
        assert_eq!(cur.term_freq(), 1);
        assert_eq!(cur.positions(), vec![0]);

        assert_eq!(cur.advance(), Some(3));
        assert_eq!(cur.term_freq(), 2);
        assert_eq!(cur.positions(), vec![0, 5]);

        assert_eq!(cur.advance(), Some(7));
        assert_eq!(cur.advance(), None, "exhausted after the last doc");
        assert_eq!(cur.term_freq(), 0);
        assert!(cur.positions().is_empty());
        assert_eq!(cur.advance(), None, "stays exhausted, never panics");
    }

    #[test]
    fn memtable_cursor_advance_to_never_moves_backward() {
        let docs = postings(&[(1, &[0]), (5, &[0]), (10, &[0]), (20, &[0])]);
        let mut cur = MemTablePostingCursor::new(&docs);

        assert_eq!(cur.advance_to(5), Some(5), "lands exactly on a present doc id");
        assert_eq!(cur.advance_to(3), Some(5), "a target behind current is a no-op");
        assert_eq!(cur.advance_to(11), Some(20), "an absent id lands on the next real doc");
        assert_eq!(cur.advance_to(1000), None, "past the last doc id exhausts the cursor");
        assert_eq!(cur.advance_to(1), None, "stays exhausted regardless of target");
    }

    #[test]
    fn memtable_cursor_max_remaining_tf_is_the_whole_list_maximum() {
        let docs = postings(&[(1, &[0]), (2, &[0, 1, 2]), (3, &[0])]);
        let mut cur = MemTablePostingCursor::new(&docs);
        assert_eq!(cur.max_remaining_tf(), 3);
        cur.advance();
        assert_eq!(cur.max_remaining_tf(), 3, "not block-tight, but still sound after moving");
        assert_eq!(cur.current_block_last_doc_id(), None, "the memtable has no block structure");
    }

    #[test]
    fn merge_cursor_unions_disjoint_sources_in_doc_id_order() {
        let a = postings(&[(1, &[0]), (5, &[0])]);
        let b = postings(&[(2, &[0]), (3, &[0]), (10, &[0])]);
        let mut merged = MergeCursor::new(vec![
            Box::new(MemTablePostingCursor::new(&a)) as Box<dyn PostingCursor>,
            Box::new(MemTablePostingCursor::new(&b)) as Box<dyn PostingCursor>,
        ]);

        let mut seen = Vec::new();
        while let Some(id) = merged.doc_id() {
            seen.push(id);
            merged.advance();
        }
        assert_eq!(seen, vec![1, 2, 3, 5, 10]);
    }

    #[test]
    fn merge_cursor_advance_to_catches_up_every_child_behind_the_target() {
        let a = postings(&[(1, &[0]), (5, &[0]), (9, &[0])]);
        let b = postings(&[(2, &[0]), (3, &[0]), (8, &[0])]);
        let mut merged = MergeCursor::new(vec![
            Box::new(MemTablePostingCursor::new(&a)) as Box<dyn PostingCursor>,
            Box::new(MemTablePostingCursor::new(&b)) as Box<dyn PostingCursor>,
        ]);

        assert_eq!(merged.advance_to(6), Some(8), "both children jump past the target together");
        let mut rest = vec![merged.doc_id().unwrap()];
        while let Some(id) = merged.advance() {
            rest.push(id);
        }
        assert_eq!(rest, vec![8, 9]);
    }

    /// A hand-controlled cursor exposing arbitrary block info, for testing
    /// [`MergeCursor`]'s block-boundary logic without needing a real segment.
    struct FixedCursor {
        docs: Vec<(DocId, u32)>,
        pos: usize,
        block_last_doc_id: Option<DocId>,
    }

    impl PostingCursor for FixedCursor {
        fn doc_id(&self) -> Option<DocId> {
            self.docs.get(self.pos).map(|&(id, _)| id)
        }
        fn term_freq(&self) -> u32 {
            self.docs.get(self.pos).map_or(0, |&(_, tf)| tf)
        }
        fn positions(&self) -> Vec<u32> {
            Vec::new()
        }
        fn advance(&mut self) -> Option<DocId> {
            self.pos += 1;
            self.doc_id()
        }
        fn advance_to(&mut self, target: DocId) -> Option<DocId> {
            while self.doc_id().is_some_and(|d| d < target) {
                self.pos += 1;
            }
            self.doc_id()
        }
        fn max_remaining_tf(&self) -> u32 {
            self.docs.get(self.pos..).map_or(0, |rest| rest.iter().map(|&(_, tf)| tf).max().unwrap_or(0))
        }
        fn current_block_last_doc_id(&self) -> Option<DocId> {
            self.block_last_doc_id
        }
    }

    #[test]
    fn merge_cursor_current_block_last_doc_id_is_the_min_across_live_children() {
        // Child A sits at doc 10 with a wide block boundary (100); child B
        // sits at doc 20 (still live, further ahead) with a much tighter
        // boundary (25). The merged skip-ahead bound must respect B's
        // tighter limit too, or a real match B is already sitting on could
        // be skipped straight over.
        let a = FixedCursor { docs: vec![(10, 1), (200, 1)], pos: 0, block_last_doc_id: Some(100) };
        let b = FixedCursor { docs: vec![(20, 1), (22, 1)], pos: 0, block_last_doc_id: Some(25) };
        let merged =
            MergeCursor::new(vec![Box::new(a) as Box<dyn PostingCursor>, Box::new(b) as Box<dyn PostingCursor>]);
        assert_eq!(merged.doc_id(), Some(10), "A holds the smaller current doc id");
        assert_eq!(merged.current_block_last_doc_id(), Some(25), "bounded by B's tighter block");
    }

    #[test]
    fn merge_cursor_current_block_last_doc_id_is_none_if_any_live_child_lacks_block_info() {
        let segment_like = FixedCursor { docs: vec![(1, 1)], pos: 0, block_last_doc_id: Some(50) };
        let mem = postings(&[(2, &[0])]);
        let merged = MergeCursor::new(vec![
            Box::new(segment_like) as Box<dyn PostingCursor>,
            Box::new(MemTablePostingCursor::new(&mem)) as Box<dyn PostingCursor>,
        ]);
        assert_eq!(merged.current_block_last_doc_id(), None, "the memtable child has no block bound");
    }
}
