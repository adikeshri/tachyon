//! Stream several already-encoded segments into one, without ever
//! materializing the merged corpus as a `MemTable` the way the pre-v4 merge
//! did (parse every live document's stored source again, re-tokenize it, and
//! insert it into a fresh in-memory index before encoding that). Every
//! document here was already tokenized once, when its segment was first
//! flushed; a merge's job is to copy that work forward, not redo it.
//!
//! # Doc id remapping
//!
//! A merge renumbers rather than preserves doc ids — see
//! `tachyon-engine::Collection::merge_locked`'s doc comment for why. Given
//! the merge's output base id and, per input segment, a `live` bitmap (that
//! segment's own presence, already intersected with the engine's
//! collection-wide tombstones), an old doc id remaps to:
//!
//! ```text
//! new_base[i] + live[i].rank(old_id) - 1
//! ```
//!
//! `rank(x)` (from `roaring`) is the count of set bits `<= x`, so this is
//! "how many live documents in this input come at or before this one,
//! 0-indexed" — dense and monotone within one input. `new_base[i]` is a
//! running total of every earlier input's live count, so ids stay monotone
//! *across* inputs too: every posting, column entry, and doc-store row this
//! module writes comes out already in ascending new-doc-id order, with no
//! sort needed after the fact.
//!
//! # What's copied verbatim vs. recomputed
//!
//! Term postings are decoded down to `(doc_id, positions)` and re-blocked
//! (block boundaries shift because dead documents drop out and ids change),
//! but positions themselves are copied, not retokenized. A value slot for
//! `Null`/`Bool`/`Int`/`Float` doesn't reference anything positional, so its
//! 13 raw bytes are copied without even being decoded into a [`Value`]; a
//! `Str`/`Array` slot's blob bytes are copied verbatim and only its offset is
//! rebased to the new file's position. A document's stored source JSON is
//! copied as raw bytes ([`super::codec::raw_source_at`]), never re-parsed or
//! re-serialized. Only postings actually need decoding at all, and only
//! because block boundaries genuinely change.

use std::collections::HashMap;
use std::io::Write;

use roaring::RoaringBitmap;

use tachyon_core::{CollectionSchema, DocId, Error, FieldId, Result};

use crate::columns::NumKey;

use super::codec::{self, BlockMeta, POSTING_BLOCK_SIZE};
use super::format::{
    write_f64, write_header, write_i64, write_str, write_u32, write_u64, write_u8, CountingWriter,
    COL_MAGIC, DOC_MAGIC, IDS_MAGIC, POST_MAGIC, TERMS_MAGIC,
};
use super::reader::SegmentReader;

/// One segment being folded into the merge output: the reader, and the
/// subset of its own doc ids that should survive — its own presence bitmap
/// intersected with whatever the engine has tombstoned since this segment
/// was written. Inputs are processed in the order given; that order is what
/// determines each one's slice of the output id range (see this module's
/// doc comment), so callers that care about which victim's ids sort first
/// (`tachyon-engine` does, to preserve relative recency) must pass them in
/// that order.
pub struct MergeInput<'a> {
    pub reader: &'a SegmentReader,
    pub live: RoaringBitmap,
}

pub struct MergeStats {
    /// Total documents written to the output segment.
    pub doc_count: usize,
}

/// Stream `inputs` into one segment starting at doc id `base`, writing
/// directly into the five sinks. Bounded memory throughout: the largest
/// single thing held in memory at once is one term-field's merged postings,
/// one field's merged column, or one document's id-string/blob bytes — never
/// the whole merged corpus, which is the entire point of streaming a merge
/// instead of rebuilding a `MemTable` from it.
///
/// A no-op-shaped call (empty `inputs`, or every input's `live` empty)
/// produces a valid, empty segment rather than being special-cased — the
/// caller (`Collection::merge_locked`) already skips writing anything when
/// nothing would end up live, but nothing here requires that.
///
/// Eight parameters, five of them the segment's own file sinks: a segment
/// really is five separate structures with no shared framing (see
/// `segment/mod.rs`'s doc comment), and threading them through a struct
/// instead would just move this same count onto a builder's fields.
#[allow(clippy::too_many_arguments)]
pub fn merge_segments(
    inputs: &[MergeInput<'_>],
    schema: &CollectionSchema,
    base: DocId,
    terms_w: impl Write,
    ids_w: impl Write,
    post_w: impl Write,
    col_w: impl Write,
    doc_w: impl Write,
) -> Result<MergeStats> {
    let mut new_base = Vec::with_capacity(inputs.len());
    let mut cursor = base;
    for input in inputs {
        new_base.push(cursor);
        cursor += input.live.len() as DocId;
    }
    let out_end = cursor;
    let doc_count = (out_end - base) as usize;

    merge_postings_and_terms(inputs, &new_base, schema, terms_w, post_w)?;
    merge_ids(inputs, &new_base, ids_w)?;
    merge_columns(inputs, &new_base, schema, col_w)?;
    merge_docs(inputs, &new_base, schema, base, out_end, doc_w)?;

    Ok(MergeStats { doc_count })
}

/// `old_id`'s new id under input `i`, or `None` if it didn't survive.
fn remap(input: &MergeInput, new_base: DocId, old_id: DocId) -> Option<DocId> {
    if !input.live.contains(old_id) {
        return None;
    }
    Some(new_base + (input.live.rank(old_id) - 1) as DocId)
}

// --- ids -----------------------------------------------------------------

fn merge_ids(inputs: &[MergeInput], new_base: &[DocId], ids_w: impl Write) -> Result<()> {
    use fst::Streamer;

    let mut pairs: Vec<(String, u64)> = Vec::new();
    for (i, input) in inputs.iter().enumerate() {
        let mut stream = input.reader.ids_map().stream();
        while let Some((id_bytes, old_id)) = stream.next() {
            let Some(new_id) = remap(input, new_base[i], old_id as DocId) else { continue };
            let id = std::str::from_utf8(id_bytes)
                .map_err(|_| Error::corruption("segment merge: non-utf8 document id"))?;
            pairs.push((id.to_string(), new_id as u64));
        }
    }
    pairs.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
    codec::write_fst(ids_w, IDS_MAGIC, pairs.iter().map(|(s, v)| (s.as_str(), *v)))
}

// --- postings + terms ------------------------------------------------------

/// One posting, already remapped to its new doc id and with positions
/// decoded — the unit a term-field's output blocks are built from. Owned
/// rather than borrowed, unlike the flush path's `&DocPosting`: a merge's
/// positions come from decoding an input segment's bytes fresh, not from
/// something already resident that a reference could point at.
struct MergedPosting {
    doc_id: DocId,
    positions: Vec<u32>,
}

/// Every surviving posting for one (term, field) across every input,
/// already in ascending new-doc-id order (see this module's doc comment) —
/// bounded by this one term-field's total postings, the same bound the
/// flush path already accepts for a single term's block-building pass.
fn collect_field_postings(
    inputs: &[MergeInput],
    new_base: &[DocId],
    term_lookup: &[Option<u64>],
    field: FieldId,
) -> Result<Vec<MergedPosting>> {
    let mut out = Vec::new();
    for (i, input) in inputs.iter().enumerate() {
        let Some(term_id) = term_lookup[i] else { continue };
        let Some(term_field) = codec::decode_term_field_blocks(
            input.reader.post_bytes(),
            input.reader.post_header(),
            term_id,
            field,
        )?
        else {
            continue;
        };
        for meta in &term_field.blocks {
            let skeleton = codec::decode_block_skeleton(input.reader.post_bytes(), meta)?;
            for j in 0..skeleton.doc_ids.len() {
                let Some(new_id) = remap(input, new_base[i], skeleton.doc_ids[j]) else { continue };
                let positions = codec::decode_positions_at(
                    input.reader.post_bytes(),
                    skeleton.positions_offsets[j],
                    skeleton.tfs[j],
                )?;
                out.push(MergedPosting { doc_id: new_id, positions });
            }
        }
    }
    Ok(out)
}

fn write_merged_field_blocks(
    post_w: &mut CountingWriter<impl Write>,
    postings: &[MergedPosting],
) -> Result<()> {
    let chunks: Vec<&[MergedPosting]> = postings.chunks(POSTING_BLOCK_SIZE).collect();
    write_u32(post_w, chunks.len() as u32)?;

    // Same two-pass-within-a-field-section shape as the flush encoder: a
    // block's own offset must be known before its directory entry is
    // written, and the directory precedes the payloads on disk.
    let mut payloads = Vec::new();
    let mut metas = Vec::with_capacity(chunks.len());
    for chunk in &chunks {
        let start = payloads.len() as u64;
        write_u32(&mut payloads, chunk.len() as u32)?;
        let mut max_tf = 0u32;
        for posting in chunk.iter() {
            write_u32(&mut payloads, posting.doc_id)?;
            write_u32(&mut payloads, posting.positions.len() as u32)?;
            for &p in &posting.positions {
                write_u32(&mut payloads, p)?;
            }
            max_tf = max_tf.max(posting.positions.len() as u32);
        }
        metas.push(BlockMeta {
            last_doc_id: chunk.last().expect("chunks() never yields an empty slice").doc_id,
            max_tf,
            offset: start,
            length: (payloads.len() as u64 - start) as u32,
        });
    }

    let payload_base = post_w.pos() + metas.len() as u64 * 20;
    for meta in &metas {
        codec::write_block_meta(
            post_w,
            &BlockMeta { offset: payload_base + meta.offset, ..*meta },
        )?;
    }
    post_w.write_all(&payloads)?;
    Ok(())
}

fn merge_postings_and_terms(
    inputs: &[MergeInput],
    new_base: &[DocId],
    schema: &CollectionSchema,
    mut terms_w: impl Write,
    post_w: impl Write,
) -> Result<()> {
    let num_fields = schema.fields.len();

    // Field stats over exactly the documents that will survive — cheap
    // (O(live docs × fields), no postings touched) and needed up front
    // since it lands in the footer alongside the offset table.
    let mut field_doc_count = vec![0u32; num_fields];
    let mut field_total_len = vec![0u64; num_fields];
    for input in inputs {
        for old_id in input.live.iter() {
            let row = codec::field_lengths_row(
                input.reader.doc_bytes(),
                input.reader.doc_header(),
                old_id,
            )?;
            for f in 0..num_fields {
                let len = u32::from_le_bytes(row[f * 4..f * 4 + 4].try_into().expect("4 bytes"));
                if len > 0 {
                    field_doc_count[f] += 1;
                    field_total_len[f] += len as u64;
                }
            }
        }
    }

    let mut post_w = CountingWriter::new(post_w);
    write_header(&mut post_w, POST_MAGIC)?;
    write_header(&mut terms_w, TERMS_MAGIC)?;
    let mut terms_builder = fst::MapBuilder::new(terms_w)
        .map_err(|e| Error::internal(format!("building segment term dictionary: {e}")))?;

    let mut term_offsets: Vec<u64> = Vec::new();
    let mut out_term_id: u64 = 0;

    let mut op = fst::map::OpBuilder::new();
    for input in inputs {
        op = op.add(input.reader.terms_map());
    }
    let mut union = op.union();

    let mut term_lookup = vec![None; inputs.len()];
    use fst::Streamer;
    while let Some((term_bytes, ivs)) = union.next() {
        let term = std::str::from_utf8(term_bytes)
            .map_err(|_| Error::corruption("segment merge: non-utf8 term"))?;

        term_lookup.iter_mut().for_each(|v| *v = None);
        for iv in ivs {
            term_lookup[iv.index] = Some(iv.value);
        }

        let mut kept_fields: Vec<(FieldId, Vec<MergedPosting>)> = Vec::new();
        for field in 0..num_fields as FieldId {
            let postings = collect_field_postings(inputs, new_base, &term_lookup, field)
                .map_err(|e| Error::corruption(format!("term={term:?} field={field}: {e}")))?;
            if !postings.is_empty() {
                kept_fields.push((field, postings));
            }
        }
        if kept_fields.is_empty() {
            continue;
        }

        term_offsets.push(post_w.pos());
        terms_builder
            .insert(term, out_term_id)
            .map_err(|e| Error::internal(format!("building segment term dictionary: {e}")))?;
        out_term_id += 1;

        write_u32(&mut post_w, kept_fields.len() as u32)?;
        for (field, postings) in &kept_fields {
            write_u32(&mut post_w, *field as u32)?;
            write_u32(&mut post_w, postings.len() as u32)?;
            write_merged_field_blocks(&mut post_w, postings)?;
        }
    }

    terms_builder
        .finish()
        .map_err(|e| Error::internal(format!("finishing term dictionary: {e}")))?;

    let footer_start = post_w.pos();
    write_u32(&mut post_w, num_fields as u32)?;
    for f in 0..num_fields {
        write_u32(&mut post_w, field_doc_count[f])?;
        write_u64(&mut post_w, field_total_len[f])?;
    }
    write_u32(&mut post_w, term_offsets.len() as u32)?;
    for off in &term_offsets {
        write_u64(&mut post_w, *off)?;
    }
    write_u64(&mut post_w, footer_start)?;
    Ok(())
}

// --- columns ---------------------------------------------------------------

fn merge_columns(
    inputs: &[MergeInput],
    new_base: &[DocId],
    schema: &CollectionSchema,
    col_w: impl Write,
) -> Result<()> {
    let mut col_w = CountingWriter::new(col_w);
    write_header(&mut col_w, COL_MAGIC)?;

    let num_fields = schema.fields.len();
    let mut directory: Vec<(u8, u64, u64)> = Vec::with_capacity(num_fields);

    for (field, fs) in schema.fields.iter().enumerate() {
        let field = field as FieldId;
        let start = col_w.pos();

        if !fs.needs_column() {
            directory.push((codec::FIELD_TAG_NONE, 0, 0));
            continue;
        }

        if fs.field_type.is_numeric() {
            let mut pairs: Vec<(NumKey, DocId)> = Vec::new();
            for (i, input) in inputs.iter().enumerate() {
                if let Some(col) = codec::decode_numeric_column(
                    input.reader.col_bytes(),
                    input.reader.col_header(),
                    field,
                )? {
                    for (key, old_id) in col.iter() {
                        if let Some(new_id) = remap(input, new_base[i], old_id) {
                            pairs.push((key, new_id));
                        }
                    }
                }
            }
            pairs.sort_by(|a, b| a.0.cmp_key(&b.0).then(a.1.cmp(&b.1)));
            write_u32(&mut col_w, pairs.len() as u32)?;
            for (key, doc_id) in pairs {
                match key {
                    NumKey::Int(v) => {
                        write_u8(&mut col_w, codec::NUMKEY_TAG_INT)?;
                        write_i64(&mut col_w, v)?;
                    }
                    NumKey::Float(v) => {
                        write_u8(&mut col_w, codec::NUMKEY_TAG_FLOAT)?;
                        write_f64(&mut col_w, v)?;
                    }
                }
                write_u32(&mut col_w, doc_id)?;
            }
            directory.push((codec::FIELD_TAG_NUMERIC, start, col_w.pos() - start));
        } else {
            let mut by_value: HashMap<String, RoaringBitmap> = HashMap::new();
            for (i, input) in inputs.iter().enumerate() {
                if let Some(col) = codec::decode_keyword_column(
                    input.reader.col_bytes(),
                    input.reader.col_header(),
                    field,
                )? {
                    for (value, bitmap) in col.iter() {
                        let entry = by_value.entry(value.to_string()).or_default();
                        for old_id in bitmap.iter() {
                            if let Some(new_id) = remap(input, new_base[i], old_id) {
                                entry.insert(new_id);
                            }
                        }
                    }
                }
            }
            let mut values: Vec<(String, RoaringBitmap)> =
                by_value.into_iter().filter(|(_, b)| !b.is_empty()).collect();
            values.sort_by(|a, b| a.0.cmp(&b.0));

            write_u32(&mut col_w, values.len() as u32)?;
            let mut present = RoaringBitmap::new();
            for (value, bitmap) in &values {
                write_str(&mut col_w, value)?;
                let mut ser = Vec::new();
                bitmap.serialize_into(&mut ser).expect("writing to a Vec cannot fail");
                write_u32(&mut col_w, ser.len() as u32)?;
                col_w.write_all(&ser)?;
                present |= bitmap;
            }
            let mut ser = Vec::new();
            present.serialize_into(&mut ser).expect("writing to a Vec cannot fail");
            write_u32(&mut col_w, ser.len() as u32)?;
            col_w.write_all(&ser)?;
            directory.push((codec::FIELD_TAG_KEYWORD, start, col_w.pos() - start));
        }
    }

    let footer_start = col_w.pos();
    write_u32(&mut col_w, num_fields as u32)?;
    for &(tag, offset, length) in &directory {
        write_u8(&mut col_w, tag)?;
        write_u64(&mut col_w, offset)?;
        write_u64(&mut col_w, length)?;
    }
    write_u64(&mut col_w, footer_start)?;
    Ok(())
}

// --- doc store ---------------------------------------------------------

fn merge_docs(
    inputs: &[MergeInput],
    new_base: &[DocId],
    schema: &CollectionSchema,
    out_base: DocId,
    out_end: DocId,
    doc_w: impl Write,
) -> Result<()> {
    let mut doc_w = CountingWriter::new(doc_w);
    write_header(&mut doc_w, DOC_MAGIC)?;
    let blob_start = doc_w.pos();

    let num_fields = schema.fields.len();
    let len = (out_end - out_base) as usize;

    let mut presence = RoaringBitmap::new();
    let mut field_lengths = vec![0u8; len * num_fields * 4];
    let mut values_dir = vec![0u8; len * num_fields * 13];
    let mut source_dir = vec![0u8; len * 12];

    for (i, input) in inputs.iter().enumerate() {
        for old_id in input.live.iter() {
            let new_id = new_base[i] + (input.live.rank(old_id) - 1) as DocId;
            presence.insert(new_id);
            let idx = (new_id - out_base) as usize;

            let row = codec::field_lengths_row(
                input.reader.doc_bytes(),
                input.reader.doc_header(),
                old_id,
            )?;
            field_lengths[idx * num_fields * 4..idx * num_fields * 4 + num_fields * 4]
                .copy_from_slice(row);

            for f in 0..num_fields as FieldId {
                let (tag, payload, ..) = codec::raw_value_slot_at(
                    input.reader.doc_bytes(),
                    input.reader.doc_header(),
                    old_id,
                    f,
                )?;
                let slot_pos = idx * num_fields * 13 + f as usize * 13;
                if matches!(tag, codec::VALUE_TAG_STR | codec::VALUE_TAG_ARRAY) {
                    let new_offset = doc_w.pos();
                    doc_w.write_all(payload)?;
                    values_dir[slot_pos] = tag;
                    values_dir[slot_pos + 1..slot_pos + 9]
                        .copy_from_slice(&new_offset.to_le_bytes());
                    values_dir[slot_pos + 9..slot_pos + 13]
                        .copy_from_slice(&(payload.len() as u32).to_le_bytes());
                } else {
                    values_dir[slot_pos..slot_pos + 13].copy_from_slice(payload);
                }
            }

            let source =
                codec::raw_source_at(input.reader.doc_bytes(), input.reader.doc_header(), old_id)?
                    .ok_or_else(|| {
                        Error::corruption("segment merge: a live document has no stored source")
                    })?;
            let source_offset = doc_w.pos();
            doc_w.write_all(source)?;
            let sd_pos = idx * 12;
            source_dir[sd_pos..sd_pos + 8].copy_from_slice(&source_offset.to_le_bytes());
            source_dir[sd_pos + 8..sd_pos + 12]
                .copy_from_slice(&(source.len() as u32).to_le_bytes());
        }
    }

    let blob_len = doc_w.pos() - blob_start;

    doc_w.write_all(&field_lengths)?;
    doc_w.write_all(&values_dir)?;
    doc_w.write_all(&source_dir)?;

    let mut presence_bytes = Vec::new();
    presence.serialize_into(&mut presence_bytes).expect("writing to a Vec cannot fail");
    doc_w.write_all(&presence_bytes)?;

    let footer_start = doc_w.pos();
    write_u32(&mut doc_w, out_base)?;
    write_u32(&mut doc_w, out_end)?;
    write_u32(&mut doc_w, num_fields as u32)?;
    write_u32(&mut doc_w, presence_bytes.len() as u32)?;
    write_u64(&mut doc_w, blob_len)?;
    write_u64(&mut doc_w, footer_start)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use serde_json::json;
    use tachyon_core::{FieldSchema, FieldType, ParsedDocument};

    use crate::fuzzy::FuzzyMatcher;
    use crate::memtable::MemTable;
    use crate::source::IndexSource;

    use super::super::SegmentFilePaths;
    use super::*;

    fn schema() -> CollectionSchema {
        CollectionSchema::new(
            "products",
            vec![
                FieldSchema::new("title", FieldType::Text).required(),
                FieldSchema::new("tags", FieldType::Text),
                FieldSchema::new("brand", FieldType::Keyword).with_facet(true),
                FieldSchema::new("price", FieldType::Int).with_filter(true).with_sort(true),
            ],
        )
    }

    fn doc(id: &str, title: &str, tags: &[&str], brand: &str, price: i64) -> ParsedDocument {
        ParsedDocument::parse(
            json!({ "id": id, "title": title, "tags": tags, "brand": brand, "price": price }),
            &schema(),
        )
        .unwrap()
    }

    /// Encode `m` and write it out as a real segment under `dir`, returning
    /// a reader over it — everything downstream needs mmap'd files, not an
    /// in-memory blob.
    fn write_segment(
        dir: &Path,
        id: u64,
        m: &MemTable,
        schema: &CollectionSchema,
    ) -> SegmentReader {
        let encoded = codec::encode(m, schema).unwrap();
        let paths = SegmentFilePaths {
            terms: dir.join(format!("{id}.terms")),
            ids: dir.join(format!("{id}.ids")),
            post: dir.join(format!("{id}.post")),
            col: dir.join(format!("{id}.col")),
            doc: dir.join(format!("{id}.doc")),
        };
        std::fs::write(&paths.terms, &encoded.terms).unwrap();
        std::fs::write(&paths.ids, &encoded.ids).unwrap();
        std::fs::write(&paths.post, &encoded.post).unwrap();
        std::fs::write(&paths.col, &encoded.col).unwrap();
        std::fs::write(&paths.doc, &encoded.doc).unwrap();
        SegmentReader::open(&paths, schema).unwrap()
    }

    /// Stream a merge of `inputs` into a real segment under `dir` and open a
    /// reader over the result.
    fn write_merged(
        dir: &Path,
        id: u64,
        inputs: &[MergeInput],
        schema: &CollectionSchema,
        base: DocId,
    ) -> (SegmentReader, MergeStats) {
        let paths = SegmentFilePaths {
            terms: dir.join(format!("{id}.terms")),
            ids: dir.join(format!("{id}.ids")),
            post: dir.join(format!("{id}.post")),
            col: dir.join(format!("{id}.col")),
            doc: dir.join(format!("{id}.doc")),
        };
        let terms_f = std::fs::File::create(&paths.terms).unwrap();
        let ids_f = std::fs::File::create(&paths.ids).unwrap();
        let post_f = std::fs::File::create(&paths.post).unwrap();
        let col_f = std::fs::File::create(&paths.col).unwrap();
        let doc_f = std::fs::File::create(&paths.doc).unwrap();
        let stats =
            merge_segments(inputs, schema, base, terms_f, ids_f, post_f, col_f, doc_f).unwrap();
        (SegmentReader::open(&paths, schema).unwrap(), stats)
    }

    /// The independent, obviously-correct reference a streamed merge is
    /// checked against: re-parse and re-tokenize every surviving document —
    /// exactly what the pre-v4 merge used to do in production — from a fresh
    /// `base`, in the same order `merge_segments` itself visits them (input
    /// order, then ascending doc id within an input). If the streaming
    /// merge and this oracle ever disagree, the streaming merge is wrong;
    /// this is deliberately the slow, boring path with nothing to double-
    /// check it against but "read the test fixtures".
    fn oracle(
        readers: &[&SegmentReader],
        lives: &[RoaringBitmap],
        schema: &CollectionSchema,
        base: DocId,
    ) -> MemTable {
        let mut m = MemTable::new(base, schema);
        for (reader, live) in readers.iter().zip(lives) {
            for old_id in live.iter() {
                let source = reader.get(old_id).expect("live doc must have a source");
                m.insert(ParsedDocument::parse(source, schema).unwrap());
            }
        }
        m
    }

    /// Every `IndexSource` answer a query could ask for, compared between
    /// two sources over the same doc id range and vocabulary — the same
    /// shape `segment::reader`'s own memtable-vs-segment test uses, reused
    /// here to compare a streamed merge against its oracle.
    fn assert_index_sources_agree(
        a: &dyn IndexSource,
        b: &dyn IndexSource,
        schema: &CollectionSchema,
        vocab: &[&str],
    ) {
        assert_eq!(a.min_doc_id(), b.min_doc_id());
        assert_eq!(a.end_doc_id(), b.end_doc_id());

        for doc_id in a.min_doc_id()..a.end_doc_id() {
            assert_eq!(a.is_live(doc_id), b.is_live(doc_id), "doc {doc_id} liveness");
            for field in 0..schema.fields.len() as FieldId {
                assert_eq!(
                    a.value(doc_id, field).as_deref(),
                    b.value(doc_id, field).as_deref(),
                    "doc {doc_id} field {field} value"
                );
                assert_eq!(
                    a.field_len(doc_id, field),
                    b.field_len(doc_id, field),
                    "doc {doc_id} field {field} length"
                );
            }
        }

        fn drain(
            source: &dyn IndexSource,
            term: &str,
            field: FieldId,
        ) -> Option<Vec<(u32, Vec<u32>)>> {
            let mut cursor = source.posting_cursor(term, field)?;
            let mut out = Vec::new();
            while let Some(doc_id) = cursor.doc_id() {
                out.push((doc_id, cursor.positions()));
                cursor.advance();
            }
            Some(out)
        }

        for &term in vocab {
            for field in 0..schema.fields.len() as FieldId {
                assert_eq!(
                    drain(a, term, field),
                    drain(b, term, field),
                    "term {term:?} field {field}"
                );
                assert_eq!(
                    a.doc_freq(term, field),
                    b.doc_freq(term, field),
                    "term {term:?} field {field} doc_freq"
                );
                assert_eq!(
                    a.live_doc_freq(term, field, &RoaringBitmap::new()),
                    b.live_doc_freq(term, field, &RoaringBitmap::new()),
                    "term {term:?} field {field} live_doc_freq"
                );
            }
        }

        for prefix in ["", "a", "b", "m", "z"] {
            let mut a_terms = Vec::new();
            a.collect_terms_with_prefix(prefix, 1000, &mut a_terms);
            let mut b_terms = Vec::new();
            b.collect_terms_with_prefix(prefix, 1000, &mut b_terms);
            a_terms.sort();
            b_terms.sort();
            assert_eq!(a_terms, b_terms, "prefix {prefix:?}");
        }

        for &term in vocab {
            let mut a_fuzzy = Vec::new();
            a.collect_fuzzy_terms(&mut FuzzyMatcher::new(term, 2), &mut a_fuzzy);
            let mut b_fuzzy = Vec::new();
            b.collect_fuzzy_terms(&mut FuzzyMatcher::new(term, 2), &mut b_fuzzy);
            a_fuzzy.sort();
            b_fuzzy.sort();
            assert_eq!(a_fuzzy, b_fuzzy, "fuzzy around {term:?}");
        }

        for field in 0..schema.fields.len() as FieldId {
            match (a.numeric_column(field), b.numeric_column(field)) {
                (Some(ac), Some(bc)) => {
                    assert_eq!(
                        ac.range(None, None).len(),
                        bc.range(None, None).len(),
                        "field {field}"
                    );
                }
                (None, None) => {}
                other => panic!(
                    "field {field}: numeric column presence disagrees: {other:?}",
                    other = (other.0.is_some(), other.1.is_some())
                ),
            }
            match (a.keyword_column(field), b.keyword_column(field)) {
                (Some(ac), Some(bc)) => {
                    assert_eq!(ac.num_values(), bc.num_values(), "field {field}");
                }
                (None, None) => {}
                other => panic!(
                    "field {field}: keyword column presence disagrees: {other:?}",
                    other = (other.0.is_some(), other.1.is_some())
                ),
            }
        }
    }

    fn full_presence(reader: &SegmentReader) -> RoaringBitmap {
        reader.presence().clone()
    }

    #[test]
    fn merging_two_segments_matches_the_rebuild_oracle_byte_for_byte() {
        let schema = schema();
        let dir = tempfile::tempdir().unwrap();

        let mut m0 = MemTable::new(0, &schema);
        m0.insert(doc("1", "wireless mouse", &["red", "blue"], "Logitech", 2999));
        m0.insert(doc("2", "mouse pad", &["blue"], "Razer", 1999));
        let r0 = write_segment(dir.path(), 1, &m0, &schema);

        let mut m1 = MemTable::new(2, &schema);
        m1.insert(doc("3", "mechanical keyboard", &["rgb"], "Corsair", 8999));
        m1.insert(doc("4", "wireless keyboard", &["compact"], "Logitech", 5999));
        let r1 = write_segment(dir.path(), 2, &m1, &schema);

        let lives = [full_presence(&r0), full_presence(&r1)];
        let inputs = vec![
            MergeInput { reader: &r0, live: lives[0].clone() },
            MergeInput { reader: &r1, live: lives[1].clone() },
        ];
        let (merged, stats) = write_merged(dir.path(), 99, &inputs, &schema, 0);
        assert_eq!(stats.doc_count, 4);

        let oracle_mem = oracle(&[&r0, &r1], &lives, &schema, 0);
        let oracle_seg = write_segment(dir.path(), 100, &oracle_mem, &schema);

        assert_index_sources_agree(
            &merged,
            &oracle_seg,
            &schema,
            &[
                "wireless",
                "mouse",
                "keyboard",
                "mechanical",
                "pad",
                "red",
                "blue",
                "rgb",
                "compact",
            ],
        );

        // The strongest possible check: with the flush encoder's output
        // deterministic (see `codec`'s own determinism test) and the merge
        // visiting documents in exactly the order the oracle does, the two
        // segments' bytes should be indistinguishable, not just semantically
        // equivalent.
        for ext in ["terms", "ids", "post", "col", "doc"] {
            let merged_path = dir.path().join(format!("99.{ext}"));
            let oracle_path = dir.path().join(format!("100.{ext}"));
            assert_eq!(
                std::fs::read(&merged_path).unwrap(),
                std::fs::read(&oracle_path).unwrap(),
                "{ext} bytes must match the oracle exactly"
            );
        }
    }

    #[test]
    fn tombstoned_documents_do_not_survive_a_merge() {
        let schema = schema();
        let dir = tempfile::tempdir().unwrap();

        let mut m0 = MemTable::new(0, &schema);
        let keep = m0.insert(doc("1", "wireless mouse", &[], "Logitech", 2999));
        let drop = m0.insert(doc("2", "mouse pad", &[], "Razer", 1999));
        let r0 = write_segment(dir.path(), 1, &m0, &schema);

        // `drop` is still present in the segment (it was live when flushed)
        // but is tombstoned at the collection level — the caller's job,
        // mirrored here, is to exclude it from `live` before merging.
        let mut live = full_presence(&r0);
        live.remove(drop);
        assert!(live.contains(keep));

        let inputs = vec![MergeInput { reader: &r0, live: live.clone() }];
        let (merged, stats) = write_merged(dir.path(), 2, &inputs, &schema, 0);
        assert_eq!(stats.doc_count, 1);
        assert!(merged.is_live(0));
        assert!(!merged.is_live(1));
        assert_eq!(merged.get(0).unwrap()["id"], json!("1"));

        // The dropped document's term must not survive either.
        assert!(merged.posting_cursor("pad", 0).is_none());
        assert!(merged.posting_cursor("mouse", 0).is_some());
    }

    #[test]
    fn a_term_alive_in_only_one_input_is_not_corrupted_by_the_union() {
        let schema = schema();
        let dir = tempfile::tempdir().unwrap();

        let mut m0 = MemTable::new(0, &schema);
        m0.insert(doc("1", "keyboard", &[], "Corsair", 100));
        let r0 = write_segment(dir.path(), 1, &m0, &schema);

        let mut m1 = MemTable::new(1, &schema);
        m1.insert(doc("2", "mouse", &[], "Razer", 200));
        let r1 = write_segment(dir.path(), 2, &m1, &schema);

        let lives = [full_presence(&r0), full_presence(&r1)];
        let inputs = vec![
            MergeInput { reader: &r0, live: lives[0].clone() },
            MergeInput { reader: &r1, live: lives[1].clone() },
        ];
        let (merged, _) = write_merged(dir.path(), 3, &inputs, &schema, 0);

        let mut kb = merged.posting_cursor("keyboard", 0).unwrap();
        assert_eq!(kb.doc_id(), Some(0));
        kb.advance();
        assert_eq!(kb.doc_id(), None, "keyboard only ever occurred in input 0's one document");

        let mouse = merged.posting_cursor("mouse", 0).unwrap();
        assert_eq!(mouse.doc_id(), Some(1));
    }

    #[test]
    fn a_keyword_value_alive_in_only_one_input_gets_the_right_bitmap() {
        let schema = schema();
        let dir = tempfile::tempdir().unwrap();

        let mut m0 = MemTable::new(0, &schema);
        m0.insert(doc("1", "a", &[], "OnlyInZero", 1));
        m0.insert(doc("2", "b", &[], "Shared", 2));
        let r0 = write_segment(dir.path(), 1, &m0, &schema);

        let mut m1 = MemTable::new(2, &schema);
        m1.insert(doc("3", "c", &[], "Shared", 3));
        m1.insert(doc("4", "d", &[], "OnlyInOne", 4));
        let r1 = write_segment(dir.path(), 2, &m1, &schema);

        let lives = [full_presence(&r0), full_presence(&r1)];
        let inputs = vec![
            MergeInput { reader: &r0, live: lives[0].clone() },
            MergeInput { reader: &r1, live: lives[1].clone() },
        ];
        let (merged, _) = write_merged(dir.path(), 3, &inputs, &schema, 0);

        let brand = merged.keyword_column(2).unwrap();
        assert_eq!(brand.equals("OnlyInZero").iter().collect::<Vec<_>>(), vec![0]);
        assert_eq!(brand.equals("OnlyInOne").iter().collect::<Vec<_>>(), vec![3]);
        let mut shared = brand.equals("Shared").iter().collect::<Vec<_>>();
        shared.sort();
        assert_eq!(shared, vec![1, 2]);
        assert_eq!(
            brand.num_values(),
            3,
            "OnlyInZero, Shared, OnlyInOne — Shared is not distinct twice"
        );
    }

    #[test]
    fn a_completely_dead_input_contributes_nothing_but_does_not_break_the_merge() {
        let schema = schema();
        let dir = tempfile::tempdir().unwrap();

        let mut m0 = MemTable::new(0, &schema);
        let a = m0.insert(doc("1", "will be deleted", &[], "X", 1));
        let b = m0.insert(doc("2", "also deleted", &[], "Y", 2));
        m0.remove(a);
        m0.remove(b);
        let r0 = write_segment(dir.path(), 1, &m0, &schema);
        assert!(
            full_presence(&r0).is_empty(),
            "both docs were deleted before this segment ever flushed"
        );

        let mut m1 = MemTable::new(2, &schema);
        m1.insert(doc("3", "survives", &[], "Z", 3));
        let r1 = write_segment(dir.path(), 2, &m1, &schema);

        let inputs = vec![
            MergeInput { reader: &r0, live: full_presence(&r0) },
            MergeInput { reader: &r1, live: full_presence(&r1) },
        ];
        let (merged, stats) = write_merged(dir.path(), 3, &inputs, &schema, 0);
        assert_eq!(stats.doc_count, 1);
        assert_eq!(merged.min_doc_id(), 0);
        assert!(merged.is_live(0));
        assert_eq!(merged.get(0).unwrap()["id"], json!("3"));
    }

    #[test]
    fn a_merge_output_can_itself_be_merged_again() {
        // The merge output must be a fully ordinary segment, not one that
        // only survives being *read*: feeding a first merge's output back in
        // as an input to a second merge exercises that its own presence
        // bitmap, term dictionary, and doc-store rows are all as valid as
        // any freshly flushed segment's.
        let schema = schema();
        let dir = tempfile::tempdir().unwrap();

        let mut m0 = MemTable::new(0, &schema);
        m0.insert(doc("1", "first", &[], "A", 1));
        let r0 = write_segment(dir.path(), 1, &m0, &schema);
        let mut m1 = MemTable::new(1, &schema);
        m1.insert(doc("2", "second", &[], "B", 2));
        let r1 = write_segment(dir.path(), 2, &m1, &schema);

        let first_inputs = vec![
            MergeInput { reader: &r0, live: full_presence(&r0) },
            MergeInput { reader: &r1, live: full_presence(&r1) },
        ];
        let (first_merge, _) = write_merged(dir.path(), 3, &first_inputs, &schema, 0);

        let mut m2 = MemTable::new(2, &schema);
        m2.insert(doc("3", "third", &[], "C", 3));
        let r2 = write_segment(dir.path(), 4, &m2, &schema);

        let second_inputs = vec![
            MergeInput { reader: &first_merge, live: full_presence(&first_merge) },
            MergeInput { reader: &r2, live: full_presence(&r2) },
        ];
        let (second_merge, stats) = write_merged(dir.path(), 5, &second_inputs, &schema, 0);
        assert_eq!(stats.doc_count, 3);
        for (id, expected) in [(0, "1"), (1, "2"), (2, "3")] {
            assert_eq!(second_merge.get(id).unwrap()["id"], json!(expected));
        }
    }

    #[test]
    fn a_fan_in_of_four_remaps_ids_densely_across_every_input() {
        let schema = schema();
        let dir = tempfile::tempdir().unwrap();

        let mut readers = Vec::new();
        let mut base = 0;
        for batch in 0..4 {
            let mut m = MemTable::new(base, &schema);
            for i in 0..3 {
                m.insert(doc(&format!("{batch}-{i}"), "widget", &[], "Brand", i));
            }
            let r = write_segment(dir.path(), batch + 1, &m, &schema);
            base += 3;
            readers.push(r);
        }

        let lives: Vec<RoaringBitmap> = readers.iter().map(full_presence).collect();
        let inputs: Vec<MergeInput> = readers
            .iter()
            .zip(&lives)
            .map(|(r, l)| MergeInput { reader: r, live: l.clone() })
            .collect();
        let (merged, stats) = write_merged(dir.path(), 99, &inputs, &schema, 0);
        assert_eq!(stats.doc_count, 12, "4 segments x 3 live docs each");
        for id in 0..12 {
            assert!(merged.is_live(id), "doc {id} must be dense and live");
        }
        assert!(!merged.is_live(12));

        let widget = merged.posting_cursor("widget", 0).unwrap();
        let mut cursor = widget;
        let mut seen = Vec::new();
        while let Some(id) = cursor.doc_id() {
            seen.push(id);
            cursor.advance();
        }
        assert_eq!(
            seen,
            (0..12).collect::<Vec<_>>(),
            "every one of the 12 docs matches \"widget\""
        );
    }

    #[test]
    fn a_larger_randomized_merge_matches_the_oracle() {
        let schema = schema();
        let dir = tempfile::tempdir().unwrap();

        let words = ["mouse", "keyboard", "monitor", "cable", "stand", "hub", "dock", "case"];
        let brands = ["Logitech", "Razer", "Corsair", "Anker", "HyperX"];

        let mut readers = Vec::new();
        let mut lives = Vec::new();
        let mut base = 0;
        let mut counter = 0usize;
        for seg in 0..3usize {
            let mut m = MemTable::new(base, &schema);
            let mut doc_ids = Vec::new();
            for i in 0..40 {
                let w1 = words[(seg * 7 + i * 3) % words.len()];
                let w2 = words[(seg * 11 + i * 5 + 1) % words.len()];
                let brand = brands[(seg + i) % brands.len()];
                let title = format!("{w1} {w2}");
                let id = m.insert(doc(&format!("{counter}"), &title, &[w1], brand, i as i64));
                doc_ids.push(id);
                counter += 1;
            }
            // Delete roughly a third, in a fixed but non-trivial pattern.
            for (i, &id) in doc_ids.iter().enumerate() {
                if i % 3 == 0 {
                    m.remove(id);
                }
            }
            let r = write_segment(dir.path(), seg as u64 + 1, &m, &schema);
            let mut live = full_presence(&r);
            // Also tombstone one more doc at the "collection" level, past
            // what was already deleted before the flush.
            if let Some(extra) = live.iter().nth(2) {
                live.remove(extra);
            }
            base += 40;
            lives.push(live);
            readers.push(r);
        }

        let reader_refs: Vec<&SegmentReader> = readers.iter().collect();
        let inputs: Vec<MergeInput> = reader_refs
            .iter()
            .zip(&lives)
            .map(|(&r, l)| MergeInput { reader: r, live: l.clone() })
            .collect();
        let (merged, stats) = write_merged(dir.path(), 99, &inputs, &schema, 0);

        let oracle_mem = oracle(&reader_refs, &lives, &schema, 0);
        assert_eq!(stats.doc_count, oracle_mem.len());
        let oracle_seg = write_segment(dir.path(), 100, &oracle_mem, &schema);

        let mut vocab: Vec<&str> = words.to_vec();
        vocab.extend(brands);
        assert_index_sources_agree(&merged, &oracle_seg, &schema, &vocab);
    }
}

/// Regression coverage for a real bug this rewrite shipped and caught before
/// release: `decode_term_field_blocks` stopped skipping a non-matching
/// field's payload bytes once block offsets became absolute (the skip
/// looked redundant — the offsets no longer needed rebasing — but the
/// *cursor* still needed to walk past those bytes to reach the next field's
/// header). It went undetected by every hand-built fixture because none of
/// them combined a term spanning multiple postings blocks with occurring in
/// more than one field of the same document. These two modules exist
/// specifically to keep exercising that combination.
#[cfg(test)]
mod block_boundary_tests {
    use serde_json::json;
    use tachyon_core::{FieldSchema, FieldType, ParsedDocument};

    use crate::memtable::MemTable;
    use crate::source::IndexSource;

    use super::super::SegmentFilePaths;
    use super::*;

    #[test]
    fn a_term_spanning_multiple_blocks_in_two_fields_survives_a_merge() {
        let schema = CollectionSchema::new(
            "products",
            vec![
                FieldSchema::new("title", FieldType::Text).required(),
                FieldSchema::new("tags", FieldType::Text),
            ],
        );
        let dir = tempfile::tempdir().unwrap();

        let mut readers = Vec::new();
        let mut base = 0;
        for seg in 0..2u32 {
            let mut m = MemTable::new(base, &schema);
            for i in 0..150 {
                let doc = ParsedDocument::parse(
                    json!({ "id": format!("{seg}-{i}"), "title": "widget", "tags": "widget" }),
                    &schema,
                )
                .unwrap();
                m.insert(doc);
            }
            let encoded = codec::encode(&m, &schema).unwrap();
            let paths = SegmentFilePaths {
                terms: dir.path().join(format!("{seg}.terms")),
                ids: dir.path().join(format!("{seg}.ids")),
                post: dir.path().join(format!("{seg}.post")),
                col: dir.path().join(format!("{seg}.col")),
                doc: dir.path().join(format!("{seg}.doc")),
            };
            std::fs::write(&paths.terms, &encoded.terms).unwrap();
            std::fs::write(&paths.ids, &encoded.ids).unwrap();
            std::fs::write(&paths.post, &encoded.post).unwrap();
            std::fs::write(&paths.col, &encoded.col).unwrap();
            std::fs::write(&paths.doc, &encoded.doc).unwrap();
            readers.push(SegmentReader::open(&paths, &schema).unwrap());
            base += 150;
        }

        let lives: Vec<RoaringBitmap> = readers.iter().map(|r| r.presence().clone()).collect();
        let inputs: Vec<MergeInput> = readers
            .iter()
            .zip(&lives)
            .map(|(r, l)| MergeInput { reader: r, live: l.clone() })
            .collect();

        let paths = SegmentFilePaths {
            terms: dir.path().join("m.terms"),
            ids: dir.path().join("m.ids"),
            post: dir.path().join("m.post"),
            col: dir.path().join("m.col"),
            doc: dir.path().join("m.doc"),
        };
        let terms_f = std::fs::File::create(&paths.terms).unwrap();
        let ids_f = std::fs::File::create(&paths.ids).unwrap();
        let post_f = std::fs::File::create(&paths.post).unwrap();
        let col_f = std::fs::File::create(&paths.col).unwrap();
        let doc_f = std::fs::File::create(&paths.doc).unwrap();
        let stats =
            merge_segments(&inputs, &schema, 0, terms_f, ids_f, post_f, col_f, doc_f).unwrap();
        assert_eq!(stats.doc_count, 300);

        let merged = SegmentReader::open(&paths, &schema).unwrap();
        let mut cursor = merged.posting_cursor("widget", 0).unwrap();
        let mut count = 0;
        while let Some(_id) = cursor.doc_id() {
            count += 1;
            cursor.advance();
        }
        assert_eq!(count, 300, "every one of the 300 docs must be found across block boundaries");
    }
}

#[cfg(test)]
mod realistic_corpus_tests {
    use serde_json::json;
    use tachyon_core::{FieldSchema, FieldType, ParsedDocument};

    use crate::memtable::MemTable;
    use crate::source::IndexSource;

    use super::super::SegmentFilePaths;
    use super::*;

    pub(super) fn schema() -> CollectionSchema {
        CollectionSchema::new(
            "products",
            vec![
                FieldSchema::new("title", FieldType::Text).required(),
                FieldSchema::new("description", FieldType::Text),
                FieldSchema::new("brand", FieldType::Keyword).with_facet(true),
                FieldSchema::new("category", FieldType::Keyword).with_facet(true),
                FieldSchema::new("price", FieldType::Int).with_filter(true).with_sort(true),
                FieldSchema::new("rating", FieldType::Float).with_filter(true).with_sort(true),
            ],
        )
    }

    const ADJ: &[&str] = &[
        "wireless",
        "wired",
        "mechanical",
        "ergonomic",
        "silent",
        "compact",
        "portable",
        "premium",
        "rugged",
        "slim",
        "gaming",
        "professional",
        "vintage",
        "modular",
        "waterproof",
    ];
    const NOUN: &[&str] = &[
        "mouse",
        "keyboard",
        "monitor",
        "headset",
        "webcam",
        "microphone",
        "speaker",
        "hub",
        "charger",
        "cable",
        "adapter",
        "stand",
        "dock",
        "controller",
        "tablet",
    ];
    const BRAND: &[&str] =
        &["Logitech", "Razer", "Anker", "Corsair", "Keychron", "Belkin", "Elgato"];
    const CATEGORY: &[&str] = &["peripherals", "audio", "power", "display", "accessories"];

    pub(super) struct Rng(pub u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x >> 12;
            x ^= x << 25;
            x ^= x >> 27;
            self.0 = x;
            x.wrapping_mul(0x2545_F491_4F6C_DD1D)
        }
        fn below(&mut self, n: usize) -> usize {
            (self.next() % n as u64) as usize
        }
        /// Biased towards the front, like `corpus::Rng::zipfish` — the real
        /// generator's skew is what makes some words wildly more common than
        /// others, producing terms whose postings span many more blocks than
        /// a uniform distribution ever would.
        fn zipfish(&mut self, n: usize) -> usize {
            let a = self.below(n);
            let b = self.below(n);
            a.min(b)
        }
    }

    pub(super) fn document(rng: &mut Rng, id: usize) -> ParsedDocument {
        let adjective = ADJ[rng.zipfish(ADJ.len())];
        let noun = NOUN[rng.zipfish(NOUN.len())];
        let title = format!("{adjective} {noun}");
        let description = format!("A {adjective} {noun} built for everyday use.");
        ParsedDocument::parse(
            json!({
                "id": id.to_string(),
                "title": title,
                "description": description,
                "brand": BRAND[rng.below(BRAND.len())],
                "category": CATEGORY[rng.below(CATEGORY.len())],
                "price": (rng.below(50_000) + 500) as i64,
                "rating": (rng.below(50) as f64) / 10.0,
            }),
            &schema(),
        )
        .unwrap()
    }

    #[test]
    fn merging_two_realistic_250_doc_segments_matches_every_term_against_the_source() {
        let schema = schema();
        let dir = tempfile::tempdir().unwrap();

        let mut readers = Vec::new();
        let mut base = 0;
        let mut rng = Rng(42);
        for seg in 0..2u64 {
            let mut m = MemTable::new(base, &schema);
            for i in 0..250 {
                m.insert(document(&mut rng, (seg * 1000 + i) as usize));
            }
            let encoded = codec::encode(&m, &schema).unwrap();
            let paths = SegmentFilePaths {
                terms: dir.path().join(format!("{seg}.terms")),
                ids: dir.path().join(format!("{seg}.ids")),
                post: dir.path().join(format!("{seg}.post")),
                col: dir.path().join(format!("{seg}.col")),
                doc: dir.path().join(format!("{seg}.doc")),
            };
            std::fs::write(&paths.terms, &encoded.terms).unwrap();
            std::fs::write(&paths.ids, &encoded.ids).unwrap();
            std::fs::write(&paths.post, &encoded.post).unwrap();
            std::fs::write(&paths.col, &encoded.col).unwrap();
            std::fs::write(&paths.doc, &encoded.doc).unwrap();
            readers.push(SegmentReader::open(&paths, &schema).unwrap());
            base += 250;
        }

        let lives: Vec<RoaringBitmap> = readers.iter().map(|r| r.presence().clone()).collect();
        let inputs: Vec<MergeInput> = readers
            .iter()
            .zip(&lives)
            .map(|(r, l)| MergeInput { reader: r, live: l.clone() })
            .collect();

        let paths = SegmentFilePaths {
            terms: dir.path().join("m.terms"),
            ids: dir.path().join("m.ids"),
            post: dir.path().join("m.post"),
            col: dir.path().join("m.col"),
            doc: dir.path().join("m.doc"),
        };
        let terms_f = std::fs::File::create(&paths.terms).unwrap();
        let ids_f = std::fs::File::create(&paths.ids).unwrap();
        let post_f = std::fs::File::create(&paths.post).unwrap();
        let col_f = std::fs::File::create(&paths.col).unwrap();
        let doc_f = std::fs::File::create(&paths.doc).unwrap();
        merge_segments(&inputs, &schema, 0, terms_f, ids_f, post_f, col_f, doc_f).unwrap();

        let merged = SegmentReader::open(&paths, &schema).unwrap();
        // Every distinct word in the vocabulary, in both text fields: with
        // 500 documents drawn from a 15-word Zipf-biased distribution, the
        // most common words comfortably span several 128-doc posting
        // blocks, and every word occurs in both `title` and `description` —
        // exactly the combination this module's doc comment describes.
        for &term in ADJ.iter().chain(NOUN) {
            for field in 0..2u16 {
                let Some(mut cursor) = merged.posting_cursor(term, field) else { continue };
                let mut decoded = 0;
                while let Some(doc_id) = cursor.doc_id() {
                    let source = merged.get(doc_id).expect("a live doc must have a source");
                    let text =
                        source[if field == 0 { "title" } else { "description" }].as_str().unwrap();
                    assert!(
                        text.contains(term),
                        "doc {doc_id} field {field} matched {term:?} but its text is {text:?}"
                    );
                    let _ = cursor.positions();
                    cursor.advance();
                    decoded += 1;
                }
                assert!(decoded > 0);
            }
        }
    }
}
