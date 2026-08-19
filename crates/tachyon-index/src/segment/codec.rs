//! Segment byte codec, v4: stream a live view of a [`MemTable`] — or, via
//! [`super::merge`], several already-encoded segments — into five segment
//! blobs, and decode them back — lazily. Nothing here holds a whole segment's
//! postings, columns, or documents in memory at once; every write function
//! streams forward through an [`impl Write`](std::io::Write) sink, bounded by
//! at most one term's, one field's, or one document's worth of scratch space
//! at a time — never by corpus size. Every decode function takes the byte
//! slice it needs and returns just the one term's postings, one field's
//! column, or one document's value the caller asked for. `tachyon-engine`'s
//! `SegmentReader` wraps these around an mmap'd file per blob and calls them
//! per query.
//!
//! No filesystem access here — opening the destination files, writing them
//! to disk, and committing them into `state.json` is `tachyon-engine`'s job,
//! per `tachyon-storage`'s "segment *contents* are the index crate's
//! business, segment *lifecycle* is the engine's" split.
//!
//! # Footer layout
//!
//! `.post`, `.col`, and `.doc` all follow the same shape: a 12-byte header,
//! then payload data written forward as it's produced, then a footer (the
//! directory a query needs to locate anything — an offset table, a column
//! directory, a doc-store section table) written last, once its contents are
//! finally known, and a trailing 8-byte absolute file offset pointing at
//! where that footer begins. A reader with the whole file mmap'd checks the
//! header, reads the last 8 bytes to find the footer, and reads the footer
//! from there — no different in cost from a front-loaded directory, since
//! mmap makes every byte in the file equally cheap to reach. What it buys is
//! a writer that never needs to know its own directory's size or contents
//! before starting to stream payload bytes. `.terms` and `.ids` need no such
//! footer: both are `fst::Map`s, and `fst::MapBuilder` already streams
//! directly into a [`Write`](std::io::Write) sink in sorted-key order.
//!
//! # Holes
//!
//! A document deleted from the memtable before ever being flushed must never
//! reach a segment. Every encoder here filters against `live`, a bitmap of
//! doc ids the memtable still holds — postings, columns, and doc records for
//! anything else are dropped for good; that's how a flush reclaims the
//! memory [`crate::inverted::InvertedIndex`]'s "postings are never removed"
//! policy leaves behind.

use std::collections::HashMap;
use std::io::Write;

use roaring::RoaringBitmap;
use serde_json::Value as Json;

use tachyon_core::{CollectionSchema, DocId, Error, FieldId, Result, Value};

use crate::columns::{KeywordColumn, NumKey, NumericColumn};
use crate::inverted::DocPosting;
use crate::memtable::MemTable;

use super::format::{
    bytes_at, f64_at, i64_at, u32_at, u64_at, u8_at, write_bytes, write_f64, write_header,
    write_i64, write_str, write_u32, write_u64, write_u8, CountingWriter, Cursor, COL_MAGIC,
    DOC_MAGIC, FOOTER_POINTER_LEN, HEADER_LEN, IDS_MAGIC, POST_MAGIC, TERMS_MAGIC,
};

/// The five byte blobs one segment is made of, ready to be written to disk.
/// Exists mainly so `codec`'s own tests (and a couple of tests elsewhere in
/// this crate that don't need real files) can call [`encode`] and get
/// something they can slice into directly — `tachyon-engine`'s real flush
/// and merge paths stream straight into files via
/// [`encode_streaming`]/[`super::merge::merge_segments`] instead, without
/// ever materializing a segment's bytes in one buffer.
pub struct EncodedSegment {
    pub terms: Vec<u8>,
    pub ids: Vec<u8>,
    pub post: Vec<u8>,
    pub col: Vec<u8>,
    pub doc: Vec<u8>,
}

/// Encode every live document in `memtable` into segment bytes, in memory.
/// A thin convenience wrapper around [`encode_streaming`] for callers (tests,
/// mostly) that want an owned blob rather than to stream into files — see
/// [`EncodedSegment`]'s doc comment.
pub fn encode(memtable: &MemTable, schema: &CollectionSchema) -> Result<EncodedSegment> {
    let mut terms = Vec::new();
    let mut ids = Vec::new();
    let mut post = Vec::new();
    let mut col = Vec::new();
    let mut doc = Vec::new();
    encode_streaming(memtable, schema, &mut terms, &mut ids, &mut post, &mut col, &mut doc)?;
    Ok(EncodedSegment { terms, ids, post, col, doc })
}

/// Stream every live document in `memtable` into the five sinks, in bounded
/// memory: `terms_w`/`ids_w` grow with the FST as `fst::MapBuilder` writes to
/// them, `post_w` never holds more than one term-field's postings at a time,
/// `col_w` never holds more than one field's column at a time, and `doc_w`'s
/// bounded scratch (`field_lengths`/`values_dir`/`source_dir`) is sized by
/// document count and field count, not by document content. Nothing here
/// scales with total corpus text or position count.
pub fn encode_streaming(
    memtable: &MemTable,
    schema: &CollectionSchema,
    terms_w: impl Write,
    ids_w: impl Write,
    post_w: impl Write,
    col_w: impl Write,
    doc_w: impl Write,
) -> Result<()> {
    let live: RoaringBitmap = memtable.iter().map(|(id, _)| id).collect();

    encode_postings_streaming(memtable, &live, schema, terms_w, post_w)?;
    encode_ids_streaming(memtable, ids_w)?;
    encode_columns_streaming(memtable, &live, schema, col_w)?;
    encode_docs_streaming(memtable, schema, doc_w)?;
    Ok(())
}

/// Validate a header and return the byte offset where the payload after it
/// begins — used by callers that mmap the file starting past the header
/// (`.terms`/`.ids`, whose `fst::Map` needs to see nothing but its own bytes).
pub fn validate_header(bytes: &[u8], magic: &[u8; 8], what: &'static str) -> Result<usize> {
    let mut cur = Cursor::new(bytes, what);
    cur.read_header(magic)?;
    Ok(cur.position())
}

/// The absolute file offset the trailing 8-byte footer pointer names, for a
/// footer-based file (`.post`/`.col`/`.doc`). `what` labels corruption errors.
fn read_footer_start(bytes: &[u8], what: &'static str) -> Result<usize> {
    if bytes.len() < HEADER_LEN + FOOTER_POINTER_LEN {
        return Err(Error::corruption(format!("{what}: truncated or malformed")));
    }
    let footer_start = u64_at(bytes, bytes.len() - FOOTER_POINTER_LEN, what)? as usize;
    if footer_start < HEADER_LEN || footer_start > bytes.len() - FOOTER_POINTER_LEN {
        return Err(Error::corruption(format!("{what}: footer pointer out of range")));
    }
    Ok(footer_start)
}

// --- ids ---------------------------------------------------------------

/// `id -> doc_id`, an `fst::Map` exactly like `.terms` — sorted, mmap'd,
/// nothing resident but the offset of the file itself. Replaces holding a
/// `HashMap<Box<str>, DocId>` for the whole segment. The `(id, doc_id)` pairs
/// themselves are held as a `Vec` to be sorted before insertion (`fst`
/// requires ascending-key insertion) — bounded by total id-string bytes,
/// which is small next to postings or document text and not worth streaming
/// around.
fn encode_ids_streaming(memtable: &MemTable, ids_w: impl Write) -> Result<()> {
    let mut pairs: Vec<(&str, DocId)> =
        memtable.iter().map(|(doc_id, doc)| (doc.id.as_str(), doc_id)).collect();
    pairs.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
    write_fst(ids_w, IDS_MAGIC, pairs.into_iter().map(|(id, doc_id)| (id, doc_id as u64)))
}

/// Write a header followed by an `fst::Map` streamed directly from a sorted
/// `(key, value)` iterator — used for both `.ids` (id order, pairs already
/// buffered and sorted by the caller) and `.terms` (dictionary order, driven
/// term-by-term by the postings writer so the map and the postings it points
/// into are built in the same pass).
pub(crate) fn write_fst<'a>(
    mut w: impl Write,
    magic: &[u8; 8],
    pairs: impl Iterator<Item = (&'a str, u64)>,
) -> Result<()> {
    write_header(&mut w, magic)?;
    let mut builder = fst::MapBuilder::new(w)
        .map_err(|e| Error::internal(format!("building segment id/term index: {e}")))?;
    for (key, value) in pairs {
        builder
            .insert(key, value)
            .map_err(|e| Error::internal(format!("building segment id/term index: {e}")))?;
    }
    builder.finish().map_err(|e| Error::internal(format!("building segment id/term index: {e}")))
}

// --- postings + terms ----------------------------------------------------

/// Fixed number of docs per posting block: small enough that a block's
/// `max_tf` tracks the true local maximum closely (a tight bound is what
/// makes block-level pruning worth doing), large enough that the directory
/// overhead per term stays trivial — a 10,000-doc term needs ~79 blocks ×
/// 20 bytes ≈ 1.5 KB.
pub(crate) const POSTING_BLOCK_SIZE: usize = 128;

/// Byte width of one [`BlockMeta`] on disk: `last_doc_id`(4) + `max_tf`(4) +
/// `offset`(8) + `length`(4).
const BLOCK_META_LEN: u64 = 20;

/// Fixed-width per-block skip metadata: lets a query decide whether to skip
/// a block, or jump straight to one, without decoding a single posting
/// inside it. 20 bytes on disk. `offset` is always an absolute file offset,
/// whether freshly computed by the streaming postings writer or rebased by
/// [`decode_term_field_blocks`] when read back.
#[derive(Debug, Clone, Copy)]
pub(crate) struct BlockMeta {
    pub last_doc_id: DocId,
    pub max_tf: u32,
    pub offset: u64,
    pub length: u32,
}

pub(crate) fn write_block_meta(w: &mut impl Write, meta: &BlockMeta) -> Result<()> {
    write_u32(w, meta.last_doc_id)?;
    write_u32(w, meta.max_tf)?;
    write_u64(w, meta.offset)?;
    write_u32(w, meta.length)?;
    Ok(())
}

fn read_block_meta(cur: &mut Cursor) -> Result<BlockMeta> {
    let last_doc_id = cur.read_u32()?;
    let max_tf = cur.read_u32()?;
    let offset = cur.read_u64()?;
    let length = cur.read_u32()?;
    Ok(BlockMeta { last_doc_id, max_tf, offset, length })
}

/// Stream `.post` (postings, one term-field's blocks at a time) and `.terms`
/// (the term dictionary, built alongside it) together, since a term is only
/// worth writing to either once it's known to have at least one surviving
/// posting after live-filtering.
fn encode_postings_streaming(
    memtable: &MemTable,
    live: &RoaringBitmap,
    schema: &CollectionSchema,
    mut terms_w: impl Write,
    post_w: impl Write,
) -> Result<()> {
    let num_fields = schema.fields.len();
    let mut field_doc_count = vec![0u32; num_fields];
    let mut field_total_len = vec![0u64; num_fields];
    for (_, doc) in memtable.iter() {
        for (field, &len) in doc.field_lengths.iter().enumerate() {
            if len > 0 {
                field_doc_count[field] += 1;
                field_total_len[field] += len as u64;
            }
        }
    }

    let mut post_w = CountingWriter::new(post_w);
    write_header(&mut post_w, POST_MAGIC)?;

    write_header(&mut terms_w, TERMS_MAGIC)?;
    let mut terms_builder = fst::MapBuilder::new(terms_w)
        .map_err(|e| Error::internal(format!("building segment term dictionary: {e}")))?;

    // fst requires ascending-key insertion; the dictionary is already sorted.
    let mut term_offsets: Vec<u64> = Vec::new();
    let mut term_id: u64 = 0;

    for (term, fields) in memtable.index().iter() {
        let mut kept: Vec<(FieldId, Vec<&DocPosting>)> = Vec::new();
        for (field, postings) in fields {
            let docs: Vec<&DocPosting> =
                postings.docs.iter().filter(|d| live.contains(d.doc_id)).collect();
            if !docs.is_empty() {
                kept.push((*field, docs));
            }
        }
        if kept.is_empty() {
            continue;
        }

        term_offsets.push(post_w.pos());
        terms_builder
            .insert(term, term_id)
            .map_err(|e| Error::internal(format!("building segment term dictionary: {e}")))?;
        term_id += 1;

        write_u32(&mut post_w, kept.len() as u32)?;
        for (field, docs) in &kept {
            write_u32(&mut post_w, *field as u32)?;
            write_u32(&mut post_w, docs.len() as u32)?; // doc_freq: O(1) to read back

            let chunks: Vec<&[&DocPosting]> = docs.chunks(POSTING_BLOCK_SIZE).collect();
            write_u32(&mut post_w, chunks.len() as u32)?;

            // Two-pass within this one field's blocks, bounded by this one
            // (term, field)'s postings — not the whole segment's: each
            // block's own offset (relative to where this field's payloads
            // begin) must be known before its directory entry is written,
            // and the directory precedes the payloads on disk, so a single
            // forward pass over just this scratch buffer can't know them yet.
            let mut payloads = Vec::new();
            let mut metas = Vec::with_capacity(chunks.len());
            for chunk in &chunks {
                let start = payloads.len() as u64;
                write_u32(&mut payloads, chunk.len() as u32)?;
                let mut max_tf = 0u32;
                for posting in *chunk {
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
                    offset: start, // relative to `payloads`; rebased to absolute below
                    length: (payloads.len() as u64 - start) as u32,
                });
            }

            let payload_base = post_w.pos() + metas.len() as u64 * BLOCK_META_LEN;
            for meta in &metas {
                write_block_meta(
                    &mut post_w,
                    &BlockMeta { offset: payload_base + meta.offset, ..*meta },
                )?;
            }
            post_w.write_all(&payloads)?;
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

/// The small, eager part of `.post`: field stats and the offset table. O(num
/// fields + num terms), never O(num postings).
pub(crate) struct PostHeader {
    pub field_doc_count: Vec<u32>,
    pub field_total_len: Vec<u64>,
    /// Absolute byte offset of term `i`'s block.
    offsets: Vec<u64>,
    /// Absolute offset the footer itself starts at — a term's block always
    /// ends either at the next term's offset, or here for the last one.
    footer_start: u64,
}

pub(crate) fn decode_post_header(bytes: &[u8]) -> Result<PostHeader> {
    let what = "segment postings";
    {
        let mut cur = Cursor::new(bytes, what);
        cur.read_header(POST_MAGIC)?;
    }
    let footer_start = read_footer_start(bytes, what)?;

    let mut cur = Cursor::new(&bytes[footer_start..bytes.len() - FOOTER_POINTER_LEN], what);
    let num_fields = cur.read_u32()? as usize;
    let mut field_doc_count = Vec::with_capacity(num_fields);
    let mut field_total_len = Vec::with_capacity(num_fields);
    for _ in 0..num_fields {
        field_doc_count.push(cur.read_u32()?);
        field_total_len.push(cur.read_u64()?);
    }

    let num_terms = cur.read_u32()? as usize;
    let mut offsets = Vec::with_capacity(num_terms);
    for _ in 0..num_terms {
        offsets.push(cur.read_u64()?);
    }

    Ok(PostHeader { field_doc_count, field_total_len, offsets, footer_start: footer_start as u64 })
}

const POST_BLOCK_WHAT: &str = "segment postings (term block)";

/// Absolute byte offset, into `.post`, of term `term_id`'s block.
fn term_block_start(header: &PostHeader, term_id: u64) -> Result<usize> {
    header
        .offsets
        .get(term_id as usize)
        .map(|&o| o as usize)
        .ok_or_else(|| Error::corruption(format!("{POST_BLOCK_WHAT}: term id out of range")))
}

fn term_block<'a>(bytes: &'a [u8], header: &PostHeader, term_id: u64) -> Result<&'a [u8]> {
    let start = term_block_start(header, term_id)?;
    let idx = term_id as usize;
    let end = if idx + 1 < header.offsets.len() {
        header.offsets[idx + 1] as usize
    } else {
        header.footer_start as usize
    };
    bytes.get(start..end).ok_or_else(|| {
        Error::corruption(format!(
            "{POST_BLOCK_WHAT}: offset table out of range (term_id={term_id} start={start} end={end} bytes.len()={} footer_start={} offsets.len()={})",
            bytes.len(), header.footer_start, header.offsets.len()
        ))
    })
}

/// One (term, field)'s block directory: a doc-frequency count, read once, and
/// every block's skip metadata — enough to decide which blocks are even
/// worth decoding, without touching a single posting.
pub(crate) struct TermFieldBlocks {
    pub doc_freq: u32,
    pub blocks: Vec<BlockMeta>,
}

/// Decode one (term, field)'s block directory: `doc_freq` and every block's
/// `BlockMeta`, whose `offset` is already absolute. `None` if the term never
/// occurs in this field. The production entry point for the postings walk —
/// no posting is decoded here, only the directory that says where they live.
pub(crate) fn decode_term_field_blocks(
    bytes: &[u8],
    header: &PostHeader,
    term_id: u64,
    field: FieldId,
) -> Result<Option<TermFieldBlocks>> {
    let what = POST_BLOCK_WHAT;
    let block = term_block(bytes, header, term_id)?;

    let mut cur = Cursor::new(block, what);
    let num_term_fields = cur.read_u32()? as usize;
    for _ in 0..num_term_fields {
        let this_field = cur.read_u32()? as FieldId;
        let doc_freq = cur.read_u32()?;
        let num_blocks = cur.read_u32()? as usize;

        // Every block's metadata is fixed-width and read up front regardless
        // of whether this is the field being asked about — for a field that
        // isn't, `BlockMeta.length` is exactly what's needed to skip its
        // payload bytes (which, unlike the metadata, this cursor never
        // otherwise touches) without decoding a single doc inside them.
        // Skipping matters even though `BlockMeta.offset` is already an
        // absolute file offset (nothing here needs rebasing, unlike the pre-
        // v4 format): this cursor is walking a *sequential* layout — this
        // field's payloads sit directly between its own directory and the
        // next field's — so failing to skip past them would leave the
        // cursor reading the next field's header starting mid-payload.
        let mut blocks = Vec::with_capacity(num_blocks);
        let mut payload_len: u64 = 0;
        for _ in 0..num_blocks {
            let meta = read_block_meta(&mut cur)?;
            payload_len += meta.length as u64;
            blocks.push(meta);
        }

        if this_field != field {
            cur.skip(payload_len as usize)?;
            continue;
        }
        return Ok(Some(TermFieldBlocks { doc_freq, blocks }));
    }
    Ok(None)
}

/// One block's per-doc skeleton — doc ids and term frequencies, decoded
/// without materializing any positions. `positions_offset[i]` is the absolute
/// byte offset (into the same `bytes` the block came from) of doc `i`'s
/// positions, for a lazy read later via [`decode_positions_at`] — paid only
/// when a document's positions are actually needed (phrase/proximity), never
/// for a block skipped or only bound-checked.
pub(crate) struct BlockSkeleton {
    pub doc_ids: Vec<DocId>,
    pub tfs: Vec<u32>,
    pub positions_offsets: Vec<u64>,
}

pub(crate) fn decode_block_skeleton(bytes: &[u8], meta: &BlockMeta) -> Result<BlockSkeleton> {
    let what = POST_BLOCK_WHAT;
    let slice = bytes_at(bytes, meta.offset as usize, meta.length as usize, what)?;
    let mut cur = Cursor::new(slice, what);
    let num_docs = cur.read_u32()? as usize;
    let mut doc_ids = Vec::with_capacity(num_docs);
    let mut tfs = Vec::with_capacity(num_docs);
    let mut positions_offsets = Vec::with_capacity(num_docs);
    for _ in 0..num_docs {
        doc_ids.push(cur.read_u32()?);
        let num_positions = cur.read_u32()?;
        tfs.push(num_positions);
        positions_offsets.push(meta.offset + cur.position() as u64);
        cur.skip(num_positions as usize * 4)?;
    }
    Ok(BlockSkeleton { doc_ids, tfs, positions_offsets })
}

/// One document's positions, decoded on demand from an offset
/// [`decode_block_skeleton`] already located.
pub(crate) fn decode_positions_at(bytes: &[u8], offset: u64, tf: u32) -> Result<Vec<u32>> {
    let mut positions = Vec::with_capacity(tf as usize);
    decode_positions_into(bytes, offset, tf, &mut positions)?;
    Ok(positions)
}

/// Same decode as [`decode_positions_at`], appending into a caller-owned,
/// reused buffer instead of allocating a fresh `Vec` per call — the shape a
/// hot query-time caller wants, since a broad query decodes positions for
/// many (document, token) pairs per search.
pub(crate) fn decode_positions_into(
    bytes: &[u8],
    offset: u64,
    tf: u32,
    out: &mut Vec<u32>,
) -> Result<()> {
    let what = POST_BLOCK_WHAT;
    let slice = bytes_at(bytes, offset as usize, tf as usize * 4, what)?;
    let mut cur = Cursor::new(slice, what);
    out.reserve(tf as usize);
    for _ in 0..tf {
        out.push(cur.read_u32()?);
    }
    Ok(())
}

/// Just the document ids one term occurs in, for one field — no positions
/// decoded or allocated. `live_doc_freq` needs every doc id (liveness varies
/// per doc) but never the positions; `doc_freq` itself no longer walks
/// anything, since [`TermFieldBlocks::doc_freq`] already has the count.
pub(crate) fn decode_term_field_doc_ids(
    bytes: &[u8],
    header: &PostHeader,
    term_id: u64,
    field: FieldId,
) -> Result<Vec<DocId>> {
    let Some(term_field) = decode_term_field_blocks(bytes, header, term_id, field)? else {
        return Ok(Vec::new());
    };
    let mut doc_ids = Vec::new();
    for meta in &term_field.blocks {
        doc_ids.extend(decode_block_skeleton(bytes, meta)?.doc_ids);
    }
    Ok(doc_ids)
}

// --- columns -----------------------------------------------------------

pub(crate) const FIELD_TAG_NONE: u8 = 0;
pub(crate) const FIELD_TAG_NUMERIC: u8 = 1;
pub(crate) const FIELD_TAG_KEYWORD: u8 = 2;
pub(crate) const NUMKEY_TAG_INT: u8 = 0;
pub(crate) const NUMKEY_TAG_FLOAT: u8 = 1;

/// Stream `.col`: one field's column at a time, in field-id order, with the
/// per-field directory (tag, offset, length) recorded as each field's data
/// is written and emitted as a footer once every field has been visited.
///
/// Keyword values are written in **sorted order** — not the `HashMap`
/// iteration order a naive per-field walk would produce. That determinism
/// isn't just tidiness: two encodes of the same live document set must
/// produce byte-identical `.col` bytes for the streaming merge path's
/// equivalence tests to be meaningful, and `HashMap`'s randomized iteration
/// order means the pre-v4 encoder didn't actually have that property even
/// though nothing depended on it before now.
fn encode_columns_streaming(
    memtable: &MemTable,
    live: &RoaringBitmap,
    schema: &CollectionSchema,
    col_w: impl Write,
) -> Result<()> {
    let mut col_w = CountingWriter::new(col_w);
    write_header(&mut col_w, COL_MAGIC)?;

    let columns = memtable.columns();
    let num_fields = schema.fields.len();
    let mut directory: Vec<(u8, u64, u64)> = Vec::with_capacity(num_fields);

    for field_id in 0..num_fields as FieldId {
        let start = col_w.pos();
        if let Some(col) = columns.numeric(field_id) {
            let mut pairs: Vec<(NumKey, DocId)> =
                col.iter().filter(|(_, d)| live.contains(*d)).collect();
            pairs.sort_by(|a, b| a.0.cmp_key(&b.0).then(a.1.cmp(&b.1)));
            write_u32(&mut col_w, pairs.len() as u32)?;
            for (key, doc_id) in pairs {
                match key {
                    NumKey::Int(i) => {
                        write_u8(&mut col_w, NUMKEY_TAG_INT)?;
                        write_i64(&mut col_w, i)?;
                    }
                    NumKey::Float(f) => {
                        write_u8(&mut col_w, NUMKEY_TAG_FLOAT)?;
                        write_f64(&mut col_w, f)?;
                    }
                }
                write_u32(&mut col_w, doc_id)?;
            }
            directory.push((FIELD_TAG_NUMERIC, start, col_w.pos() - start));
        } else if let Some(col) = columns.keyword(field_id) {
            let mut values: Vec<(&str, RoaringBitmap)> = col
                .iter()
                .filter_map(|(v, b)| {
                    let mut filtered = b.clone();
                    filtered &= live;
                    (!filtered.is_empty()).then_some((v, filtered))
                })
                .collect();
            values.sort_by(|a, b| a.0.cmp(b.0));
            write_u32(&mut col_w, values.len() as u32)?;
            for (value, bitmap) in &values {
                write_str(&mut col_w, value)?;
                let mut ser = Vec::new();
                bitmap.serialize_into(&mut ser).expect("writing to a Vec cannot fail");
                write_bytes(&mut col_w, &ser)?;
            }
            let mut present = col.present().clone();
            present &= live;
            let mut ser = Vec::new();
            present.serialize_into(&mut ser).expect("writing to a Vec cannot fail");
            write_bytes(&mut col_w, &ser)?;
            directory.push((FIELD_TAG_KEYWORD, start, col_w.pos() - start));
        } else {
            directory.push((FIELD_TAG_NONE, 0, 0));
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

struct ColEntry {
    tag: u8,
    offset: u64,
    length: u64,
}

pub(crate) struct ColHeader {
    directory: Vec<ColEntry>,
}

pub(crate) fn decode_col_header(bytes: &[u8]) -> Result<ColHeader> {
    let what = "segment columns";
    {
        let mut cur = Cursor::new(bytes, what);
        cur.read_header(COL_MAGIC)?;
    }
    let footer_start = read_footer_start(bytes, what)?;

    let mut cur = Cursor::new(&bytes[footer_start..bytes.len() - FOOTER_POINTER_LEN], what);
    let num_fields = cur.read_u32()? as usize;
    let mut directory = Vec::with_capacity(num_fields);
    for _ in 0..num_fields {
        let tag = cur.read_u8()?;
        let offset = cur.read_u64()?;
        let length = cur.read_u64()?;
        directory.push(ColEntry { tag, offset, length });
    }
    Ok(ColHeader { directory })
}

pub(crate) fn decode_numeric_column(
    bytes: &[u8],
    header: &ColHeader,
    field: FieldId,
) -> Result<Option<NumericColumn>> {
    let Some(entry) = header.directory.get(field as usize) else { return Ok(None) };
    if entry.tag != FIELD_TAG_NUMERIC {
        return Ok(None);
    }
    let slice = bytes_at(bytes, entry.offset as usize, entry.length as usize, "segment columns")?;
    let mut cur = Cursor::new(slice, "segment columns (numeric field)");
    let n = cur.read_u32()? as usize;
    let mut pairs = Vec::with_capacity(n);
    for _ in 0..n {
        let key = match cur.read_u8()? {
            NUMKEY_TAG_INT => NumKey::Int(cur.read_i64()?),
            NUMKEY_TAG_FLOAT => NumKey::Float(cur.read_f64()?),
            other => {
                return Err(Error::corruption(format!(
                    "segment columns: bad numeric key tag {other}"
                )))
            }
        };
        let doc_id = cur.read_u32()?;
        pairs.push((key, doc_id));
    }
    Ok(Some(NumericColumn::from_sorted(pairs)))
}

pub(crate) fn decode_keyword_column(
    bytes: &[u8],
    header: &ColHeader,
    field: FieldId,
) -> Result<Option<KeywordColumn>> {
    let Some(entry) = header.directory.get(field as usize) else { return Ok(None) };
    if entry.tag != FIELD_TAG_KEYWORD {
        return Ok(None);
    }
    let slice = bytes_at(bytes, entry.offset as usize, entry.length as usize, "segment columns")?;
    let mut cur = Cursor::new(slice, "segment columns (keyword field)");
    let n = cur.read_u32()? as usize;
    let mut by_value = HashMap::with_capacity(n);
    for _ in 0..n {
        let value = cur.read_str()?.to_string();
        let ser = cur.read_bytes()?;
        let bitmap = RoaringBitmap::deserialize_from(ser)?;
        by_value.insert(value.into_boxed_str(), bitmap);
    }
    let ser = cur.read_bytes()?;
    let present = RoaringBitmap::deserialize_from(ser)?;
    Ok(Some(KeywordColumn::from_parts(by_value, present)))
}

// --- doc store -----------------------------------------------------------
//
// One forward pass, one document at a time, in doc id order:
//
//   blob          value overflow (Str/Array) + source JSON, appended as
//                 encountered, immediately after the header             streamed
//   field_lengths dense (end-base)*num_fields*4                         bounded scratch,
//   values_dir    dense (end-base)*num_fields*VALUE_SLOT_LEN            written after
//   source_dir    dense (end-base)*12                                   the blob
//   presence      serialized roaring bitmap
//   footer        base/end/num_fields/presence_len/blob_len/footer_start
//
// The three dense sections are bounded by document count × field count, not
// by document *content* — the same order of magnitude a memtable already
// holds resident, and not what this format change is about. What it is
// about is `blob`: raw text and JSON, the part that actually scales with
// corpus size, which streams straight to the sink as each document is
// visited instead of accumulating in a `Vec` for the whole segment.
//
// `field_lengths` is BM25's `|d|`, read once per scored document — it must
// never cost an allocation, which is why it gets its own flat array instead
// of living inside a per-doc record that would need decoding to reach it.

const VALUE_TAG_NULL: u8 = 0;
const VALUE_TAG_BOOL: u8 = 1;
const VALUE_TAG_INT: u8 = 2;
const VALUE_TAG_FLOAT: u8 = 3;
pub(crate) const VALUE_TAG_STR: u8 = 4;
pub(crate) const VALUE_TAG_ARRAY: u8 = 5;

/// 1 tag byte + 8-byte blob offset + 4-byte blob length. `Null`/`Bool`/
/// `Int`/`Float` only use the first of those 12 payload bytes (or the first
/// 9, for `Int`/`Float`'s inline value) and leave the rest zeroed; `Str`/
/// `Array` use all of it to point into the blob. One width for every tag
/// keeps the array directly indexable by `(doc, field)` — an `Int` costs the
/// same 13 bytes a `Str` does, in exchange for O(1) addressing.
const VALUE_SLOT_LEN: usize = 13;

/// The general recursive encoding used for `Str`/`Array` values — written
/// into the blob and referenced from a slot by `(offset, length)`.
fn write_value_full(w: &mut impl Write, value: &Value) -> Result<()> {
    match value {
        Value::Null => write_u8(w, VALUE_TAG_NULL)?,
        Value::Bool(b) => {
            write_u8(w, VALUE_TAG_BOOL)?;
            write_u8(w, *b as u8)?;
        }
        Value::Int(i) => {
            write_u8(w, VALUE_TAG_INT)?;
            write_i64(w, *i)?;
        }
        Value::Float(f) => {
            write_u8(w, VALUE_TAG_FLOAT)?;
            write_f64(w, *f)?;
        }
        Value::Str(s) => {
            write_u8(w, VALUE_TAG_STR)?;
            write_str(w, s)?;
        }
        Value::Array(items) => {
            write_u8(w, VALUE_TAG_ARRAY)?;
            write_u32(w, items.len() as u32)?;
            for item in items {
                write_value_full(w, item)?;
            }
        }
    }
    Ok(())
}

fn read_value_full(cur: &mut Cursor) -> Result<Value> {
    match cur.read_u8()? {
        VALUE_TAG_NULL => Ok(Value::Null),
        VALUE_TAG_BOOL => Ok(Value::Bool(cur.read_u8()? != 0)),
        VALUE_TAG_INT => Ok(Value::Int(cur.read_i64()?)),
        VALUE_TAG_FLOAT => Ok(Value::Float(cur.read_f64()?)),
        VALUE_TAG_STR => Ok(Value::Str(cur.read_str()?.to_string())),
        VALUE_TAG_ARRAY => {
            let n = cur.read_u32()? as usize;
            let mut items = Vec::with_capacity(n);
            for _ in 0..n {
                items.push(read_value_full(cur)?);
            }
            Ok(Value::Array(items))
        }
        other => Err(Error::corruption(format!("segment docs: bad value tag {other}"))),
    }
}

/// Write one field's value into its fixed-width directory slot. `Str`/
/// `Array` values must already have been written to the blob by the caller;
/// `blob_offset`/`blob_len` locate those bytes. Every other tag ignores both.
fn encode_value_slot(slot: &mut [u8], value: &Value, blob_offset: u64, blob_len: u32) {
    debug_assert_eq!(slot.len(), VALUE_SLOT_LEN);
    match value {
        Value::Null => slot[0] = VALUE_TAG_NULL,
        Value::Bool(b) => {
            slot[0] = VALUE_TAG_BOOL;
            slot[1] = *b as u8;
        }
        Value::Int(i) => {
            slot[0] = VALUE_TAG_INT;
            slot[1..9].copy_from_slice(&i.to_le_bytes());
        }
        Value::Float(f) => {
            slot[0] = VALUE_TAG_FLOAT;
            slot[1..9].copy_from_slice(&f.to_le_bytes());
        }
        Value::Str(_) | Value::Array(_) => {
            slot[0] = if matches!(value, Value::Str(_)) { VALUE_TAG_STR } else { VALUE_TAG_ARRAY };
            slot[1..9].copy_from_slice(&blob_offset.to_le_bytes());
            slot[9..13].copy_from_slice(&blob_len.to_le_bytes());
        }
    }
}

fn encode_docs_streaming(
    memtable: &MemTable,
    schema: &CollectionSchema,
    doc_w: impl Write,
) -> Result<()> {
    let mut doc_w = CountingWriter::new(doc_w);
    write_header(&mut doc_w, DOC_MAGIC)?;

    let base = memtable.base();
    let end = memtable.next_doc_id(); // exclusive
    let len = (end - base) as usize;
    let num_fields = schema.fields.len();

    let blob_start = doc_w.pos();

    let mut presence = RoaringBitmap::new();
    let mut field_lengths = vec![0u8; len * num_fields * 4];
    let mut values_dir = vec![0u8; len * num_fields * VALUE_SLOT_LEN];
    let mut source_dir = vec![0u8; len * 12]; // u64 offset + u32 length

    for (doc_id, doc) in memtable.iter() {
        presence.insert(doc_id);
        let idx = (doc_id - base) as usize;

        for f in 0..num_fields {
            let field_len = doc.field_lengths.get(f).copied().unwrap_or(0);
            let fl_pos = idx * num_fields * 4 + f * 4;
            field_lengths[fl_pos..fl_pos + 4].copy_from_slice(&field_len.to_le_bytes());

            let value = doc.values.get(f).unwrap_or(&Value::Null);
            let slot_pos = idx * num_fields * VALUE_SLOT_LEN + f * VALUE_SLOT_LEN;

            let (blob_offset, blob_len) = if matches!(value, Value::Str(_) | Value::Array(_)) {
                let offset = doc_w.pos();
                write_value_full(&mut doc_w, value)?;
                (offset, (doc_w.pos() - offset) as u32)
            } else {
                (0, 0)
            };
            encode_value_slot(
                &mut values_dir[slot_pos..slot_pos + VALUE_SLOT_LEN],
                value,
                blob_offset,
                blob_len,
            );
        }

        let source_offset = doc_w.pos();
        serde_json::to_writer(&mut doc_w, &doc.source)?;
        let source_len = (doc_w.pos() - source_offset) as u32;
        let sd_pos = idx * 12;
        source_dir[sd_pos..sd_pos + 8].copy_from_slice(&source_offset.to_le_bytes());
        source_dir[sd_pos + 8..sd_pos + 12].copy_from_slice(&source_len.to_le_bytes());
    }

    let blob_len = doc_w.pos() - blob_start;

    doc_w.write_all(&field_lengths)?;
    doc_w.write_all(&values_dir)?;
    doc_w.write_all(&source_dir)?;

    let mut presence_bytes = Vec::new();
    presence.serialize_into(&mut presence_bytes).expect("writing to a Vec cannot fail");
    doc_w.write_all(&presence_bytes)?;

    let footer_start = doc_w.pos();
    write_u32(&mut doc_w, base)?;
    write_u32(&mut doc_w, end)?;
    write_u32(&mut doc_w, num_fields as u32)?;
    write_u32(&mut doc_w, presence_bytes.len() as u32)?;
    write_u64(&mut doc_w, blob_len)?;
    write_u64(&mut doc_w, footer_start)?;
    Ok(())
}

/// The small, eager part of `.doc`: the doc-id range, field count, presence
/// bitmap, and the byte offsets of each dense section. O(corpus / 8) for the
/// presence bitmap (typically far less once roaring compresses a dense
/// range); everything else here is O(1).
pub(crate) struct DocHeader {
    pub base: DocId,
    pub end: DocId, // exclusive
    pub num_fields: usize,
    pub presence: RoaringBitmap,
    field_lengths_start: usize,
    values_dir_start: usize,
    source_dir_start: usize,
}

pub(crate) fn decode_doc_header(bytes: &[u8]) -> Result<DocHeader> {
    let what = "segment doc store";
    {
        let mut cur = Cursor::new(bytes, what);
        cur.read_header(DOC_MAGIC)?;
    }
    let footer_start = read_footer_start(bytes, what)?;

    let mut cur = Cursor::new(&bytes[footer_start..bytes.len() - FOOTER_POINTER_LEN], what);
    let base = cur.read_u32()?;
    let end = cur.read_u32()?;
    if end < base {
        return Err(Error::corruption("segment doc store: end before base".to_string()));
    }
    let num_fields = cur.read_u32()? as usize;
    let presence_len = cur.read_u32()? as usize;
    let blob_len = cur.read_u64()?;

    let len = (end - base) as usize;
    let field_lengths_start = HEADER_LEN + blob_len as usize;
    let values_dir_start = field_lengths_start + len * num_fields * 4;
    let source_dir_start = values_dir_start + len * num_fields * VALUE_SLOT_LEN;
    let presence_start = source_dir_start + len * 12;
    let expected_end = presence_start + presence_len;
    if expected_end > footer_start {
        return Err(Error::corruption(
            "segment doc store: sections run past the footer".to_string(),
        ));
    }
    let presence =
        RoaringBitmap::deserialize_from(bytes_at(bytes, presence_start, presence_len, what)?)?;

    Ok(DocHeader {
        base,
        end,
        num_fields,
        presence,
        field_lengths_start,
        values_dir_start,
        source_dir_start,
    })
}

fn doc_index(header: &DocHeader, doc_id: DocId) -> Result<usize> {
    doc_id
        .checked_sub(header.base)
        .filter(|&i| header.base + i < header.end)
        .map(|i| i as usize)
        .ok_or_else(|| Error::corruption("segment doc store: doc id out of range".to_string()))
}

/// BM25's `|d|`. Direct offset read, no decode, no allocation.
pub(crate) fn field_len_at(
    bytes: &[u8],
    header: &DocHeader,
    doc_id: DocId,
    field: FieldId,
) -> Result<u32> {
    let idx = doc_index(header, doc_id)?;
    if field as usize >= header.num_fields {
        return Ok(0);
    }
    let pos = header.field_lengths_start + idx * header.num_fields * 4 + field as usize * 4;
    u32_at(bytes, pos, "segment doc store (field length)")
}

/// A document's value for one field. Zero-copy in spirit for `Null`/`Bool`/
/// `Int`/`Float` — the returned `Value` owns nothing that wasn't already a
/// stack value. `Str`/`Array` pay one decode of just that field's bytes.
pub(crate) fn value_at(
    bytes: &[u8],
    header: &DocHeader,
    doc_id: DocId,
    field: FieldId,
) -> Result<Option<Value>> {
    let idx = doc_index(header, doc_id)?;
    if field as usize >= header.num_fields {
        return Ok(None);
    }
    let pos = header.values_dir_start
        + idx * header.num_fields * VALUE_SLOT_LEN
        + field as usize * VALUE_SLOT_LEN;
    let what = "segment doc store (value)";
    let tag = u8_at(bytes, pos, what)?;
    let value = match tag {
        VALUE_TAG_NULL => Value::Null,
        VALUE_TAG_BOOL => Value::Bool(bytes_at(bytes, pos + 1, 1, what)?[0] != 0),
        VALUE_TAG_INT => Value::Int(i64_at(bytes, pos + 1, what)?),
        VALUE_TAG_FLOAT => Value::Float(f64_at(bytes, pos + 1, what)?),
        VALUE_TAG_STR | VALUE_TAG_ARRAY => {
            let offset = u64_at(bytes, pos + 1, what)? as usize;
            let length = u32_at(bytes, pos + 9, what)? as usize;
            let slice = bytes_at(bytes, offset, length, what)?;
            let mut cur = Cursor::new(slice, what);
            read_value_full(&mut cur)?
        }
        other => return Err(Error::corruption(format!("{what}: bad value tag {other}"))),
    };
    Ok(Some(value))
}

/// The document's verbatim source JSON — the one thing here that's actually
/// expensive, paid only when a matched document becomes a returned hit.
pub(crate) fn source_at(bytes: &[u8], header: &DocHeader, doc_id: DocId) -> Result<Option<Json>> {
    let idx = doc_index(header, doc_id)?;
    let what = "segment doc store (source)";
    let pos = header.source_dir_start + idx * 12;
    let offset = u64_at(bytes, pos, what)? as usize;
    let length = u32_at(bytes, pos + 8, what)? as usize;
    if length == 0 {
        return Ok(None);
    }
    let slice = bytes_at(bytes, offset, length, what)?;
    Ok(Some(serde_json::from_slice(slice)?))
}

/// The document's verbatim source JSON as raw bytes, with no parse — used by
/// the streaming merge path to copy a document's stored form into a new
/// segment's blob without round-tripping it through `serde_json::Value`.
pub(crate) fn raw_source_at<'a>(
    bytes: &'a [u8],
    header: &DocHeader,
    doc_id: DocId,
) -> Result<Option<&'a [u8]>> {
    let idx = doc_index(header, doc_id)?;
    let what = "segment doc store (source)";
    let pos = header.source_dir_start + idx * 12;
    let offset = u64_at(bytes, pos, what)? as usize;
    let length = u32_at(bytes, pos + 8, what)? as usize;
    if length == 0 {
        return Ok(None);
    }
    Ok(Some(bytes_at(bytes, offset, length, what)?))
}

/// A field's raw value-slot bytes and, if the tag is `Str`/`Array`, the raw
/// blob bytes they point at — used by the streaming merge path to copy a
/// document's value into a new segment without decoding it into a `Value`
/// and re-encoding, for every field whose slot doesn't need rebasing (i.e.
/// isn't `Str`/`Array`) or whose blob bytes can be copied verbatim.
pub(crate) fn raw_value_slot_at<'a>(
    bytes: &'a [u8],
    header: &DocHeader,
    doc_id: DocId,
    field: FieldId,
) -> Result<(u8, &'a [u8], u64, u32)> {
    let idx = doc_index(header, doc_id)?;
    let what = "segment doc store (value)";
    if field as usize >= header.num_fields {
        return Err(Error::corruption(format!("{what}: field out of range")));
    }
    let pos = header.values_dir_start
        + idx * header.num_fields * VALUE_SLOT_LEN
        + field as usize * VALUE_SLOT_LEN;
    let slot = bytes_at(bytes, pos, VALUE_SLOT_LEN, what)?;
    let tag = slot[0];
    match tag {
        VALUE_TAG_STR | VALUE_TAG_ARRAY => {
            let offset = u64_at(bytes, pos + 1, what)? as usize;
            let length = u32_at(bytes, pos + 9, what)? as usize;
            let blob = bytes_at(bytes, offset, length, what)?;
            Ok((tag, blob, offset as u64, length as u32))
        }
        _ => Ok((tag, slot, 0, 0)),
    }
}

pub(crate) fn field_lengths_row<'a>(
    bytes: &'a [u8],
    header: &DocHeader,
    doc_id: DocId,
) -> Result<&'a [u8]> {
    let idx = doc_index(header, doc_id)?;
    let what = "segment doc store (field length)";
    let pos = header.field_lengths_start + idx * header.num_fields * 4;
    bytes_at(bytes, pos, header.num_fields * 4, what)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tachyon_core::{FieldSchema, FieldType, ParsedDocument};

    use crate::inverted::FieldPostings;

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

    /// A memtable with a live document, a replaced document, and a document
    /// deleted before ever being flushed — the three cases a segment must
    /// tell apart.
    fn fixture() -> (MemTable, CollectionSchema) {
        let schema = schema();
        let mut m = MemTable::new(0, &schema);
        m.insert(doc("1", "wireless mouse", &["red", "blue"], "Logitech", 2999));
        m.insert(doc("2", "mouse pad", &["blue"], "Razer", 1999));
        let hole = m.insert(doc("3", "keyboard", &[], "Logitech", 4999));
        m.remove(hole);
        (m, schema)
    }

    /// Walk every block of one (term, field) and materialize a full
    /// [`FieldPostings`], positions included — the reference decode every
    /// block-boundary test below checks the fast, lazy path against.
    fn decode_term_field_full(
        post_bytes: &[u8],
        header: &PostHeader,
        term_id: u64,
        field: FieldId,
    ) -> Option<FieldPostings> {
        let term_field = decode_term_field_blocks(post_bytes, header, term_id, field).unwrap()?;
        let mut docs = Vec::new();
        for meta in &term_field.blocks {
            let skeleton = decode_block_skeleton(post_bytes, meta).unwrap();
            for i in 0..skeleton.doc_ids.len() {
                let positions =
                    decode_positions_at(post_bytes, skeleton.positions_offsets[i], skeleton.tfs[i])
                        .unwrap();
                docs.push(DocPosting { doc_id: skeleton.doc_ids[i], positions });
            }
        }
        Some(FieldPostings { docs })
    }

    fn term_postings(
        terms_bytes: &[u8],
        post_bytes: &[u8],
        term: &str,
        field: FieldId,
    ) -> Option<FieldPostings> {
        let terms = validate_and_load_fst(terms_bytes, TERMS_MAGIC);
        let term_id = terms.get(term)?;
        let header = decode_post_header(post_bytes).unwrap();
        decode_term_field_full(post_bytes, &header, term_id, field)
    }

    /// Load an `fst::Map` straight from an in-memory encoded blob, skipping
    /// the file/mmap step — used only by tests that don't need real files.
    fn validate_and_load_fst(bytes: &[u8], magic: &[u8; 8]) -> fst::Map<Vec<u8>> {
        let start = validate_header(bytes, magic, "test fst").unwrap();
        fst::Map::new(bytes[start..].to_vec()).unwrap()
    }

    #[test]
    fn holes_are_excluded_from_every_blob() {
        let (m, schema) = fixture();
        let encoded = encode(&m, &schema).unwrap();

        let terms = validate_and_load_fst(&encoded.terms, TERMS_MAGIC);
        assert!(terms.get("keyboard").is_none(), "a term only the hole used must not survive");
        assert!(terms.get("mouse").is_some());

        let ids = validate_and_load_fst(&encoded.ids, IDS_MAGIC);
        assert!(ids.get("3").is_none(), "the hole's id must not resolve");
        assert_eq!(ids.get("1"), Some(0));

        let col_header = decode_col_header(&encoded.col).unwrap();
        let brand = decode_keyword_column(&encoded.col, &col_header, 2).unwrap().unwrap();
        assert_eq!(brand.equals("Logitech").iter().collect::<Vec<_>>(), vec![0]);

        let price = decode_numeric_column(&encoded.col, &col_header, 3).unwrap().unwrap();
        assert!(
            price.range(Some(NumKey::Int(4999)), Some(NumKey::Int(4999))).is_empty(),
            "doc 3's price (4999) must not survive"
        );

        let doc_header = decode_doc_header(&encoded.doc).unwrap();
        assert!(!doc_header.presence.contains(2), "the hole must be absent, not a live doc");
        assert!(source_at(&encoded.doc, &doc_header, 2).unwrap().is_none());
    }

    #[test]
    fn postings_match_positions_and_are_grouped_by_field() {
        let (m, schema) = fixture();
        let encoded = encode(&m, &schema).unwrap();

        // "mouse" occurs in title (field 0) of docs 0 and 1.
        let mouse = term_postings(&encoded.terms, &encoded.post, "mouse", 0).unwrap();
        assert_eq!(mouse.docs.iter().map(|d| d.doc_id).collect::<Vec<_>>(), vec![0, 1]);
        assert_eq!(mouse.docs[0].positions, vec![1], "\"wireless mouse\": mouse is token 1");
        assert_eq!(mouse.docs[1].positions, vec![0], "\"mouse pad\": mouse is token 0");

        // "blue" is a multi-valued tag (field 1) shared by both live docs.
        let blue = term_postings(&encoded.terms, &encoded.post, "blue", 1).unwrap();
        assert_eq!(blue.docs.iter().map(|d| d.doc_id).collect::<Vec<_>>(), vec![0, 1]);
        assert_eq!(blue.docs[0].positions, vec![1 + crate::memtable::MULTI_VALUE_POSITION_GAP]);

        assert!(term_postings(&encoded.terms, &encoded.post, "keyboard", 0).is_none());
    }

    #[test]
    fn lean_doc_id_decode_matches_the_full_postings_decode_minus_positions() {
        let (m, schema) = fixture();
        let encoded = encode(&m, &schema).unwrap();
        let terms = validate_and_load_fst(&encoded.terms, TERMS_MAGIC);
        let header = decode_post_header(&encoded.post).unwrap();

        for (term, field) in [("mouse", 0u16), ("blue", 1), ("pad", 0)] {
            let term_id = terms.get(term).unwrap();
            let expected: Vec<DocId> =
                decode_term_field_full(&encoded.post, &header, term_id, field)
                    .map(|p| p.docs.iter().map(|d| d.doc_id).collect())
                    .unwrap_or_default();

            let lean = decode_term_field_doc_ids(&encoded.post, &header, term_id, field).unwrap();
            assert_eq!(lean, expected, "term {term:?} field {field}");
        }

        // A field the term never occurs in comes back empty, not an error.
        let mouse_id = terms.get("mouse").unwrap();
        assert!(decode_term_field_doc_ids(&encoded.post, &header, mouse_id, 3).unwrap().is_empty());
    }

    #[test]
    fn field_stats_count_only_live_documents() {
        let (m, schema) = fixture();
        let encoded = encode(&m, &schema).unwrap();
        let header = decode_post_header(&encoded.post).unwrap();

        // title (field 0): "wireless mouse" (2 tokens) + "mouse pad" (2
        // tokens); the hole's "keyboard" (1 token) must not count.
        assert_eq!(header.field_doc_count[0], 2);
        assert_eq!(header.field_total_len[0], 4);
    }

    #[test]
    fn columns_round_trip_numeric_and_keyword() {
        let (m, schema) = fixture();
        let encoded = encode(&m, &schema).unwrap();
        let header = decode_col_header(&encoded.col).unwrap();

        let price = decode_numeric_column(&encoded.col, &header, 3).unwrap().unwrap();
        assert_eq!(price.range(Some(NumKey::Int(1999)), Some(NumKey::Int(2999))).len(), 2);

        let brand = decode_keyword_column(&encoded.col, &header, 2).unwrap().unwrap();
        assert_eq!(brand.equals("Logitech").iter().collect::<Vec<_>>(), vec![0]);
        assert_eq!(brand.equals("Razer").iter().collect::<Vec<_>>(), vec![1]);
        assert_eq!(brand.num_values(), 2, "the hole's Logitech must not add a phantom value");

        // A field the schema gives no column at all.
        assert!(decode_numeric_column(&encoded.col, &header, 0).unwrap().is_none());
        assert!(decode_keyword_column(&encoded.col, &header, 0).unwrap().is_none());
    }

    #[test]
    fn doc_store_round_trips_source_values_and_lengths() {
        let (m, schema) = fixture();
        let original = m.get(0).unwrap().clone();
        let encoded = encode(&m, &schema).unwrap();
        let header = decode_doc_header(&encoded.doc).unwrap();

        let source = source_at(&encoded.doc, &header, 0).unwrap().unwrap();
        assert_eq!(source, original.source);

        for (field, expected) in original.values.iter().enumerate() {
            let got = value_at(&encoded.doc, &header, 0, field as FieldId).unwrap().unwrap();
            assert_eq!(&got, expected, "field {field}");
        }
        for (field, &expected_len) in original.field_lengths.iter().enumerate() {
            let got = field_len_at(&encoded.doc, &header, 0, field as FieldId).unwrap();
            assert_eq!(got, expected_len, "field {field}");
        }

        let ids = validate_and_load_fst(&encoded.ids, IDS_MAGIC);
        assert_eq!(ids.get("1"), Some(0));
    }

    #[test]
    fn multi_valued_and_scalar_values_round_trip_through_the_fixed_slot() {
        let schema = CollectionSchema::new(
            "things",
            vec![
                FieldSchema::new("title", FieldType::Text),
                FieldSchema::new("tags", FieldType::Keyword),
                FieldSchema::new("count", FieldType::Int),
                FieldSchema::new("score", FieldType::Float),
                FieldSchema::new("active", FieldType::Bool),
            ],
        );
        let mut m = MemTable::new(0, &schema);
        let raw = json!({
            "id": "1", "title": "thing", "tags": ["a", "b", "c"],
            "count": 42, "score": 3.5, "active": true,
        });
        m.insert(ParsedDocument::parse(raw, &schema).unwrap());

        let encoded = encode(&m, &schema).unwrap();
        let header = decode_doc_header(&encoded.doc).unwrap();

        assert_eq!(
            value_at(&encoded.doc, &header, 0, 1).unwrap().unwrap(),
            Value::Array(vec![
                Value::Str("a".into()),
                Value::Str("b".into()),
                Value::Str("c".into())
            ])
        );
        assert_eq!(value_at(&encoded.doc, &header, 0, 2).unwrap().unwrap(), Value::Int(42));
        assert_eq!(value_at(&encoded.doc, &header, 0, 3).unwrap().unwrap(), Value::Float(3.5));
        assert_eq!(value_at(&encoded.doc, &header, 0, 4).unwrap().unwrap(), Value::Bool(true));
    }

    #[test]
    fn an_all_deleted_memtable_still_encodes_a_valid_empty_segment() {
        let schema = schema();
        let mut m = MemTable::new(0, &schema);
        let a = m.insert(doc("1", "one", &[], "X", 1));
        let b = m.insert(doc("2", "two", &[], "Y", 2));
        m.remove(a);
        m.remove(b);
        assert_eq!(m.next_doc_id(), m.base() + 2, "ids were allocated even though none are live");

        let encoded = encode(&m, &schema).unwrap();
        let terms = validate_and_load_fst(&encoded.terms, TERMS_MAGIC);
        assert!(terms.get("one").is_none());

        let post_header = decode_post_header(&encoded.post).unwrap();
        assert!(post_header.offsets.is_empty());

        let doc_header = decode_doc_header(&encoded.doc).unwrap();
        assert_eq!(doc_header.base, 0);
        assert_eq!(doc_header.end, 2);
        assert!(doc_header.presence.is_empty());
    }

    #[test]
    fn corrupt_headers_are_rejected_not_panicked() {
        let (m, schema) = fixture();
        let encoded = encode(&m, &schema).unwrap();

        let mut bad_terms = encoded.terms.clone();
        bad_terms[0] ^= 0xff;
        assert!(validate_header(&bad_terms, TERMS_MAGIC, "test").is_err());

        let mut bad_post = encoded.post.clone();
        bad_post[0] ^= 0xff;
        assert!(decode_post_header(&bad_post).is_err());

        let mut bad_col = encoded.col.clone();
        bad_col[0] ^= 0xff;
        assert!(decode_col_header(&bad_col).is_err());

        let mut bad_doc = encoded.doc.clone();
        bad_doc[0] ^= 0xff;
        assert!(decode_doc_header(&bad_doc).is_err());

        // Truncated mid-section, not just a bad header.
        let truncated = &encoded.doc[..encoded.doc.len() - 3];
        assert!(decode_doc_header(truncated).is_err());

        let version_mismatch = {
            let mut b = encoded.doc.clone();
            b[8..12].copy_from_slice(&999u32.to_le_bytes());
            b
        };
        assert!(decode_doc_header(&version_mismatch).is_err());
    }

    #[test]
    fn prefix_scan_over_the_fst_matches_the_memtable() {
        let (m, schema) = fixture();
        let encoded = encode(&m, &schema).unwrap();
        let terms = validate_and_load_fst(&encoded.terms, TERMS_MAGIC);

        use fst::{Automaton, IntoStreamer, Streamer};
        let mut found = Vec::new();
        let mut stream = terms.search(fst::automaton::Str::new("mo").starts_with()).into_stream();
        while let Some((t, _)) = stream.next() {
            found.push(std::str::from_utf8(t).unwrap().to_owned());
        }
        let mut expected: Vec<String> =
            m.index().terms_with_prefix("mo").map(str::to_owned).collect();
        found.sort();
        expected.sort();
        assert_eq!(found, expected);
    }

    // --- postings block layout ----------------------------------------

    /// `n` documents, each a single-token "mouse" title at position 0 — every
    /// document occupies exactly one posting, so `n` directly controls how
    /// many blocks a term spans.
    fn broad_fixture(n: usize) -> (MemTable, CollectionSchema) {
        let schema = schema();
        let mut m = MemTable::new(0, &schema);
        for i in 0..n {
            m.insert(doc(&i.to_string(), "mouse", &[], "Brand", i as i64));
        }
        (m, schema)
    }

    #[test]
    fn a_term_spanning_exactly_n_blocks_has_no_partial_last_block() {
        let (m, schema) = broad_fixture(POSTING_BLOCK_SIZE * 2);
        let encoded = encode(&m, &schema).unwrap();
        let terms = validate_and_load_fst(&encoded.terms, TERMS_MAGIC);
        let header = decode_post_header(&encoded.post).unwrap();
        let term_id = terms.get("mouse").unwrap();

        let term_field =
            decode_term_field_blocks(&encoded.post, &header, term_id, 0).unwrap().unwrap();
        assert_eq!(term_field.doc_freq, (POSTING_BLOCK_SIZE * 2) as u32);
        assert_eq!(
            term_field.blocks.len(),
            2,
            "256 docs at block size 128 is exactly two full blocks"
        );
        assert_eq!(term_field.blocks[0].last_doc_id, POSTING_BLOCK_SIZE as u32 - 1);
        assert_eq!(term_field.blocks[1].last_doc_id, POSTING_BLOCK_SIZE as u32 * 2 - 1);

        for meta in &term_field.blocks {
            let skeleton = decode_block_skeleton(&encoded.post, meta).unwrap();
            assert_eq!(skeleton.doc_ids.len(), POSTING_BLOCK_SIZE, "no partial last block");
        }
    }

    #[test]
    fn a_term_with_one_doc_past_a_block_boundary_gets_a_singleton_final_block() {
        let (m, schema) = broad_fixture(POSTING_BLOCK_SIZE + 1);
        let encoded = encode(&m, &schema).unwrap();
        let terms = validate_and_load_fst(&encoded.terms, TERMS_MAGIC);
        let header = decode_post_header(&encoded.post).unwrap();
        let term_id = terms.get("mouse").unwrap();

        let term_field =
            decode_term_field_blocks(&encoded.post, &header, term_id, 0).unwrap().unwrap();
        assert_eq!(term_field.blocks.len(), 2);
        let first = decode_block_skeleton(&encoded.post, &term_field.blocks[0]).unwrap();
        let second = decode_block_skeleton(&encoded.post, &term_field.blocks[1]).unwrap();
        assert_eq!(first.doc_ids.len(), POSTING_BLOCK_SIZE);
        assert_eq!(second.doc_ids.len(), 1, "the 129th doc gets its own singleton final block");
        assert_eq!(second.doc_ids[0], POSTING_BLOCK_SIZE as u32);
        assert_eq!(term_field.blocks[1].last_doc_id, POSTING_BLOCK_SIZE as u32);
    }

    #[test]
    fn decoding_across_a_block_boundary_matches_a_hand_built_expectation() {
        let (m, schema) = broad_fixture(POSTING_BLOCK_SIZE * 2);
        let encoded = encode(&m, &schema).unwrap();
        let terms = validate_and_load_fst(&encoded.terms, TERMS_MAGIC);
        let header = decode_post_header(&encoded.post).unwrap();
        let term_id = terms.get("mouse").unwrap();
        let term_field =
            decode_term_field_blocks(&encoded.post, &header, term_id, 0).unwrap().unwrap();

        // The last doc of block 0 and the first doc of block 1 straddle the
        // boundary — both must decode correctly from their own block.
        let block0 = decode_block_skeleton(&encoded.post, &term_field.blocks[0]).unwrap();
        let block1 = decode_block_skeleton(&encoded.post, &term_field.blocks[1]).unwrap();
        let last_of_0 = block0.doc_ids.len() - 1;

        assert_eq!(block0.doc_ids[last_of_0], POSTING_BLOCK_SIZE as u32 - 1);
        assert_eq!(block1.doc_ids[0], POSTING_BLOCK_SIZE as u32);

        let positions_last_of_0 = decode_positions_at(
            &encoded.post,
            block0.positions_offsets[last_of_0],
            block0.tfs[last_of_0],
        )
        .unwrap();
        let positions_first_of_1 =
            decode_positions_at(&encoded.post, block1.positions_offsets[0], block1.tfs[0]).unwrap();
        assert_eq!(
            positions_last_of_0,
            vec![0],
            "single-token title: \"mouse\" is always position 0"
        );
        assert_eq!(positions_first_of_1, vec![0]);
    }

    #[test]
    fn skipping_a_field_nobody_asked_for_does_not_touch_its_postings() {
        // "mouse" occurs in both title (field 0, broad) and, via the fixture
        // below, nowhere else — this exercises the skip-by-BlockMeta-length
        // path in `decode_term_field_blocks` for a field that isn't present
        // at all, which must come back `None` rather than an error.
        let (m, schema) = broad_fixture(POSTING_BLOCK_SIZE * 2);
        let encoded = encode(&m, &schema).unwrap();
        let terms = validate_and_load_fst(&encoded.terms, TERMS_MAGIC);
        let header = decode_post_header(&encoded.post).unwrap();
        let term_id = terms.get("mouse").unwrap();

        assert!(decode_term_field_blocks(&encoded.post, &header, term_id, 1).unwrap().is_none());
    }

    #[test]
    fn encoding_the_same_memtable_twice_produces_byte_identical_segments() {
        // The pre-v4 encoder's `.col` keyword section iterated a `HashMap`
        // directly, so two encodes of the same live document set were not
        // guaranteed to agree byte-for-byte even though nothing depended on
        // that — this pins the determinism the streaming merge path's own
        // equivalence tests lean on.
        let schema = CollectionSchema::new(
            "things",
            vec![
                FieldSchema::new("title", FieldType::Text),
                FieldSchema::new("brand", FieldType::Keyword).with_facet(true),
            ],
        );
        let mut m = MemTable::new(0, &schema);
        for (i, brand) in
            ["Logitech", "Razer", "Anker", "Corsair", "HyperX", "SteelSeries"].iter().enumerate()
        {
            m.insert(
                ParsedDocument::parse(
                    json!({ "id": i.to_string(), "title": format!("item {i}"), "brand": brand }),
                    &schema,
                )
                .unwrap(),
            );
        }

        let a = encode(&m, &schema).unwrap();
        let b = encode(&m, &schema).unwrap();
        assert_eq!(a.terms, b.terms);
        assert_eq!(a.ids, b.ids);
        assert_eq!(a.post, b.post);
        assert_eq!(a.col, b.col);
        assert_eq!(a.doc, b.doc);
    }
}
