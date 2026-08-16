//! Shared segment file primitives: the format version, per-file magic
//! headers, and the small bounds-checked binary cursor `.post`/`.col`/`.doc`
//! are read through. `.terms` is an [`fst::Map`] wrapped in the same header;
//! `fst` manages the bytes after it.
//!
//! Bump [`SEGMENT_FORMAT_VERSION`] whenever the byte layout changes, or
//! whenever the tokenizer changes — see `tokenizer.rs`'s warning that a
//! tokenizer change invalidates existing segments.

use tachyon_core::{Error, Result};

/// v2: postings, columns, and the document store are read lazily from mmap'd
/// files via offset tables instead of being fully decoded at open time. v1
/// segments are not readable by this build — same policy as a tokenizer
/// change, which also invalidates existing segments.
pub const SEGMENT_FORMAT_VERSION: u32 = 2;

/// Byte length of every segment file's header (8-byte magic + 4-byte version).
pub const HEADER_LEN: usize = 12;

pub const TERMS_MAGIC: &[u8; 8] = b"TCHYTRM\0";
pub const IDS_MAGIC: &[u8; 8] = b"TCHYIDS\0";
pub const POST_MAGIC: &[u8; 8] = b"TCHYPST\0";
pub const COL_MAGIC: &[u8; 8] = b"TCHYCOL\0";
pub const DOC_MAGIC: &[u8; 8] = b"TCHYDOC\0";

pub fn write_header(buf: &mut Vec<u8>, magic: &[u8; 8]) {
    buf.extend_from_slice(magic);
    buf.extend_from_slice(&SEGMENT_FORMAT_VERSION.to_le_bytes());
}

pub fn write_u8(buf: &mut Vec<u8>, v: u8) {
    buf.push(v);
}
pub fn write_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_le_bytes());
}
pub fn write_u64(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_le_bytes());
}
pub fn write_i64(buf: &mut Vec<u8>, v: i64) {
    buf.extend_from_slice(&v.to_le_bytes());
}
pub fn write_f64(buf: &mut Vec<u8>, v: f64) {
    buf.extend_from_slice(&v.to_le_bytes());
}
pub fn write_bytes(buf: &mut Vec<u8>, bytes: &[u8]) {
    write_u32(buf, bytes.len() as u32);
    buf.extend_from_slice(bytes);
}
pub fn write_str(buf: &mut Vec<u8>, s: &str) {
    write_bytes(buf, s.as_bytes());
}

/// A read cursor over a decoded segment blob, with bounds-checked primitives.
/// A read past the end, or a header mismatch, is [`Error::corruption`] —
/// never a panic, since these bytes come from disk and may be truncated or
/// tampered with.
pub struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
    what: &'static str,
}

impl<'a> Cursor<'a> {
    pub fn new(bytes: &'a [u8], what: &'static str) -> Cursor<'a> {
        Cursor { bytes, pos: 0, what }
    }

    /// Byte offset consumed so far, e.g. to find where a header-prefixed
    /// sub-format (like the `.terms` file's `fst::Map` payload) begins.
    pub fn position(&self) -> usize {
        self.pos
    }

    fn err(&self) -> Error {
        Error::corruption(format!("{}: truncated or malformed", self.what))
    }

    pub fn read_header(&mut self, magic: &[u8; 8]) -> Result<()> {
        let got = self.take(8)?;
        if got != &magic[..] {
            return Err(Error::corruption(format!("{}: not a Tachyon segment file", self.what)));
        }
        let version = self.read_u32()?;
        if version != SEGMENT_FORMAT_VERSION {
            return Err(Error::corruption(format!(
                "{}: segment format version {version}, this build understands \
                 {SEGMENT_FORMAT_VERSION}",
                self.what
            )));
        }
        Ok(())
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self.pos.checked_add(n).ok_or_else(|| self.err())?;
        if end > self.bytes.len() {
            return Err(self.err());
        }
        let slice = &self.bytes[self.pos..end];
        self.pos = end;
        Ok(slice)
    }

    pub fn read_u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    pub fn read_u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().expect("4 bytes")))
    }

    pub fn read_u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().expect("8 bytes")))
    }

    pub fn read_i64(&mut self) -> Result<i64> {
        Ok(i64::from_le_bytes(self.take(8)?.try_into().expect("8 bytes")))
    }

    pub fn read_f64(&mut self) -> Result<f64> {
        Ok(f64::from_le_bytes(self.take(8)?.try_into().expect("8 bytes")))
    }

    /// A length-prefixed byte string.
    pub fn read_bytes(&mut self) -> Result<&'a [u8]> {
        let len = self.read_u32()? as usize;
        self.take(len)
    }

    pub fn read_str(&mut self) -> Result<&'a str> {
        std::str::from_utf8(self.read_bytes()?).map_err(|_| self.err())
    }

    /// Exactly `n` bytes, with no length prefix of their own — for a length
    /// already known from elsewhere (e.g. a field read just before this one).
    pub fn read_exact(&mut self, n: usize) -> Result<&'a [u8]> {
        self.take(n)
    }

    /// Advance past `n` bytes without reading them — for skipping data a
    /// caller doesn't need (e.g. positions, when only a document count is
    /// wanted) without paying to decode and allocate it.
    pub fn skip(&mut self, n: usize) -> Result<()> {
        self.take(n).map(|_| ())
    }
}

// --- direct-offset access -------------------------------------------------
//
// Used for the fixed-width sections of `.doc` (`field_lengths`, `values_dir`)
// that are addressed directly by `doc_id`/`field` arithmetic rather than
// walked sequentially — no `Cursor`, no decode step, just a bounds-checked
// read at a computed byte offset. This is what makes `field_len` and a
// numeric/bool `value` genuinely zero-copy: the mmap'd page is touched, but
// nothing is parsed or allocated.

fn slice_at<'a>(bytes: &'a [u8], offset: usize, len: usize, what: &'static str) -> Result<&'a [u8]> {
    let end = offset
        .checked_add(len)
        .ok_or_else(|| Error::corruption(format!("{what}: offset overflow")))?;
    bytes
        .get(offset..end)
        .ok_or_else(|| Error::corruption(format!("{what}: offset out of range")))
}

pub fn u8_at(bytes: &[u8], offset: usize, what: &'static str) -> Result<u8> {
    Ok(slice_at(bytes, offset, 1, what)?[0])
}

pub fn u32_at(bytes: &[u8], offset: usize, what: &'static str) -> Result<u32> {
    Ok(u32::from_le_bytes(slice_at(bytes, offset, 4, what)?.try_into().expect("4 bytes")))
}

pub fn i64_at(bytes: &[u8], offset: usize, what: &'static str) -> Result<i64> {
    Ok(i64::from_le_bytes(slice_at(bytes, offset, 8, what)?.try_into().expect("8 bytes")))
}

pub fn f64_at(bytes: &[u8], offset: usize, what: &'static str) -> Result<f64> {
    Ok(f64::from_le_bytes(slice_at(bytes, offset, 8, what)?.try_into().expect("8 bytes")))
}

pub fn bytes_at<'a>(bytes: &'a [u8], offset: usize, len: usize, what: &'static str) -> Result<&'a [u8]> {
    slice_at(bytes, offset, len, what)
}
