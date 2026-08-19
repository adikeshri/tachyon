//! Shared segment file primitives: the format version, per-file magic
//! headers, and the small bounds-checked binary cursor `.post`/`.col`/`.doc`
//! are read through. `.terms` is an [`fst::Map`] wrapped in the same header;
//! `fst` manages the bytes after it.
//!
//! Bump [`SEGMENT_FORMAT_VERSION`] whenever the byte layout changes, or
//! whenever the tokenizer changes — see `tokenizer.rs`'s warning that a
//! tokenizer change invalidates existing segments.

use std::io::Write;

use tachyon_core::{Error, Result};

/// v4: `.post`, `.col`, and `.doc` moved their directory (offset table /
/// column directory / doc-store section table) from the front of the file to
/// a footer at the end, so a writer can stream payload bytes forward as it
/// goes and only needs to know the directory's *contents* — never its
/// position — once everything before it has already been written. Nothing
/// about how a query reads a segment changed; this only changes how one gets
/// written, which is `tachyon-engine`'s `SegmentWriter`/merge path, not this
/// crate's query-time decode functions. Also widens every byte offset that
/// pointed into a variable-length blob (`.doc`'s value/source blob, `.col`'s
/// per-field data) from `u32` to `u64` — those blobs hold document text and
/// scale with corpus size, and a `u32` offset silently wrapped past 4 GiB.
///
/// v3: `.post` postings are grouped into fixed-size blocks with per-block
/// skip metadata (max doc id, max term frequency, byte offset/length), so a
/// query can skip a block — or jump straight to one — without decoding the
/// blocks in between. v1/v2/v3 segments are not readable by this build —
/// same policy as a tokenizer change, which also invalidates existing
/// segments.
pub const SEGMENT_FORMAT_VERSION: u32 = 4;

/// Byte length of every segment file's header (8-byte magic + 4-byte version).
pub const HEADER_LEN: usize = 12;

/// Byte length of the trailing pointer every footer-based file (`.post`,
/// `.col`, `.doc`) ends with: the absolute file offset its footer starts at.
pub const FOOTER_POINTER_LEN: usize = 8;

pub const TERMS_MAGIC: &[u8; 8] = b"TCHYTRM\0";
pub const IDS_MAGIC: &[u8; 8] = b"TCHYIDS\0";
pub const POST_MAGIC: &[u8; 8] = b"TCHYPST\0";
pub const COL_MAGIC: &[u8; 8] = b"TCHYCOL\0";
pub const DOC_MAGIC: &[u8; 8] = b"TCHYDOC\0";

/// Every `write_*` primitive is generic over [`Write`] rather than tied to
/// `Vec<u8>`, so the exact same encoding logic works whether the destination
/// is a real file (the streaming path segment writing and merging use) or an
/// in-memory buffer (what `codec::encode`'s tests build against) — one
/// implementation of the byte format, not two that could drift apart. A
/// `Vec<u8>`'s own `Write` impl never fails, so the `Result` these return
/// costs nothing there; it matters only for the real-file case.
pub fn write_header(w: &mut impl Write, magic: &[u8; 8]) -> Result<()> {
    w.write_all(magic)?;
    w.write_all(&SEGMENT_FORMAT_VERSION.to_le_bytes())?;
    Ok(())
}

pub fn write_u8(w: &mut impl Write, v: u8) -> Result<()> {
    w.write_all(&[v])?;
    Ok(())
}
pub fn write_u32(w: &mut impl Write, v: u32) -> Result<()> {
    w.write_all(&v.to_le_bytes())?;
    Ok(())
}
pub fn write_u64(w: &mut impl Write, v: u64) -> Result<()> {
    w.write_all(&v.to_le_bytes())?;
    Ok(())
}
pub fn write_i64(w: &mut impl Write, v: i64) -> Result<()> {
    w.write_all(&v.to_le_bytes())?;
    Ok(())
}
pub fn write_f64(w: &mut impl Write, v: f64) -> Result<()> {
    w.write_all(&v.to_le_bytes())?;
    Ok(())
}
pub fn write_bytes(w: &mut impl Write, bytes: &[u8]) -> Result<()> {
    write_u32(w, bytes.len() as u32)?;
    w.write_all(bytes)?;
    Ok(())
}
pub fn write_str(w: &mut impl Write, s: &str) -> Result<()> {
    write_bytes(w, s.as_bytes())
}

/// Wraps any [`Write`] to track the number of bytes written so far, so a
/// streaming writer can record "the block I'm about to write starts at byte
/// N" without the sink itself (a plain [`std::fs::File`] most of the time)
/// being seekable or otherwise able to answer that question.
pub struct CountingWriter<W> {
    inner: W,
    pos: u64,
}

impl<W> CountingWriter<W> {
    pub fn new(inner: W) -> CountingWriter<W> {
        CountingWriter { inner, pos: 0 }
    }

    /// Bytes written so far.
    pub fn pos(&self) -> u64 {
        self.pos
    }
}

impl<W: Write> Write for CountingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.pos += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
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

fn slice_at<'a>(
    bytes: &'a [u8],
    offset: usize,
    len: usize,
    what: &'static str,
) -> Result<&'a [u8]> {
    let end = offset
        .checked_add(len)
        .ok_or_else(|| Error::corruption(format!("{what}: offset overflow")))?;
    bytes.get(offset..end).ok_or_else(|| {
        Error::corruption(format!(
            "{what}: offset out of range (offset={offset} len={len} end={end} bytes.len()={})",
            bytes.len()
        ))
    })
}

pub fn u8_at(bytes: &[u8], offset: usize, what: &'static str) -> Result<u8> {
    Ok(slice_at(bytes, offset, 1, what)?[0])
}

pub fn u32_at(bytes: &[u8], offset: usize, what: &'static str) -> Result<u32> {
    Ok(u32::from_le_bytes(slice_at(bytes, offset, 4, what)?.try_into().expect("4 bytes")))
}

pub fn u64_at(bytes: &[u8], offset: usize, what: &'static str) -> Result<u64> {
    Ok(u64::from_le_bytes(slice_at(bytes, offset, 8, what)?.try_into().expect("8 bytes")))
}

pub fn i64_at(bytes: &[u8], offset: usize, what: &'static str) -> Result<i64> {
    Ok(i64::from_le_bytes(slice_at(bytes, offset, 8, what)?.try_into().expect("8 bytes")))
}

pub fn f64_at(bytes: &[u8], offset: usize, what: &'static str) -> Result<f64> {
    Ok(f64::from_le_bytes(slice_at(bytes, offset, 8, what)?.try_into().expect("8 bytes")))
}

pub fn bytes_at<'a>(
    bytes: &'a [u8],
    offset: usize,
    len: usize,
    what: &'static str,
) -> Result<&'a [u8]> {
    slice_at(bytes, offset, len, what)
}
