//! Write-ahead log (PRD §7.2, §10: "append to WAL → update in-memory index →
//! acknowledge write").
//!
//! # File format
//!
//! ```text
//! header:  "TCHYWAL\0"  u32 version
//! frame:   u32 payload_len  u32 crc32(payload)  payload
//! ```
//!
//! All integers are little-endian. The payload is a JSON-encoded [`WalRecord`].
//! JSON costs a few bytes against a binary encoding, but the WAL is truncated
//! at every flush so its size is bounded by the memtable threshold, the cost is
//! dwarfed by the fsync it sits behind, and a log an operator can read with
//! `strings` is worth a lot during an incident.
//!
//! # Torn tails
//!
//! A crash mid-append leaves a partial frame. Replay stops at the first frame
//! that is short or fails its CRC and truncates the file there: a frame that
//! was never fully written was also never acknowledged, so no client was told
//! it was durable.

use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use serde_json::Value as Json;

use tachyon_core::{Error, Result};

const MAGIC: &[u8; 8] = b"TCHYWAL\0";
const WAL_FORMAT_VERSION: u32 = 1;
const HEADER_LEN: u64 = 12;

/// Refuse to allocate for an implausible frame length. A single document is
/// capped well below this; anything larger means a corrupt length prefix.
const MAX_FRAME_LEN: u32 = 64 * 1024 * 1024;

/// One logical mutation. `seq` is monotonically increasing within a collection
/// and is what the flush checkpoint refers to.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum WalRecord {
    /// Insert or replace the document with this `id` (PRD §7.2: upsert by id).
    Upsert { seq: u64, doc: Json },
    /// Remove the document with this id, if present.
    Delete { seq: u64, id: String },
}

impl WalRecord {
    pub fn seq(&self) -> u64 {
        match self {
            WalRecord::Upsert { seq, .. } | WalRecord::Delete { seq, .. } => *seq,
        }
    }
}

/// Borrowing twin of [`WalRecord`] used on the write path, so appending a
/// document does not require cloning its JSON. Serializes identically.
#[derive(Debug, Serialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum WalRecordRef<'a> {
    Upsert { seq: u64, doc: &'a Json },
    Delete { seq: u64, id: &'a str },
}

/// When to force data to the platter.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum SyncPolicy {
    /// fsync before acknowledging every batch. No acknowledged write is ever
    /// lost. The default.
    #[default]
    Always,
    /// fsync at most once per interval. A crash can lose up to `interval` worth
    /// of acknowledged writes; markedly faster for bulk ingest.
    Interval(Duration),
    /// Never fsync explicitly; durability is left to the OS. Only appropriate
    /// for rebuildable data.
    Never,
}

/// An append handle on one WAL generation.
pub struct Wal {
    file: File,
    path: PathBuf,
    len: u64,
    policy: SyncPolicy,
    last_sync: Instant,
    /// Bytes appended since the last successful fsync.
    unsynced: u64,
}

impl Wal {
    /// Open `path` for appending, creating it (with a header) if absent.
    pub fn open(path: impl Into<PathBuf>, policy: SyncPolicy) -> Result<Wal> {
        let path = path.into();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let mut file =
            OpenOptions::new().read(true).write(true).create(true).truncate(false).open(&path)?;
        let mut len = file.metadata()?.len();

        if len == 0 {
            file.write_all(MAGIC)?;
            file.write_all(&WAL_FORMAT_VERSION.to_le_bytes())?;
            file.sync_all()?;
            if let Some(parent) = path.parent() {
                crate::layout::sync_dir(parent)?;
            }
            len = HEADER_LEN;
        } else {
            verify_header(&mut file, &path)?;
        }

        file.seek(SeekFrom::End(0))?;
        Ok(Wal { file, path, len, policy, last_sync: Instant::now(), unsynced: 0 })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Current file size in bytes, used to decide when to flush a memtable.
    pub fn size(&self) -> u64 {
        self.len
    }

    pub fn append<T: Serialize>(&mut self, record: &T) -> Result<()> {
        self.append_batch(std::slice::from_ref(record))
    }

    /// Append a batch as a single write, then honour the sync policy.
    ///
    /// Batching matters: the fsync, not the write, is what a bulk indexing run
    /// is paying for, and one batch costs one fsync regardless of size.
    ///
    /// Generic over the record type so the write path can pass
    /// [`WalRecordRef`] and avoid cloning document bodies.
    pub fn append_batch<T: Serialize>(&mut self, records: &[T]) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }

        let mut buf = Vec::with_capacity(records.len() * 256);
        for record in records {
            let payload = serde_json::to_vec(record)?;
            if payload.len() as u64 > MAX_FRAME_LEN as u64 {
                return Err(Error::validation(format!(
                    "record is {} bytes, exceeding the {MAX_FRAME_LEN} byte limit",
                    payload.len()
                )));
            }
            let crc = crc32fast::hash(&payload);
            buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
            buf.extend_from_slice(&crc.to_le_bytes());
            buf.extend_from_slice(&payload);
        }

        self.file.write_all(&buf)?;
        self.len += buf.len() as u64;
        self.unsynced += buf.len() as u64;
        self.maybe_sync()?;
        Ok(())
    }

    fn maybe_sync(&mut self) -> Result<()> {
        match self.policy {
            SyncPolicy::Always => self.sync(),
            SyncPolicy::Interval(interval) => {
                if self.last_sync.elapsed() >= interval {
                    self.sync()
                } else {
                    Ok(())
                }
            }
            SyncPolicy::Never => Ok(()),
        }
    }

    /// Force everything appended so far to durable storage.
    pub fn sync(&mut self) -> Result<()> {
        if self.unsynced == 0 {
            return Ok(());
        }
        // sync_data, not sync_all: the file's length is the only metadata that
        // matters here and sync_data flushes it, at a lower cost than a full
        // inode sync on every batch.
        self.file.sync_data()?;
        self.unsynced = 0;
        self.last_sync = Instant::now();
        Ok(())
    }

    /// Remove this WAL generation from disk. Called once its contents are
    /// durably captured in a segment.
    pub fn remove(self) -> Result<()> {
        let path = self.path.clone();
        drop(self.file);
        match std::fs::remove_file(&path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        if let Some(parent) = path.parent() {
            crate::layout::sync_dir(parent)?;
        }
        Ok(())
    }
}

/// Outcome of reading a WAL generation from disk.
#[derive(Debug, Default)]
pub struct WalScan {
    pub records: Vec<WalRecord>,
    /// `Some(offset)` if a torn or corrupt frame was found and the file was
    /// truncated there. Worth logging loudly: it means the process died mid-append.
    pub truncated_at: Option<u64>,
}

/// Read every intact record from a WAL, truncating a torn tail if present.
///
/// A missing file is not an error — it means no un-flushed writes exist.
pub fn read(path: &Path) -> Result<WalScan> {
    let mut file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(WalScan::default()),
        Err(e) => return Err(e.into()),
    };

    let total_len = file.metadata()?.len();
    if total_len == 0 {
        return Ok(WalScan::default());
    }
    verify_header(&mut file, path)?;

    let mut reader = BufReader::new(file);
    let mut records = Vec::new();
    let mut offset = HEADER_LEN;
    let mut truncated_at = None;

    loop {
        let mut header = [0u8; 8];
        match read_exact_or_eof(&mut reader, &mut header)? {
            ReadOutcome::Eof => break,
            ReadOutcome::Short => {
                truncated_at = Some(offset);
                break;
            }
            ReadOutcome::Full => {}
        }

        let payload_len = u32::from_le_bytes(header[0..4].try_into().expect("4 bytes"));
        let expected_crc = u32::from_le_bytes(header[4..8].try_into().expect("4 bytes"));

        if payload_len > MAX_FRAME_LEN || offset + 8 + payload_len as u64 > total_len {
            truncated_at = Some(offset);
            break;
        }

        let mut payload = vec![0u8; payload_len as usize];
        match read_exact_or_eof(&mut reader, &mut payload)? {
            ReadOutcome::Full => {}
            _ => {
                truncated_at = Some(offset);
                break;
            }
        }

        if crc32fast::hash(&payload) != expected_crc {
            truncated_at = Some(offset);
            break;
        }

        match serde_json::from_slice::<WalRecord>(&payload) {
            Ok(record) => records.push(record),
            Err(e) => {
                // The CRC matched, so the bytes are exactly what was written:
                // this is a format mismatch, not a torn write, and silently
                // dropping it would lose an acknowledged mutation.
                return Err(Error::corruption(format!(
                    "{}: frame at offset {offset} is not a valid WAL record: {e}",
                    path.display()
                )));
            }
        }

        offset += 8 + payload_len as u64;
    }

    if let Some(at) = truncated_at {
        tracing::warn!(
            wal = %path.display(),
            offset = at,
            recovered = records.len(),
            "torn tail in write-ahead log; truncating"
        );
        let file = OpenOptions::new().write(true).open(path)?;
        file.set_len(at)?;
        file.sync_all()?;
    }

    Ok(WalScan { records, truncated_at })
}

enum ReadOutcome {
    Full,
    Short,
    Eof,
}

fn read_exact_or_eof<R: Read>(reader: &mut R, buf: &mut [u8]) -> Result<ReadOutcome> {
    let mut filled = 0;
    while filled < buf.len() {
        match reader.read(&mut buf[filled..]) {
            Ok(0) => break,
            Ok(n) => filled += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e.into()),
        }
    }
    Ok(match filled {
        0 => ReadOutcome::Eof,
        n if n == buf.len() => ReadOutcome::Full,
        _ => ReadOutcome::Short,
    })
}

fn verify_header(file: &mut File, path: &Path) -> Result<()> {
    file.seek(SeekFrom::Start(0))?;
    let mut header = [0u8; HEADER_LEN as usize];
    file.read_exact(&mut header).map_err(|e| {
        Error::corruption(format!("{}: could not read WAL header: {e}", path.display()))
    })?;

    if &header[0..8] != MAGIC {
        return Err(Error::corruption(format!("{} is not a Tachyon WAL file", path.display())));
    }
    let version = u32::from_le_bytes(header[8..12].try_into().expect("4 bytes"));
    if version != WAL_FORMAT_VERSION {
        return Err(Error::corruption(format!(
            "{} is WAL format version {version}, this build understands {WAL_FORMAT_VERSION}",
            path.display()
        )));
    }
    file.seek(SeekFrom::Start(HEADER_LEN))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn upsert(seq: u64, id: &str) -> WalRecord {
        WalRecord::Upsert { seq, doc: json!({ "id": id, "title": "a mouse" }) }
    }

    #[test]
    fn round_trips_records() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("0000000001.wal");

        let mut wal = Wal::open(&path, SyncPolicy::Always).unwrap();
        wal.append(&upsert(1, "a")).unwrap();
        wal.append_batch(&[upsert(2, "b"), WalRecord::Delete { seq: 3, id: "a".into() }]).unwrap();
        drop(wal);

        let scan = read(&path).unwrap();
        assert!(scan.truncated_at.is_none());
        assert_eq!(scan.records.len(), 3);
        assert_eq!(scan.records[0], upsert(1, "a"));
        assert_eq!(scan.records[2], WalRecord::Delete { seq: 3, id: "a".into() });
    }

    #[test]
    fn reopening_appends_rather_than_overwrites() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("w.wal");

        let mut wal = Wal::open(&path, SyncPolicy::Always).unwrap();
        wal.append(&upsert(1, "a")).unwrap();
        drop(wal);

        let mut wal = Wal::open(&path, SyncPolicy::Always).unwrap();
        wal.append(&upsert(2, "b")).unwrap();
        drop(wal);

        assert_eq!(read(&path).unwrap().records.len(), 2);
    }

    #[test]
    fn missing_file_reads_as_empty() {
        let dir = tempfile::tempdir().unwrap();
        let scan = read(&dir.path().join("absent.wal")).unwrap();
        assert!(scan.records.is_empty());
        assert!(scan.truncated_at.is_none());
    }

    #[test]
    fn recovers_records_before_a_torn_tail() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("w.wal");

        let mut wal = Wal::open(&path, SyncPolicy::Always).unwrap();
        wal.append(&upsert(1, "a")).unwrap();
        wal.append(&upsert(2, "b")).unwrap();
        let good_len = wal.size();
        drop(wal);

        // Simulate a crash part-way through a third frame.
        let file = OpenOptions::new().append(true).open(&path).unwrap();
        let mut file = file;
        file.write_all(&[9u8, 0, 0, 0, 1, 2, 3]).unwrap();
        drop(file);

        let scan = read(&path).unwrap();
        assert_eq!(scan.records.len(), 2);
        assert_eq!(scan.truncated_at, Some(good_len));
        // The truncation is persisted, so the next append starts from clean bytes.
        assert_eq!(std::fs::metadata(&path).unwrap().len(), good_len);

        let mut wal = Wal::open(&path, SyncPolicy::Always).unwrap();
        wal.append(&upsert(3, "c")).unwrap();
        drop(wal);
        assert_eq!(read(&path).unwrap().records.len(), 3);
    }

    #[test]
    fn detects_bit_rot_in_a_payload() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("w.wal");

        let mut wal = Wal::open(&path, SyncPolicy::Always).unwrap();
        wal.append(&upsert(1, "a")).unwrap();
        wal.append(&upsert(2, "b")).unwrap();
        drop(wal);

        // Flip a byte inside the first payload; its CRC no longer matches.
        let mut bytes = std::fs::read(&path).unwrap();
        let payload_start = HEADER_LEN as usize + 8;
        bytes[payload_start + 2] ^= 0xff;
        std::fs::write(&path, &bytes).unwrap();

        let scan = read(&path).unwrap();
        assert!(scan.records.is_empty());
        assert_eq!(scan.truncated_at, Some(HEADER_LEN));
    }

    #[test]
    fn rejects_a_foreign_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("w.wal");
        std::fs::write(&path, b"this is not a wal file at all").unwrap();
        assert!(read(&path).is_err());
        assert!(Wal::open(&path, SyncPolicy::Always).is_err());
    }

    #[test]
    fn size_tracks_appends() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("w.wal");
        let mut wal = Wal::open(&path, SyncPolicy::Never).unwrap();
        assert_eq!(wal.size(), HEADER_LEN);
        wal.append(&upsert(1, "a")).unwrap();
        assert!(wal.size() > HEADER_LEN);
        assert_eq!(wal.size(), std::fs::metadata(&path).unwrap().len());
    }

    #[test]
    fn remove_deletes_the_generation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("w.wal");
        let mut wal = Wal::open(&path, SyncPolicy::Always).unwrap();
        wal.append(&upsert(1, "a")).unwrap();
        wal.remove().unwrap();
        assert!(!path.exists());
    }
}
