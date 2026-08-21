//! On-disk layout.
//!
//! ```text
//! <data_dir>/
//!   TACHYON              marker file, holds the store format version
//!   collections/
//!     <name>/
//!       schema.json      immutable collection schema (PRD §7.1)
//!       state.json       committed segments + WAL checkpoint (atomically replaced)
//!       wal/
//!         <gen>.wal      append-only log; a new generation starts at each flush
//!       segments/
//!         <id>.<ext>     immutable segment files
//! ```
//!
//! Everything a collection owns lives under one directory so dropping a
//! collection is a single `remove_dir_all`, and so a collection can be copied
//! between machines by copying a directory.

use std::path::{Path, PathBuf};

use tachyon_core::{Error, Result};

/// Bumped when the on-disk format changes incompatibly.
pub const STORE_FORMAT_VERSION: u32 = 1;

/// Marker file at the root of a data directory.
pub const MARKER_FILE: &str = "TACHYON";

pub const SCHEMA_FILE: &str = "schema.json";
pub const STATE_FILE: &str = "state.json";
pub const COLLECTIONS_DIR: &str = "collections";
pub const WAL_DIR: &str = "wal";
pub const SEGMENTS_DIR: &str = "segments";

/// Resolves paths within a data directory. Cheap to clone.
#[derive(Debug, Clone)]
pub struct Layout {
    root: PathBuf,
}

impl Layout {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Layout { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn marker_file(&self) -> PathBuf {
        self.root.join(MARKER_FILE)
    }

    pub fn collections_dir(&self) -> PathBuf {
        self.root.join(COLLECTIONS_DIR)
    }

    pub fn collection_dir(&self, name: &str) -> PathBuf {
        self.collections_dir().join(name)
    }

    pub fn schema_file(&self, name: &str) -> PathBuf {
        self.collection_dir(name).join(SCHEMA_FILE)
    }

    pub fn state_file(&self, name: &str) -> PathBuf {
        self.collection_dir(name).join(STATE_FILE)
    }

    pub fn wal_dir(&self, name: &str) -> PathBuf {
        self.collection_dir(name).join(WAL_DIR)
    }

    pub fn wal_file(&self, name: &str, generation: u64) -> PathBuf {
        self.wal_dir(name).join(format!("{generation:010}.wal"))
    }

    /// Every WAL generation number currently on disk for a collection,
    /// ascending. A collection that predates off-lock flush, or that has
    /// never flushed, has exactly one.
    pub fn list_wal_generations(&self, name: &str) -> Result<Vec<u64>> {
        let dir = self.wal_dir(name);
        if !dir.exists() {
            return Ok(Vec::new());
        }
        let mut gens = Vec::new();
        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            let file_name = entry.file_name();
            let Some(stem) =
                Path::new(&file_name).file_stem().and_then(|s| s.to_str()).filter(|_| {
                    Path::new(&file_name).extension().and_then(|e| e.to_str()) == Some("wal")
                })
            else {
                continue;
            };
            if let Ok(gen) = stem.parse::<u64>() {
                gens.push(gen);
            }
        }
        gens.sort_unstable();
        Ok(gens)
    }

    pub fn segments_dir(&self, name: &str) -> PathBuf {
        self.collection_dir(name).join(SEGMENTS_DIR)
    }

    /// Path of one file belonging to a segment, e.g. `ext = "post"`.
    pub fn segment_file(&self, name: &str, segment_id: u64, ext: &str) -> PathBuf {
        self.segments_dir(name).join(format!("{segment_id:010}.{ext}"))
    }

    /// Create the data directory if needed and verify we understand its format.
    pub fn initialize(&self) -> Result<()> {
        std::fs::create_dir_all(self.collections_dir())?;
        let marker = self.marker_file();
        if marker.exists() {
            let contents = std::fs::read_to_string(&marker)?;
            let version: u32 = contents.trim().parse().map_err(|_| {
                Error::corruption(format!("{} does not contain a version number", marker.display()))
            })?;
            if version != STORE_FORMAT_VERSION {
                return Err(Error::corruption(format!(
                    "data directory {} is format version {version}, this build understands {STORE_FORMAT_VERSION}",
                    self.root.display()
                )));
            }
        } else {
            std::fs::write(&marker, format!("{STORE_FORMAT_VERSION}\n"))?;
        }
        Ok(())
    }

    /// Names of collections that have a persisted schema.
    pub fn list_collections(&self) -> Result<Vec<String>> {
        let dir = self.collections_dir();
        if !dir.exists() {
            return Ok(Vec::new());
        }
        let mut names = Vec::new();
        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            if self.schema_file(&name).exists() {
                names.push(name);
            }
        }
        names.sort();
        Ok(names)
    }
}

/// Write a file such that readers only ever observe the old or new contents,
/// never a partial write: write a sibling temp file, fsync it, then rename.
pub fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    use std::io::Write;

    let parent = path
        .parent()
        .ok_or_else(|| Error::internal(format!("{} has no parent directory", path.display())))?;
    std::fs::create_dir_all(parent)?;

    let tmp = path.with_extension("tmp");
    {
        let mut file = std::fs::File::create(&tmp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
    }
    std::fs::rename(&tmp, path)?;
    // Also fsync the directory, or the rename itself may not survive a crash.
    sync_dir(parent)?;
    Ok(())
}

/// fsync a directory so that entry creations/renames within it are durable.
pub fn sync_dir(dir: &Path) -> Result<()> {
    // Opening a directory read-only and syncing it is the portable-enough way
    // to flush its metadata on Linux and macOS. Not meaningful on Windows,
    // where the open itself fails; treat that as a no-op.
    match std::fs::File::open(dir) {
        Ok(file) => {
            let _ = file.sync_all();
            Ok(())
        }
        Err(e) if cfg!(windows) => {
            tracing::debug!(dir = %dir.display(), error = %e, "skipping directory fsync");
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paths_are_nested_under_the_collection() {
        let l = Layout::new("/data");
        assert_eq!(l.schema_file("products"), Path::new("/data/collections/products/schema.json"));
        assert_eq!(
            l.wal_file("products", 7),
            Path::new("/data/collections/products/wal/0000000007.wal")
        );
        assert_eq!(
            l.segment_file("products", 3, "post"),
            Path::new("/data/collections/products/segments/0000000003.post")
        );
    }

    #[test]
    fn initialize_is_idempotent_and_version_checked() {
        let dir = tempfile::tempdir().unwrap();
        let l = Layout::new(dir.path());
        l.initialize().unwrap();
        l.initialize().unwrap();
        assert!(l.marker_file().exists());

        std::fs::write(l.marker_file(), "999\n").unwrap();
        assert!(l.initialize().is_err());
    }

    #[test]
    fn lists_wal_generations_ascending_and_ignores_stray_files() {
        let dir = tempfile::tempdir().unwrap();
        let l = Layout::new(dir.path());
        std::fs::create_dir_all(l.wal_dir("products")).unwrap();
        std::fs::write(l.wal_file("products", 3), b"").unwrap();
        std::fs::write(l.wal_file("products", 1), b"").unwrap();
        std::fs::write(l.wal_dir("products").join("stray.txt"), b"").unwrap();
        assert_eq!(l.list_wal_generations("products").unwrap(), vec![1, 3]);
    }

    #[test]
    fn lists_no_wal_generations_for_an_unflushed_collection() {
        let dir = tempfile::tempdir().unwrap();
        let l = Layout::new(dir.path());
        assert_eq!(l.list_wal_generations("products").unwrap(), Vec::<u64>::new());
    }

    #[test]
    fn lists_only_collections_with_a_schema() {
        let dir = tempfile::tempdir().unwrap();
        let l = Layout::new(dir.path());
        l.initialize().unwrap();
        std::fs::create_dir_all(l.collection_dir("half_created")).unwrap();
        std::fs::create_dir_all(l.collection_dir("products")).unwrap();
        std::fs::write(l.schema_file("products"), "{}").unwrap();
        assert_eq!(l.list_collections().unwrap(), vec!["products".to_string()]);
    }

    #[test]
    fn atomic_write_replaces_contents() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.json");
        write_atomic(&path, b"one").unwrap();
        write_atomic(&path, b"two").unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"two");
        assert!(!path.with_extension("tmp").exists());
    }
}
