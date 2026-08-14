//! The engine: the set of open collections and their lifecycle.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

use tachyon_core::{CollectionSchema, Error, Result};
use tachyon_storage::{meta, Layout};

use crate::collection::{Collection, CollectionStats};
use crate::config::EngineConfig;

pub struct Engine {
    layout: Layout,
    config: EngineConfig,
    /// Every collection is opened at startup and stays open: a collection's
    /// memtable is its live data, so there is nothing to lazily load.
    collections: RwLock<HashMap<String, Arc<Collection>>>,
}

impl Engine {
    /// Open a data directory, recovering every collection it contains.
    pub fn open(config: EngineConfig) -> Result<Engine> {
        let layout = Layout::new(&config.data_dir);
        layout.initialize()?;

        let mut collections = HashMap::new();
        for name in layout.list_collections()? {
            let collection = Collection::open(&layout, &name, &config)?;
            tracing::info!(
                collection = %name,
                documents = collection.stats().num_documents,
                "opened collection"
            );
            collections.insert(name, Arc::new(collection));
        }

        Ok(Engine { layout, config, collections: RwLock::new(collections) })
    }

    pub fn create_collection(&self, schema: CollectionSchema) -> Result<Arc<Collection>> {
        // Validate before taking the lock so a bad request never blocks writers.
        schema.validate()?;

        let mut collections = self.collections.write();
        if collections.contains_key(&schema.name) {
            return Err(Error::CollectionExists(schema.name.clone()));
        }

        let name = schema.name.clone();
        let collection = Arc::new(Collection::create(&self.layout, schema, &self.config)?);
        collections.insert(name, Arc::clone(&collection));
        Ok(collection)
    }

    pub fn collection(&self, name: &str) -> Result<Arc<Collection>> {
        self.collections
            .read()
            .get(name)
            .cloned()
            .ok_or_else(|| Error::CollectionNotFound(name.to_string()))
    }

    /// Stats for every collection, ordered by name.
    pub fn list_collections(&self) -> Vec<CollectionStats> {
        let collections = self.collections.read();
        let mut stats: Vec<_> = collections.values().map(|c| c.stats()).collect();
        stats.sort_by(|a, b| a.name.cmp(&b.name));
        stats
    }

    pub fn drop_collection(&self, name: &str) -> Result<()> {
        let mut collections = self.collections.write();
        let Some(collection) = collections.remove(name) else {
            return Err(Error::CollectionNotFound(name.to_string()));
        };
        // Release our handle so the WAL file is closed before the directory
        // goes away; other threads holding an Arc keep reading a doomed
        // collection until they drop it, which is harmless.
        drop(collection);
        meta::drop_collection(&self.layout, name)
    }

    /// Force every collection's WAL to durable storage.
    pub fn sync_all(&self) -> Result<()> {
        for collection in self.collections.read().values() {
            collection.sync()?;
        }
        Ok(())
    }

    pub fn config(&self) -> &EngineConfig {
        &self.config
    }

    pub fn data_dir(&self) -> &std::path::Path {
        self.layout.root()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tachyon_core::{FieldSchema, FieldType};

    fn schema(name: &str) -> CollectionSchema {
        CollectionSchema::new(
            name,
            vec![
                FieldSchema::new("title", FieldType::Text).required(),
                FieldSchema::new("price", FieldType::Int).with_sort(true),
            ],
        )
    }

    fn engine(dir: &tempfile::TempDir) -> Engine {
        Engine::open(EngineConfig::new(dir.path())).unwrap()
    }

    #[test]
    fn creates_lists_and_drops() {
        let dir = tempfile::tempdir().unwrap();
        let e = engine(&dir);
        assert!(e.list_collections().is_empty());

        e.create_collection(schema("products")).unwrap();
        e.create_collection(schema("articles")).unwrap();

        let names: Vec<_> = e.list_collections().into_iter().map(|s| s.name).collect();
        assert_eq!(names, vec!["articles", "products"]);

        e.drop_collection("products").unwrap();
        assert_eq!(e.list_collections().len(), 1);
        assert!(matches!(e.collection("products"), Err(Error::CollectionNotFound(_))));
        assert!(matches!(e.drop_collection("products"), Err(Error::CollectionNotFound(_))));
    }

    #[test]
    fn duplicate_creation_conflicts() {
        let dir = tempfile::tempdir().unwrap();
        let e = engine(&dir);
        e.create_collection(schema("products")).unwrap();
        assert!(matches!(e.create_collection(schema("products")), Err(Error::CollectionExists(_))));
    }

    #[test]
    fn invalid_schemas_never_reach_disk() {
        let dir = tempfile::tempdir().unwrap();
        let e = engine(&dir);
        let bad = CollectionSchema::new("products", vec![FieldSchema::new("id", FieldType::Text)]);
        assert!(e.create_collection(bad).is_err());
        assert!(e.list_collections().is_empty());
    }

    #[test]
    fn reopening_recovers_every_collection() {
        let dir = tempfile::tempdir().unwrap();
        {
            let e = engine(&dir);
            let products = e.create_collection(schema("products")).unwrap();
            products.upsert(json!({"id": "1", "title": "Mouse", "price": 10})).unwrap();
            e.create_collection(schema("articles")).unwrap();
        }

        let e = engine(&dir);
        assert_eq!(e.list_collections().len(), 2);
        let products = e.collection("products").unwrap();
        assert_eq!(products.get("1").unwrap()["title"], json!("Mouse"));
    }

    #[test]
    fn a_dropped_collection_stays_dropped() {
        let dir = tempfile::tempdir().unwrap();
        {
            let e = engine(&dir);
            let c = e.create_collection(schema("products")).unwrap();
            c.upsert(json!({"id": "1", "title": "Mouse", "price": 10})).unwrap();
            e.drop_collection("products").unwrap();
        }
        let e = engine(&dir);
        assert!(e.list_collections().is_empty());
        // And the name is free again.
        e.create_collection(schema("products")).unwrap();
        assert!(e.collection("products").unwrap().get("1").is_err());
    }
}
