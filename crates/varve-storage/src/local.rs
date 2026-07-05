use crate::store::{ObjectStore, StorageError};
use std::path::Path;
use std::sync::Arc;
use varve_config::{BuildContext, ComponentFactory, ConfigSection, RegistryError};

/// Local-filesystem store, backed by `object_store::local::LocalFileSystem`.
/// Creates `dir` (and any missing parents) if it does not already exist;
/// writes go through the backend's own temp-file-then-rename path, so `put`
/// stays atomic per the `ObjectStore` contract.
pub fn local_store(dir: &Path) -> Result<Arc<dyn ObjectStore>, StorageError> {
    std::fs::create_dir_all(dir)?;
    let fs = object_store::local::LocalFileSystem::new_with_prefix(dir)
        .map_err(StorageError::Backend)?;
    Ok(Arc::new(fs))
}

#[derive(serde::Deserialize)]
struct LocalStoreConfig {
    dir: String,
}

/// Registry factory: `[storage] backend = "local"`, configured via a nested
/// `[storage.local]` table (`dir` required).
pub struct LocalStoreFactory;

impl ComponentFactory<dyn ObjectStore> for LocalStoreFactory {
    fn name(&self) -> &'static str {
        "local"
    }

    fn build(
        &self,
        cfg: &ConfigSection,
        _ctx: &BuildContext,
    ) -> Result<Arc<dyn ObjectStore>, RegistryError> {
        let local = cfg.child("local").ok_or_else(|| RegistryError::Build {
            kind: "storage",
            name: "local".into(),
            source: "missing [storage.local] section (requires `dir`)"
                .to_string()
                .into(),
        })?;
        let config: LocalStoreConfig = local.get()?;
        local_store(Path::new(&config.dir)).map_err(|e| RegistryError::Build {
            kind: "storage",
            name: "local".into(),
            source: Box::new(e),
        })
    }
}
