pub mod cache;
pub mod disk;
pub mod keys;
pub mod local;
pub mod manifest;
pub mod memory;
pub mod probe;
#[cfg(feature = "s3")]
pub mod s3;
pub mod store;

pub use cache::{CacheKey, CacheTier, CachedStore, MemoryCache, MemoryCacheFactory};
pub use disk::{DiskCache, DiskCacheFactory};
pub use local::{local_store, LocalStoreFactory};
pub use manifest::{latest_manifest, BlockManifest, TableTries, TrieEntry};
pub use memory::{memory_store, MemoryStoreFactory};
pub use probe::{probe_conditional_put, ProbeReport, ProbeVerdict, PROBE_PREFIX};
#[cfg(feature = "s3")]
pub use s3::S3StoreFactory;
pub use store::{CondPut, ConditionalStore, ObjectStore, StorageError};

use varve_config::{ComponentFactory, Registry};

/// All built-in storage backends, registered under kind "storage".
pub fn storage_registry() -> Registry<dyn ObjectStore> {
    let mut reg = Registry::new("storage");
    register_builtin(&mut reg, Box::new(MemoryStoreFactory));
    register_builtin(&mut reg, Box::new(LocalStoreFactory));
    #[cfg(feature = "s3")]
    register_builtin(&mut reg, Box::new(s3::S3StoreFactory));
    reg
}

/// Registers a built-in factory, panicking on a duplicate name. Builtin
/// names are a static, distinct set fixed at compile time — a collision here
/// is a programming error in this crate, never a runtime configuration
/// problem (same rationale as `varve_log::log_registry`).
fn register_builtin(
    reg: &mut Registry<dyn ObjectStore>,
    factory: Box<dyn ComponentFactory<dyn ObjectStore>>,
) {
    if let Err(e) = reg.register(factory) {
        unreachable!("built-in storage factory registration must not collide: {e}");
    }
}

/// All built-in cache tiers, registered under kind "cache".
pub fn cache_registry() -> Registry<dyn CacheTier> {
    let mut reg = Registry::new("cache");
    register_cache_builtin(&mut reg, Box::new(MemoryCacheFactory));
    register_cache_builtin(&mut reg, Box::new(DiskCacheFactory));
    reg
}

/// Same rationale as `register_builtin`: builtin names are a static,
/// distinct set — a collision is a programming error in this crate.
fn register_cache_builtin(
    reg: &mut Registry<dyn CacheTier>,
    factory: Box<dyn ComponentFactory<dyn CacheTier>>,
) {
    if let Err(e) = reg.register(factory) {
        unreachable!("built-in cache factory registration must not collide: {e}");
    }
}
