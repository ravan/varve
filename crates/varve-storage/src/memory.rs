use crate::store::ObjectStore;
use std::sync::Arc;
use varve_config::{ComponentFactory, ConfigSection, RegistryError};

/// Volatile in-process store (tests, `Db::memory()`). Contents live only for
/// the process lifetime — restart loses everything.
pub fn memory_store() -> Arc<dyn ObjectStore> {
    Arc::new(object_store::memory::InMemory::new())
}

/// Registry factory: `[storage] backend = "memory"`.
pub struct MemoryStoreFactory;

impl ComponentFactory<dyn ObjectStore> for MemoryStoreFactory {
    fn name(&self) -> &'static str {
        "memory"
    }

    fn build(&self, _cfg: &ConfigSection) -> Result<Arc<dyn ObjectStore>, RegistryError> {
        Ok(memory_store())
    }
}
