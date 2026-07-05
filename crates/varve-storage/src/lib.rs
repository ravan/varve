pub mod keys;
pub mod local;
pub mod memory;
pub mod store;

pub use local::{local_store, LocalStoreFactory};
pub use memory::{memory_store, MemoryStoreFactory};
pub use store::{ObjectStore, StorageError};

use varve_config::{ComponentFactory, Registry};

/// All built-in storage backends, registered under kind "storage".
pub fn storage_registry() -> Registry<dyn ObjectStore> {
    let mut reg = Registry::new("storage");
    register_builtin(&mut reg, Box::new(MemoryStoreFactory));
    register_builtin(&mut reg, Box::new(LocalStoreFactory));
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
