pub mod local;
pub mod log;
pub mod memory;
#[cfg(feature = "object-store")]
pub mod object_store;
pub mod record;

pub use local::{LocalLog, LocalLogFactory, DEFAULT_SEGMENT_MAX_BYTES};
pub use log::{Log, LogError};
pub use memory::{MemoryLog, MemoryLogFactory};
#[cfg(feature = "object-store")]
pub use object_store::{ObjectStoreLog, ObjectStoreLogFactory};
pub use record::{decode_frames, LogRecord, TableEffects};

use varve_config::{ComponentFactory, Registry};

/// All built-in log backends, registered under kind "log".
pub fn log_registry() -> Registry<dyn Log> {
    let mut reg = Registry::new("log");
    register_builtin(&mut reg, Box::new(MemoryLogFactory));
    register_builtin(&mut reg, Box::new(LocalLogFactory));
    #[cfg(feature = "object-store")]
    register_builtin(&mut reg, Box::new(object_store::ObjectStoreLogFactory));
    reg
}

/// Registers a built-in factory, panicking on a duplicate name. Builtin
/// names are a static, distinct set fixed at compile time — a collision here
/// is a programming error in this crate, not a runtime configuration
/// problem, so it must never be turned into a `Result` the caller has to
/// handle.
fn register_builtin(reg: &mut Registry<dyn Log>, factory: Box<dyn ComponentFactory<dyn Log>>) {
    if let Err(e) = reg.register(factory) {
        unreachable!("built-in log factory registration must not collide: {e}");
    }
}
