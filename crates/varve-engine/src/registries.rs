use crate::clock::{Clock, SystemClockFactory};
#[cfg(feature = "cas-failover")]
use crate::coord::cas::CasFailoverFactory;
use crate::coord::designated::DesignatedWriterFactory;
use crate::coord::Coordinator;
use varve_config::Registry;
use varve_log::Log;
use varve_storage::{CacheTier, ObjectStore};

/// Per-subsystem component registries (spec §4). `with_builtins()` wires up
/// everything compiled in; embedding applications may `register` additional
/// factories before calling `Db::open_with`.
pub struct Registries {
    pub log: Registry<dyn Log, varve_log::LogDependencies>,
    pub clock: Registry<dyn Clock>,
    pub storage: Registry<dyn ObjectStore>,
    pub cache: Registry<dyn CacheTier>,
    pub coordinator: Registry<dyn Coordinator, CoordinatorDependencies>,
}

impl Registries {
    pub fn with_builtins() -> Registries {
        let mut clock = Registry::new("clock");
        // Builtin names are a static, distinct set — duplicates are bugs.
        if let Err(e) = clock.register(Box::new(SystemClockFactory)) {
            unreachable!("duplicate builtin clock factory: {e}");
        }
        let mut coordinator = Registry::new("coordinator");
        if let Err(e) = coordinator.register(Box::new(DesignatedWriterFactory)) {
            unreachable!("duplicate builtin coordinator factory: {e}");
        }
        #[cfg(feature = "cas-failover")]
        if let Err(e) = coordinator.register(Box::new(CasFailoverFactory)) {
            unreachable!("duplicate builtin coordinator factory: {e}");
        }
        Registries {
            log: varve_log::log_registry(),
            clock,
            storage: varve_storage::storage_registry(),
            cache: varve_storage::cache_registry(),
            coordinator,
        }
    }
}

/// Required inputs for coordinator factories, assembled before acquisition.
pub struct CoordinatorDependencies {
    pub store: std::sync::Arc<dyn ObjectStore>,
    pub clock: std::sync::Arc<dyn Clock>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use varve_config::ConfigSection;

    #[test]
    fn builtins_cover_log_and_clock() {
        let registries = Registries::with_builtins();
        assert_eq!(
            registries.log.names(),
            vec!["local", "memory", "object-store"]
        );
        assert_eq!(registries.clock.names(), vec!["system"]);
        assert_eq!(registries.storage.names(), vec!["local", "memory", "s3"]);
        assert_eq!(registries.cache.names(), vec!["disk", "memory"]);
        #[cfg(feature = "cas-failover")]
        assert_eq!(
            registries.coordinator.names(),
            vec!["cas-failover", "designated-writer"]
        );
        #[cfg(not(feature = "cas-failover"))]
        assert_eq!(registries.coordinator.names(), vec!["designated-writer"]);
    }

    #[test]
    fn builds_by_name_from_empty_sections() {
        let registries = Registries::with_builtins();
        let _log = registries
            .log
            .build(
                "memory",
                &ConfigSection::empty(),
                &varve_log::LogDependencies {
                    store: varve_storage::memory_store(),
                },
            )
            .unwrap();
        let clock = registries
            .clock
            .build("system", &ConfigSection::empty(), &())
            .unwrap();
        assert!(clock.next().as_micros() > 0);
    }
}
