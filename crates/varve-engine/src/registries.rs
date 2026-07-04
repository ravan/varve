use crate::clock::{Clock, SystemClockFactory};
use varve_config::Registry;
use varve_log::Log;

/// Per-subsystem component registries (spec §4). `with_builtins()` wires up
/// everything compiled in; embedding applications may `register` additional
/// factories before calling `Db::open_with`.
pub struct Registries {
    pub log: Registry<dyn Log>,
    pub clock: Registry<dyn Clock>,
}

impl Registries {
    pub fn with_builtins() -> Registries {
        let mut clock = Registry::new("clock");
        // Builtin names are a static, distinct set — duplicates are bugs.
        if let Err(e) = clock.register(Box::new(SystemClockFactory)) {
            unreachable!("duplicate builtin clock factory: {e}");
        }
        Registries {
            log: varve_log::log_registry(),
            clock,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use varve_config::ConfigSection;

    #[test]
    fn builtins_cover_log_and_clock() {
        let registries = Registries::with_builtins();
        assert_eq!(registries.log.names(), vec!["local", "memory"]);
        assert_eq!(registries.clock.names(), vec!["system"]);
    }

    #[test]
    fn builds_by_name_from_empty_sections() {
        let registries = Registries::with_builtins();
        let _log = registries
            .log
            .build("memory", &ConfigSection::empty())
            .unwrap();
        let clock = registries
            .clock
            .build("system", &ConfigSection::empty())
            .unwrap();
        assert!(clock.next().as_micros() > 0);
    }
}
