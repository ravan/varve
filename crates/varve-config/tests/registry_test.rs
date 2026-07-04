use std::sync::Arc;
use varve_config::registry::{ComponentFactory, Registry, RegistryError};
use varve_config::{Config, ConfigSection};

// Toy subsystem trait standing in for Log/ObjectStore/…
trait Greeter: Send + Sync + std::fmt::Debug {
    fn greet(&self) -> String;
}

#[derive(Debug)]
struct EnglishGreeter {
    name: String,
}
impl Greeter for EnglishGreeter {
    fn greet(&self) -> String {
        format!("hello {}", self.name)
    }
}

struct EnglishFactory;
impl ComponentFactory<dyn Greeter> for EnglishFactory {
    fn name(&self) -> &'static str {
        "english"
    }
    fn build(&self, cfg: &ConfigSection) -> Result<Arc<dyn Greeter>, RegistryError> {
        #[derive(serde::Deserialize)]
        struct C {
            name: String,
        }
        let c: C = cfg.get()?;
        Ok(Arc::new(EnglishGreeter { name: c.name }))
    }
}

#[allow(clippy::unwrap_used)]
fn section(toml: &str, name: &str) -> ConfigSection {
    Config::from_toml_str(toml).unwrap().section(name).unwrap()
}

#[test]
fn builds_registered_component_from_config() {
    let mut reg: Registry<dyn Greeter> = Registry::new("greeter");
    reg.register(Box::new(EnglishFactory)).unwrap();
    let cfg = section("[greeter]\nname = \"ada\"", "greeter");
    let g = reg.build("english", &cfg).unwrap();
    assert_eq!(g.greet(), "hello ada");
}

#[test]
fn unknown_name_error_lists_available() {
    let mut reg: Registry<dyn Greeter> = Registry::new("greeter");
    reg.register(Box::new(EnglishFactory)).unwrap();
    let cfg = section("[greeter]\nname = \"x\"", "greeter");
    let err = reg.build("klingon", &cfg).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("klingon"), "{msg}");
    assert!(msg.contains("english"), "{msg}");
}

#[test]
fn duplicate_registration_rejected() {
    let mut reg: Registry<dyn Greeter> = Registry::new("greeter");
    reg.register(Box::new(EnglishFactory)).unwrap();
    assert!(reg.register(Box::new(EnglishFactory)).is_err());
}
