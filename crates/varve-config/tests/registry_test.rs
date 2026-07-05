use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use varve_config::registry::{ComponentFactory, Registry, RegistryError};
use varve_config::{BuildContext, Config, ConfigSection};

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
    fn build(
        &self,
        cfg: &ConfigSection,
        _ctx: &BuildContext,
    ) -> Result<Arc<dyn Greeter>, RegistryError> {
        #[derive(serde::Deserialize)]
        struct C {
            name: String,
        }
        let c: C = cfg.get()?;
        Ok(Arc::new(EnglishGreeter { name: c.name }))
    }
}

#[derive(Debug)]
struct CountedGreeter {
    count: Arc<AtomicU32>,
}
impl Greeter for CountedGreeter {
    fn greet(&self) -> String {
        format!(
            "greeting #{}",
            self.count.fetch_add(1, Ordering::SeqCst) + 1
        )
    }
}

struct CountedFactory;
impl ComponentFactory<dyn Greeter> for CountedFactory {
    fn name(&self) -> &'static str {
        "counted"
    }
    fn build(
        &self,
        _cfg: &ConfigSection,
        ctx: &BuildContext,
    ) -> Result<Arc<dyn Greeter>, RegistryError> {
        let count = ctx
            .get::<Arc<AtomicU32>>()
            .ok_or_else(|| RegistryError::Build {
                kind: "greeter",
                name: "counted".into(),
                source: "requires counter component in BuildContext"
                    .to_string()
                    .into(),
            })?;
        Ok(Arc::new(CountedGreeter { count }))
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
    let g = reg.build("english", &cfg, &BuildContext::empty()).unwrap();
    assert_eq!(g.greet(), "hello ada");
}

#[test]
fn unknown_name_error_lists_available() {
    let mut reg: Registry<dyn Greeter> = Registry::new("greeter");
    reg.register(Box::new(EnglishFactory)).unwrap();
    let cfg = section("[greeter]\nname = \"x\"", "greeter");
    let err = reg
        .build("klingon", &cfg, &BuildContext::empty())
        .unwrap_err();
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

#[test]
fn factories_can_consume_context_components() {
    let mut reg: Registry<dyn Greeter> = Registry::new("greeter");
    reg.register(Box::new(CountedFactory)).unwrap();
    let counter = Arc::new(AtomicU32::new(0));
    let mut ctx = BuildContext::empty();
    ctx.insert(Arc::clone(&counter));
    let g = reg.build("counted", &ConfigSection::empty(), &ctx).unwrap();
    assert_eq!(g.greet(), "greeting #1");
    assert_eq!(counter.load(Ordering::SeqCst), 1, "shares ctx Arc");
}

#[test]
fn missing_context_component_is_a_build_error() {
    let mut reg: Registry<dyn Greeter> = Registry::new("greeter");
    reg.register(Box::new(CountedFactory)).unwrap();
    let err = reg
        .build("counted", &ConfigSection::empty(), &BuildContext::empty())
        .unwrap_err();
    assert!(err.to_string().contains("counter component"), "{err}");
}

#[test]
fn context_get_is_typed() {
    let mut ctx = BuildContext::empty();
    ctx.insert(7u32);
    assert_eq!(ctx.get::<u32>(), Some(7));
    assert_eq!(ctx.get::<u64>(), None, "different type, different slot");
}
