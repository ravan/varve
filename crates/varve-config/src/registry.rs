use crate::{ConfigError, ConfigSection};
use std::any::{Any, TypeId};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use thiserror::Error;

/// Failure modes for registering or building components by name.
#[derive(Debug, Error)]
pub enum RegistryError {
    /// [`Registry::register`] was called twice with a factory of the same
    /// `name()` and `kind` (e.g. two `"local"` log factories) — builtin
    /// names are a fixed, distinct set, so this is a programming error.
    #[error("{kind} implementation '{name}' is already registered")]
    Duplicate {
        kind: &'static str,
        name: &'static str,
    },
    /// [`Registry::build`] was asked for a `name` with no registered
    /// factory; `available` lists every name that IS registered (e.g. a
    /// typo'd `[log] backend = "kafka"` reports `available: [local, memory]`).
    #[error("unknown {kind} implementation '{name}'; available: [{}]", available.join(", "))]
    Unknown {
        kind: &'static str,
        name: String,
        available: Vec<String>,
    },
    /// The named factory matched, but its own `build` failed (e.g. the
    /// `local` log factory couldn't open its directory).
    #[error("failed to build {kind} '{name}': {source}")]
    Build {
        kind: &'static str,
        name: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    /// A factory's `build` needed to deserialize its config section and that
    /// failed — wraps [`ConfigError`].
    #[error(transparent)]
    Config(#[from] ConfigError),
}

/// Already-built components later factories may depend on — spec §4's
/// `ctx` parameter. Typed lookup: components keyed by FULL type
/// (e.g. `Arc<dyn ObjectStore>`), and `get` clones the stored value out, so
/// components cheap-to-clone handles (`Arc`s) by convention.
///
/// engine populates in dependency order (storage first), so
/// factory only see components built before own subsystem — if a
/// factory needs something absent it fails with actionable
/// [`RegistryError::Build`], never panic.
#[derive(Default)]
pub struct BuildContext {
    components: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
}

impl BuildContext {
    /// No components — common case for config-only factories and tests.
    pub fn empty() -> BuildContext {
        BuildContext::default()
    }

    /// Stores `component` under its type; second insert of same type
    /// replaces first.
    pub fn insert<C: Clone + Send + Sync + 'static>(&mut self, component: C) {
        self.components
            .insert(TypeId::of::<C>(), Box::new(component));
    }

    /// Clones component of type `C` out, if one inserted.
    pub fn get<C: Clone + Send + Sync + 'static>(&self) -> Option<C> {
        self.components
            .get(&TypeId::of::<C>())
            .and_then(|b| b.downcast_ref::<C>())
            .cloned()
    }
}

/// One named, pluggable implementation of `T` (spec §4 extension point:
/// `Log`, `Clock`, and future backends are all built this way). `name()` is
/// the exact string a `[section] backend = "..."` key selects; `build` reads
/// only its own config section, e.g. the `local` log factory's `build` calls
/// `cfg.child("local")` to reach `[log.local]`.
pub trait ComponentFactory<T: ?Sized>: Send + Sync {
    /// The registry key this factory answers to (matched against a
    /// `backend = "..."` config value).
    fn name(&self) -> &'static str;
    /// Builds one instance of `T` from `cfg` — the section the instance was
    /// selected under, e.g. `[log]` for a log factory (not a pre-narrowed
    /// child section; a factory reaches into its own nested table itself).
    /// `ctx` carries already-built components the factory may consume
    /// (spec §4) or ignores for config-only builds.
    fn build(&self, cfg: &ConfigSection, ctx: &BuildContext) -> Result<Arc<T>, RegistryError>;
}

/// A named lookup table of [`ComponentFactory`]s for one component kind
/// (e.g. `"log"`, `"clock"`). Built once at startup (builtins registered,
/// embedders may add more), then [`Registry::build`] turns a config-selected
/// name into a live `Arc<T>`.
pub struct Registry<T: ?Sized> {
    kind: &'static str,
    factories: BTreeMap<&'static str, Box<dyn ComponentFactory<T>>>,
}

impl<T: ?Sized> Registry<T> {
    /// Creates an empty registry; `kind` labels it in error messages (e.g.
    /// `"log"`, `"clock"`).
    pub fn new(kind: &'static str) -> Self {
        Registry {
            kind,
            factories: BTreeMap::new(),
        }
    }

    /// Adds `f` under its own `name()`. Errors with
    /// [`RegistryError::Duplicate`] if that name is already registered.
    pub fn register(&mut self, f: Box<dyn ComponentFactory<T>>) -> Result<(), RegistryError> {
        let name = f.name();
        if self.factories.contains_key(name) {
            return Err(RegistryError::Duplicate {
                kind: self.kind,
                name,
            });
        }
        self.factories.insert(name, f);
        Ok(())
    }

    /// Looks up `name` and builds it from `cfg` (spec §4: this is the
    /// config → live-component step, e.g. `log.build("local", &log_section, &ctx)`
    /// for `[log] backend = "local"`). Errors with
    /// [`RegistryError::Unknown`] if no factory answers to `name`.
    pub fn build(
        &self,
        name: &str,
        cfg: &ConfigSection,
        ctx: &BuildContext,
    ) -> Result<Arc<T>, RegistryError> {
        match self.factories.get(name) {
            Some(f) => f.build(cfg, ctx),
            None => Err(RegistryError::Unknown {
                kind: self.kind,
                name: name.to_string(),
                available: self.factories.keys().map(|s| s.to_string()).collect(),
            }),
        }
    }

    /// Every registered name, for diagnostics and tests (e.g. asserting the
    /// builtin log registry covers exactly `["local", "memory"]`).
    pub fn names(&self) -> Vec<&'static str> {
        self.factories.keys().copied().collect()
    }
}
