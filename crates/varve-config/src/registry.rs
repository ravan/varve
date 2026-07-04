use crate::{ConfigError, ConfigSection};
use std::collections::BTreeMap;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RegistryError {
    #[error("{kind} implementation '{name}' is already registered")]
    Duplicate {
        kind: &'static str,
        name: &'static str,
    },
    #[error("unknown {kind} implementation '{name}'; available: [{}]", available.join(", "))]
    Unknown {
        kind: &'static str,
        name: String,
        available: Vec<String>,
    },
    #[error("failed to build {kind} '{name}': {source}")]
    Build {
        kind: &'static str,
        name: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[error(transparent)]
    Config(#[from] ConfigError),
}

pub trait ComponentFactory<T: ?Sized>: Send + Sync {
    fn name(&self) -> &'static str;
    fn build(&self, cfg: &ConfigSection) -> Result<Arc<T>, RegistryError>;
}

pub struct Registry<T: ?Sized> {
    kind: &'static str,
    factories: BTreeMap<&'static str, Box<dyn ComponentFactory<T>>>,
}

impl<T: ?Sized> Registry<T> {
    pub fn new(kind: &'static str) -> Self {
        Registry {
            kind,
            factories: BTreeMap::new(),
        }
    }

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

    pub fn build(&self, name: &str, cfg: &ConfigSection) -> Result<Arc<T>, RegistryError> {
        match self.factories.get(name) {
            Some(f) => f.build(cfg),
            None => Err(RegistryError::Unknown {
                kind: self.kind,
                name: name.to_string(),
                available: self.factories.keys().map(|s| s.to_string()).collect(),
            }),
        }
    }

    pub fn names(&self) -> Vec<&'static str> {
        self.factories.keys().copied().collect()
    }
}
