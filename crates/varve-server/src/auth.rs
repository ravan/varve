use serde::Deserialize;
use std::{collections::HashSet, fmt, sync::Arc};
use subtle::ConstantTimeEq;
use thiserror::Error;
use varve_config::{BuildContext, ComponentFactory, ConfigSection, RegistryError};

pub trait Authenticator: Send + Sync {
    fn authenticate(&self, bearer: Option<&str>) -> Result<Principal, AuthError>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Principal {
    pub subject: String,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum AuthError {
    #[error("authentication credentials are missing")]
    Missing,
    #[error("authentication credentials are invalid")]
    Invalid,
}

struct StaticAuth {
    tokens: Vec<(String, Vec<u8>)>,
}

impl fmt::Debug for StaticAuth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StaticAuth")
            .field("token_count", &self.tokens.len())
            .finish()
    }
}

impl Authenticator for StaticAuth {
    fn authenticate(&self, bearer: Option<&str>) -> Result<Principal, AuthError> {
        let candidate = bearer.ok_or(AuthError::Missing)?.as_bytes();
        let mut matched = 0u8;
        let mut subject = None;
        for (configured_subject, token) in &self.tokens {
            let equal = if candidate.len() == token.len() {
                candidate.ct_eq(token).unwrap_u8()
            } else {
                0
            };
            matched |= equal;
            if equal == 1 {
                subject = Some(configured_subject.clone());
            }
        }
        if matched == 1 {
            Ok(Principal {
                subject: subject.unwrap_or_default(),
            })
        } else {
            Err(AuthError::Invalid)
        }
    }
}

pub fn static_auth(entries: &[(&str, &str)]) -> Result<Arc<dyn Authenticator>, AuthConfigError> {
    build_entries(
        entries
            .iter()
            .map(|(subject, token)| TokenConfig {
                subject: (*subject).to_string(),
                token: (*token).to_string(),
            })
            .collect(),
    )
}

#[derive(Debug, Error)]
#[error("invalid static authentication configuration: {0}")]
pub struct AuthConfigError(String);

#[derive(Deserialize)]
struct StaticConfig {
    tokens: Vec<TokenConfig>,
}

#[derive(Deserialize)]
struct TokenConfig {
    subject: String,
    token: String,
}

fn build_entries(entries: Vec<TokenConfig>) -> Result<Arc<dyn Authenticator>, AuthConfigError> {
    if entries.is_empty() {
        return Err(AuthConfigError("at least one token is required".into()));
    }
    let mut tokens = HashSet::new();
    for entry in &entries {
        if entry.subject.is_empty() || entry.token.is_empty() {
            return Err(AuthConfigError(
                "subjects and tokens must be non-empty".into(),
            ));
        }
        if !tokens.insert(entry.token.as_str()) {
            return Err(AuthConfigError("tokens must be unique".into()));
        }
    }
    Ok(Arc::new(StaticAuth {
        tokens: entries
            .into_iter()
            .map(|entry| (entry.subject, entry.token.into_bytes()))
            .collect(),
    }))
}

pub(crate) struct StaticAuthFactory;

impl ComponentFactory<dyn Authenticator> for StaticAuthFactory {
    fn name(&self) -> &'static str {
        "static"
    }

    fn build(
        &self,
        cfg: &ConfigSection,
        _ctx: &BuildContext,
    ) -> Result<Arc<dyn Authenticator>, RegistryError> {
        let result = cfg
            .child("static")
            .ok_or_else(|| AuthConfigError("[auth.static] is required".into()))
            .and_then(|section| {
                section
                    .get::<StaticConfig>()
                    .map_err(|error| AuthConfigError(error.to_string()))
            })
            .and_then(|config| build_entries(config.tokens));
        result.map_err(|source| RegistryError::Build {
            kind: "authenticator",
            name: "static".into(),
            source: Box::new(source),
        })
    }
}
