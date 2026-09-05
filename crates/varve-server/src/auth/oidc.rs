//! `[auth] backend = "oidc"`: bearer JWTs verified against one or more JWKS
//! issuers (Silt-0a). A token is accepted when its header names an allowed
//! algorithm and a `kid`, its unverified `iss` matches a configured issuer,
//! that issuer's JWKS holds the `kid`, and the signature, `exp`, `nbf`, and
//! `aud` verify within the configured clock skew. The subject is the
//! configured claim (`sub` by default); an RFC 8693 `act` claim is carried on
//! the [`Principal`] for logging only.
//!
//! [`Authenticator::authenticate`] is synchronous, so the JWKS fetch is too:
//! the factory fetches every issuer's key set once at build time (startup
//! fails if an issuer is unreachable), and a miss on an unknown `kid`
//! refreshes at most once per [`JWKS_REFRESH_MIN_INTERVAL`]. Each fetch runs
//! on its own OS thread with a private current-thread runtime; inside a
//! multi-thread tokio runtime the wait is `block_in_place`, so a worker is
//! never parked.

use crate::auth::{AuthError, Authenticator, Principal};
use base64::Engine as _;
use jsonwebtoken::{decode, decode_header, jwk::JwkSet, Algorithm, DecodingKey, Validation};
use serde::Deserialize;
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    fmt,
    sync::{Arc, Mutex, RwLock},
    time::{Duration, Instant},
};
use thiserror::Error;
use varve_config::{ComponentFactory, ConfigSection, RegistryError};

/// Signature algorithms a token may name. Symmetric algorithms are refused:
/// a shared secret would let any verifier mint tokens.
pub const ALLOWED_ALGS: &[Algorithm] = &[Algorithm::RS256, Algorithm::ES256, Algorithm::EdDSA];
/// Default `[auth.oidc] clock_skew_secs`.
pub const DEFAULT_CLOCK_SKEW_SECS: u64 = 60;
/// Default `[[auth.oidc.issuers]] subject_claim`.
pub const DEFAULT_SUBJECT_CLAIM: &str = "sub";
/// Minimum gap between two on-miss JWKS refreshes for one issuer.
pub const JWKS_REFRESH_MIN_INTERVAL: Duration = Duration::from_secs(30);
/// Timeout for one JWKS or discovery fetch.
const HTTP_TIMEOUT: Duration = Duration::from_secs(10);

fn default_clock_skew_secs() -> u64 {
    DEFAULT_CLOCK_SKEW_SECS
}

fn default_subject_claim() -> String {
    DEFAULT_SUBJECT_CLAIM.to_string()
}

/// `[auth.oidc]`.
#[derive(Clone, Debug, Deserialize)]
pub struct OidcConfig {
    /// `[[auth.oidc.issuers]]`; at least one. The first exact `iss` match
    /// wins.
    pub issuers: Vec<IssuerConfig>,
    /// Leeway applied to `exp` and `nbf`.
    #[serde(default = "default_clock_skew_secs")]
    pub clock_skew_secs: u64,
}

/// One `[[auth.oidc.issuers]]` entry.
#[derive(Clone, Debug, Deserialize)]
pub struct IssuerConfig {
    /// Exact `iss` claim value.
    pub issuer: String,
    /// JWKS document URL. Absent: read `jwks_uri` from
    /// `<issuer>/.well-known/openid-configuration` at build time.
    #[serde(default)]
    pub jwks_url: Option<String>,
    /// Exact `aud` match (a string claim, or one member of an array claim).
    pub audience: String,
    /// The claim that becomes `Principal.subject`; must be a non-empty string.
    #[serde(default = "default_subject_claim")]
    pub subject_claim: String,
}

#[derive(Debug, Error)]
#[error("invalid oidc authentication configuration: {0}")]
pub struct OidcConfigError(String);

/// Bearer JWT verification against the configured issuers.
pub struct OidcAuth {
    issuers: Vec<Issuer>,
    skew: Duration,
}

impl fmt::Debug for OidcAuth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OidcAuth")
            .field(
                "issuers",
                &self
                    .issuers
                    .iter()
                    .map(|issuer| issuer.cfg.issuer.as_str())
                    .collect::<Vec<_>>(),
            )
            .field("skew", &self.skew)
            .finish()
    }
}

struct Issuer {
    cfg: IssuerConfig,
    jwks: JwksCache,
}

/// One issuer's signing keys by `kid`, refreshed on a miss.
struct JwksCache {
    url: String,
    keys: RwLock<HashMap<String, DecodingKey>>,
    /// When the last on-miss refresh ran. The build-time fetch does not
    /// count, so a rotation right after startup is still picked up.
    last_refresh: Mutex<Option<Instant>>,
    min_refresh: Duration,
}

impl JwksCache {
    fn new(url: String, min_refresh: Duration) -> Self {
        Self {
            url,
            keys: RwLock::new(HashMap::new()),
            last_refresh: Mutex::new(None),
            min_refresh,
        }
    }

    /// Returns the key for `kid`. On a miss, refreshes once (rate-limited to
    /// one refresh per `min_refresh`) and looks again. A second miss is
    /// `AuthError::Invalid`.
    fn key(&self, kid: &str) -> Result<DecodingKey, AuthError> {
        if let Some(key) = self.lookup(kid) {
            return Ok(key);
        }
        if self.take_refresh_slot() {
            if let Err(error) = self.refresh() {
                tracing::warn!(url = %self.url, %error, "jwks refresh failed");
            }
        }
        self.lookup(kid).ok_or(AuthError::Invalid)
    }

    fn lookup(&self, kid: &str) -> Option<DecodingKey> {
        self.keys.read().ok()?.get(kid).cloned()
    }

    /// Claims the refresh slot if the last on-miss refresh is old enough.
    fn take_refresh_slot(&self) -> bool {
        let Ok(mut last) = self.last_refresh.lock() else {
            return false;
        };
        let now = Instant::now();
        match *last {
            Some(at) if now.duration_since(at) < self.min_refresh => false,
            _ => {
                *last = Some(now);
                true
            }
        }
    }

    /// Replaces the key map with the JWKS document at `url`. Keys without a
    /// `kid`, or that `jsonwebtoken` cannot turn into a verifying key, are
    /// skipped.
    fn refresh(&self) -> Result<(), String> {
        let set: JwkSet = serde_json::from_value(fetch_json(&self.url)?)
            .map_err(|error| format!("jwks document is not a JWK set: {error}"))?;
        let mut keys = HashMap::new();
        for jwk in set.keys {
            let Some(kid) = jwk.common.key_id.clone() else {
                continue;
            };
            match DecodingKey::from_jwk(&jwk) {
                Ok(key) => {
                    keys.insert(kid, key);
                }
                Err(error) => tracing::warn!(%kid, %error, "jwks key skipped"),
            }
        }
        *self
            .keys
            .write()
            .map_err(|_| "jwks cache lock poisoned".to_string())? = keys;
        Ok(())
    }
}

impl OidcAuth {
    /// Validates `config` and fetches every issuer's JWKS once. An issuer
    /// that cannot be reached, or whose discovery document lacks
    /// `jwks_uri`, is a build error.
    pub fn new(config: OidcConfig) -> Result<Self, OidcConfigError> {
        Self::with_refresh_interval(config, JWKS_REFRESH_MIN_INTERVAL)
    }

    /// [`Self::new`] with an explicit minimum gap between on-miss refreshes.
    pub fn with_refresh_interval(
        config: OidcConfig,
        min_refresh: Duration,
    ) -> Result<Self, OidcConfigError> {
        if config.issuers.is_empty() {
            return Err(OidcConfigError("at least one issuer is required".into()));
        }
        let mut seen = HashSet::new();
        let mut issuers = Vec::with_capacity(config.issuers.len());
        for cfg in config.issuers {
            if cfg.issuer.is_empty() || cfg.audience.is_empty() || cfg.subject_claim.is_empty() {
                return Err(OidcConfigError(
                    "issuer, audience, and subject_claim must be non-empty".into(),
                ));
            }
            if !seen.insert(cfg.issuer.clone()) {
                return Err(OidcConfigError(format!(
                    "issuer '{}' is configured twice",
                    cfg.issuer
                )));
            }
            let url = match &cfg.jwks_url {
                Some(url) => url.clone(),
                None => discover_jwks_url(&cfg.issuer)?,
            };
            let jwks = JwksCache::new(url, min_refresh);
            jwks.refresh().map_err(|error| {
                OidcConfigError(format!(
                    "issuer '{}': jwks fetch from {} failed: {error}",
                    cfg.issuer, jwks.url
                ))
            })?;
            issuers.push(Issuer { cfg, jwks });
        }
        Ok(Self {
            issuers,
            skew: Duration::from_secs(config.clock_skew_secs),
        })
    }
}

/// Reads `jwks_uri` from the issuer's OpenID discovery document.
fn discover_jwks_url(issuer: &str) -> Result<String, OidcConfigError> {
    let url = format!(
        "{}/.well-known/openid-configuration",
        issuer.trim_end_matches('/')
    );
    let document = fetch_json(&url).map_err(|error| {
        OidcConfigError(format!("issuer '{issuer}': discovery failed: {error}"))
    })?;
    document
        .get("jwks_uri")
        .and_then(Value::as_str)
        .filter(|uri| !uri.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| {
            OidcConfigError(format!(
                "issuer '{issuer}': discovery document has no jwks_uri"
            ))
        })
}

/// GETs `url` and parses the body as JSON, from a synchronous caller.
fn fetch_json(url: &str) -> Result<Value, String> {
    let url = url.to_string();
    blocking(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| error.to_string())?;
        runtime.block_on(async {
            let client = reqwest::Client::builder()
                .timeout(HTTP_TIMEOUT)
                .build()
                .map_err(|error| error.to_string())?;
            let response = client
                .get(&url)
                .send()
                .await
                .and_then(reqwest::Response::error_for_status)
                .map_err(|error| error.to_string())?;
            response
                .json::<Value>()
                .await
                .map_err(|error| error.to_string())
        })
    })?
}

/// Runs `f` on a fresh OS thread and waits for it. Inside a multi-thread
/// tokio runtime the wait is `block_in_place`, so the worker is released;
/// anywhere else (a current-thread runtime, or no runtime) the calling
/// thread simply waits.
fn blocking<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> Result<T, String> {
    let wait = move || {
        std::thread::spawn(f)
            .join()
            .map_err(|_| "background fetch thread panicked".to_string())
    };
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(wait)
        }
        _ => wait(),
    }
}

/// The `iss` claim before any verification, used only to pick the issuer
/// whose key set is consulted. Nothing is trusted from it.
fn unverified_issuer(token: &str) -> Result<String, AuthError> {
    let payload = token.split('.').nth(1).ok_or(AuthError::Invalid)?;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .map_err(|_| AuthError::Invalid)?;
    let claims: Value = serde_json::from_slice(&bytes).map_err(|_| AuthError::Invalid)?;
    claims
        .get("iss")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or(AuthError::Invalid)
}

impl Authenticator for OidcAuth {
    /// Order: parse header (alg in [`ALLOWED_ALGS`], `kid` present) → pick
    /// issuer by unverified `iss` → key by `kid` → verify signature, `exp`,
    /// `nbf`, `aud` with skew → subject = claims[`subject_claim`] as a
    /// non-empty string → act = claims["act"]["sub"] or claims["act"] as a
    /// string, when present.
    fn authenticate(&self, bearer: Option<&str>) -> Result<Principal, AuthError> {
        let token = bearer.ok_or(AuthError::Missing)?;
        let header = decode_header(token).map_err(|_| AuthError::Invalid)?;
        if !ALLOWED_ALGS.contains(&header.alg) {
            return Err(AuthError::Invalid);
        }
        let kid = header.kid.ok_or(AuthError::Invalid)?;
        let iss = unverified_issuer(token)?;
        let issuer = self
            .issuers
            .iter()
            .find(|issuer| issuer.cfg.issuer == iss)
            .ok_or(AuthError::Invalid)?;
        let key = issuer.jwks.key(&kid)?;

        let mut validation = Validation::new(header.alg);
        validation.set_issuer(&[issuer.cfg.issuer.as_str()]);
        validation.set_audience(&[issuer.cfg.audience.as_str()]);
        validation.leeway = self.skew.as_secs();
        validation.validate_exp = true;
        validation.validate_nbf = true;
        let claims = decode::<serde_json::Map<String, Value>>(token, &key, &validation)
            .map_err(|_| AuthError::Invalid)?
            .claims;

        let subject = claims
            .get(&issuer.cfg.subject_claim)
            .and_then(Value::as_str)
            .filter(|subject| !subject.is_empty())
            .ok_or(AuthError::Invalid)?
            .to_string();
        let act = claims.get("act").and_then(|act| match act {
            Value::String(actor) => Some(actor.clone()),
            Value::Object(actor) => actor.get("sub").and_then(Value::as_str).map(str::to_owned),
            _ => None,
        });
        Ok(Principal { subject, act })
    }
}

/// `[auth] backend = "oidc"`; reads `[auth.oidc]`.
pub(crate) struct OidcAuthFactory;

impl ComponentFactory<dyn Authenticator> for OidcAuthFactory {
    fn name(&self) -> &'static str {
        "oidc"
    }

    fn build(
        &self,
        cfg: &ConfigSection,
        _ctx: &(),
    ) -> Result<Arc<dyn Authenticator>, RegistryError> {
        let result = cfg
            .child("oidc")?
            .ok_or_else(|| OidcConfigError("[auth.oidc] is required".into()))
            .and_then(|section| {
                section
                    .get::<OidcConfig>()
                    .map_err(|error| OidcConfigError(error.to_string()))
            })
            .and_then(OidcAuth::new)
            .map(|auth| Arc::new(auth) as Arc<dyn Authenticator>);
        result.map_err(|source| RegistryError::Build {
            kind: "authenticator",
            name: "oidc".into(),
            source: Box::new(source),
        })
    }
}
