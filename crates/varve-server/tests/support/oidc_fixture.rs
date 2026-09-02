//! In-process OIDC issuer for tests: serves a swappable JWK set at `/jwks`
//! and an OpenID discovery document that points at it, and signs test tokens
//! with the committed Ed25519 fixture keys (`tests/fixtures/oidc/`).
#![allow(dead_code, clippy::unwrap_used, clippy::expect_used)]

use axum::{extract::State, routing::get, Json, Router};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use serde_json::{json, Value};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

pub const FIXTURES: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/oidc");
pub const AUDIENCE: &str = "varve";

/// A running fixture issuer. `base` is both the `iss` value and the URL root.
pub struct FixtureIssuer {
    pub base: String,
    served: Arc<RwLock<Value>>,
    _task: tokio::task::JoinHandle<()>,
}

impl FixtureIssuer {
    /// Starts the issuer on a loopback port inside the current tokio runtime,
    /// serving `jwks` (a fixture file name, e.g. `"jwks.json"`).
    pub async fn start(jwks: &str) -> FixtureIssuer {
        let served = Arc::new(RwLock::new(load_json(jwks)));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let discovery = json!({ "issuer": base, "jwks_uri": format!("{base}/jwks") });
        let app = Router::new()
            .route(
                "/jwks",
                get(|State(served): State<Arc<RwLock<Value>>>| async move {
                    Json(served.read().unwrap().clone())
                }),
            )
            .route(
                "/.well-known/openid-configuration",
                get(move || async move { Json(discovery) }),
            )
            .with_state(Arc::clone(&served));
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        FixtureIssuer {
            base,
            served,
            _task: task,
        }
    }

    pub fn jwks_url(&self) -> String {
        format!("{}/jwks", self.base)
    }

    /// Replaces the served JWK set with another fixture file (key rotation).
    pub fn serve(&self, jwks: &str) {
        *self.served.write().unwrap() = load_json(jwks);
    }

    /// Standard claims for this issuer: `sub: "ada"`, `exp` in five minutes.
    pub fn claims(&self) -> Value {
        json!({
            "iss": self.base,
            "aud": AUDIENCE,
            "sub": "ada",
            "iat": now_secs(),
            "exp": now_secs() + 300,
        })
    }

    /// One `[[auth.oidc.issuers]]` TOML entry pointing at this issuer.
    pub fn toml_issuer(&self) -> String {
        format!(
            "[[auth.oidc.issuers]]\nissuer = \"{}\"\njwks_url = \"{}\"\naudience = \"{AUDIENCE}\"\n",
            self.base,
            self.jwks_url()
        )
    }
}

pub fn load_json(name: &str) -> Value {
    serde_json::from_slice(&std::fs::read(format!("{FIXTURES}/{name}")).unwrap()).unwrap()
}

pub fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// Signs `claims` as an EdDSA JWT with the fixture key `pem` under `kid`.
pub fn sign(kid: &str, pem: &str, claims: &Value) -> String {
    let key =
        EncodingKey::from_ed_pem(&std::fs::read(format!("{FIXTURES}/{pem}")).unwrap()).unwrap();
    let mut header = Header::new(Algorithm::EdDSA);
    header.kid = Some(kid.to_string());
    encode(&header, claims, &key).unwrap()
}

/// A token signed with the primary fixture key (`kid: "test-1"`).
pub fn sign_primary(claims: &Value) -> String {
    sign("test-1", "ed25519.pem", claims)
}
