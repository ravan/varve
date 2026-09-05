//! Silt-0a task 8: `[auth] backend = "oidc"` verifies bearer JWTs against a
//! JWKS issuer. The matrix runs against an in-process issuer
//! (`support/oidc_fixture.rs`) and the committed Ed25519 fixture keys.
#![cfg(feature = "oidc")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use serde_json::json;
use std::time::Duration;
use varve_config::Config;
use varve_server::auth::oidc::{IssuerConfig, OidcAuth, OidcConfig, DEFAULT_CLOCK_SKEW_SECS};
use varve_server::{AuthError, Authenticator, ServerRegistries};

#[path = "support/oidc_fixture.rs"]
mod oidc_fixture;
use oidc_fixture::{sign, sign_primary, FixtureIssuer, AUDIENCE};

fn issuer_config(issuer: &FixtureIssuer, jwks_url: Option<String>) -> IssuerConfig {
    IssuerConfig {
        issuer: issuer.base.clone(),
        jwks_url,
        audience: AUDIENCE.to_string(),
        subject_claim: "sub".to_string(),
    }
}

fn config(issuer: &FixtureIssuer) -> OidcConfig {
    OidcConfig {
        issuers: vec![issuer_config(issuer, Some(issuer.jwks_url()))],
        clock_skew_secs: DEFAULT_CLOCK_SKEW_SECS,
    }
}

fn auth(issuer: &FixtureIssuer) -> OidcAuth {
    OidcAuth::new(config(issuer)).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn valid_token_yields_the_subject_and_the_act_claim() {
    let issuer = FixtureIssuer::start("jwks.json").await;
    let auth = auth(&issuer);

    let principal = auth
        .authenticate(Some(&sign_primary(&issuer.claims())))
        .unwrap();
    assert_eq!(principal.subject, "ada");
    assert_eq!(principal.act, None);

    let mut claims = issuer.claims();
    claims["act"] = json!({ "sub": "siltd" });
    let principal = auth.authenticate(Some(&sign_primary(&claims))).unwrap();
    assert_eq!(principal.act.as_deref(), Some("siltd"));

    let mut claims = issuer.claims();
    claims["act"] = json!("siltd");
    let principal = auth.authenticate(Some(&sign_primary(&claims))).unwrap();
    assert_eq!(principal.act.as_deref(), Some("siltd"));

    // `aud` may be an array holding the configured audience.
    let mut claims = issuer.claims();
    claims["aud"] = json!(["other", AUDIENCE]);
    assert_eq!(
        auth.authenticate(Some(&sign_primary(&claims)))
            .unwrap()
            .subject,
        "ada"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn negative_matrix_is_invalid() {
    let issuer = FixtureIssuer::start("jwks.json").await;
    let auth = auth(&issuer);

    assert!(matches!(auth.authenticate(None), Err(AuthError::Missing)));

    let mut wrong_aud = issuer.claims();
    wrong_aud["aud"] = json!("someone-else");
    let mut wrong_iss = issuer.claims();
    wrong_iss["iss"] = json!("https://other.example");
    let mut expired = issuer.claims();
    expired["exp"] = json!(oidc_fixture::now_secs() - 2 * DEFAULT_CLOCK_SKEW_SECS);
    let mut not_yet = issuer.claims();
    not_yet["nbf"] = json!(oidc_fixture::now_secs() + 2 * DEFAULT_CLOCK_SKEW_SECS);
    let mut empty_sub = issuer.claims();
    empty_sub["sub"] = json!("");
    let mut no_exp = issuer.claims();
    no_exp.as_object_mut().unwrap().remove("exp");

    for (name, claims) in [
        ("wrong aud", wrong_aud),
        ("wrong iss", wrong_iss),
        ("expired beyond skew", expired),
        ("nbf beyond skew", not_yet),
        ("empty sub", empty_sub),
        ("no exp", no_exp),
    ] {
        assert!(
            matches!(
                auth.authenticate(Some(&sign_primary(&claims))),
                Err(AuthError::Invalid)
            ),
            "{name}"
        );
    }

    // Symmetric algorithm: refused by the header check.
    let mut header = Header::new(Algorithm::HS256);
    header.kid = Some("test-1".into());
    let hs256 = encode(
        &header,
        &issuer.claims(),
        &EncodingKey::from_secret(b"shared-secret"),
    )
    .unwrap();
    assert!(matches!(
        auth.authenticate(Some(&hs256)),
        Err(AuthError::Invalid)
    ));

    // Missing kid.
    let no_kid = encode(
        &Header::new(Algorithm::EdDSA),
        &issuer.claims(),
        &EncodingKey::from_ed_pem(
            &std::fs::read(format!("{}/ed25519.pem", oidc_fixture::FIXTURES)).unwrap(),
        )
        .unwrap(),
    )
    .unwrap();
    assert!(matches!(
        auth.authenticate(Some(&no_kid)),
        Err(AuthError::Invalid)
    ));

    // Unknown kid: one refresh, still unknown.
    let unknown_kid = sign("test-2", "ed25519-2.pem", &issuer.claims());
    assert!(matches!(
        auth.authenticate(Some(&unknown_kid)),
        Err(AuthError::Invalid)
    ));

    // Tampered signature.
    let good = sign_primary(&issuer.claims());
    let (head, signature) = good.rsplit_once('.').unwrap();
    let mut flipped = signature.to_string();
    let first = flipped.remove(0);
    flipped.insert(0, if first == 'A' { 'B' } else { 'A' });
    assert!(matches!(
        auth.authenticate(Some(&format!("{head}.{flipped}"))),
        Err(AuthError::Invalid)
    ));

    // Not a JWT at all.
    assert!(matches!(
        auth.authenticate(Some("static-token")),
        Err(AuthError::Invalid)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn clock_skew_accepts_a_recently_expired_token() {
    let issuer = FixtureIssuer::start("jwks.json").await;
    let auth = auth(&issuer);
    let mut claims = issuer.claims();
    claims["exp"] = json!(oidc_fixture::now_secs() - 30);
    assert_eq!(
        auth.authenticate(Some(&sign_primary(&claims)))
            .unwrap()
            .subject,
        "ada"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subject_claim_selects_another_claim() {
    let issuer = FixtureIssuer::start("jwks.json").await;
    let mut config = config(&issuer);
    config.issuers[0].subject_claim = "email".into();
    let auth = OidcAuth::new(config).unwrap();

    let mut claims = issuer.claims();
    claims["email"] = json!("ada@example.org");
    assert_eq!(
        auth.authenticate(Some(&sign_primary(&claims)))
            .unwrap()
            .subject,
        "ada@example.org"
    );
    assert!(matches!(
        auth.authenticate(Some(&sign_primary(&issuer.claims()))),
        Err(AuthError::Invalid)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn discovery_finds_the_jwks_url_when_none_is_configured() {
    let issuer = FixtureIssuer::start("jwks.json").await;
    let auth = OidcAuth::new(OidcConfig {
        issuers: vec![issuer_config(&issuer, None)],
        clock_skew_secs: DEFAULT_CLOCK_SKEW_SECS,
    })
    .unwrap();
    assert_eq!(
        auth.authenticate(Some(&sign_primary(&issuer.claims())))
            .unwrap()
            .subject,
        "ada"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rotation_refreshes_on_an_unknown_kid_once_per_interval() {
    let issuer = FixtureIssuer::start("jwks.json").await;
    let auth = auth(&issuer);
    let rotated = sign("test-2", "ed25519-2.pem", &issuer.claims());

    // The key set now holds test-2; the first miss refreshes and passes.
    issuer.serve("jwks-rotated.json");
    assert_eq!(auth.authenticate(Some(&rotated)).unwrap().subject, "ada");

    // A fresh authenticator that already spent its refresh on a bogus kid
    // stays rate-limited: the rotated key is not fetched again inside the
    // interval.
    issuer.serve("jwks.json");
    let auth = auth_with_interval(&issuer, Duration::from_secs(3600));
    let bogus = sign("nope", "ed25519-2.pem", &issuer.claims());
    assert!(matches!(
        auth.authenticate(Some(&bogus)),
        Err(AuthError::Invalid)
    ));
    issuer.serve("jwks-rotated.json");
    assert!(matches!(
        auth.authenticate(Some(&rotated)),
        Err(AuthError::Invalid)
    ));

    // With no rate limit the same authenticator picks it up.
    let auth = auth_with_interval(&issuer, Duration::ZERO);
    assert_eq!(auth.authenticate(Some(&rotated)).unwrap().subject, "ada");
}

fn auth_with_interval(issuer: &FixtureIssuer, interval: Duration) -> OidcAuth {
    OidcAuth::with_refresh_interval(config(issuer), interval).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn startup_fails_when_the_jwks_is_unreachable_or_the_config_is_bad() {
    let issuer = FixtureIssuer::start("jwks.json").await;
    let mut unreachable = config(&issuer);
    unreachable.issuers[0].jwks_url = Some("http://127.0.0.1:1/jwks".into());
    assert!(OidcAuth::new(unreachable).is_err());

    let empty = OidcConfig {
        issuers: vec![],
        clock_skew_secs: 60,
    };
    assert!(OidcAuth::new(empty).is_err());

    let mut twice = config(&issuer);
    twice
        .issuers
        .push(issuer_config(&issuer, Some(issuer.jwks_url())));
    assert!(OidcAuth::new(twice).is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn factory_builds_from_toml_and_reports_missing_sections() {
    let issuer = FixtureIssuer::start("jwks.json").await;
    let registries = ServerRegistries::with_builtins().unwrap();
    assert!(registries.authenticator.names().contains(&"oidc"));

    let toml = format!(
        "[auth]\nbackend = \"oidc\"\n[auth.oidc]\nclock_skew_secs = 5\n{}",
        issuer.toml_issuer()
    );
    let config = Config::from_toml_str(&toml).unwrap();
    let section = config.section("auth").unwrap().unwrap();
    let auth = registries
        .authenticator
        .build("oidc", &section, &())
        .unwrap();
    assert_eq!(
        auth.authenticate(Some(&sign_primary(&issuer.claims())))
            .unwrap()
            .subject,
        "ada"
    );

    let config = Config::from_toml_str("[auth]\nbackend = \"oidc\"\n").unwrap();
    let section = config.section("auth").unwrap().unwrap();
    assert!(registries
        .authenticator
        .build("oidc", &section, &())
        .is_err());
}
