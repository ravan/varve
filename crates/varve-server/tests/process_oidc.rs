//! Silt-0a task 9: a real `varved` process with `[auth] backend = "oidc"`
//! accepts a token signed by the fixture issuer and refuses a static token.
#![cfg(all(feature = "http", feature = "oidc"))]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use reqwest::StatusCode;

#[path = "support/oidc_fixture.rs"]
mod oidc_fixture;
#[path = "support/process_cluster.rs"]
mod process_cluster;

use oidc_fixture::{sign_primary, FixtureIssuer};
use process_cluster::ProcessCluster;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn varved_accepts_oidc_tokens_and_refuses_static_ones() {
    let issuer = FixtureIssuer::start("jwks.json").await;
    let auth_toml = format!(
        "[auth]\nbackend = \"oidc\"\n[auth.oidc]\n{}",
        issuer.toml_issuer()
    );
    let cluster = ProcessCluster::start_writer_with_auth(&auth_toml)
        .await
        .unwrap();

    let token = sign_primary(&issuer.claims());
    let (status, body) = cluster
        .tx_with_bearer(
            cluster.writer_url(),
            &token,
            "INSERT (:Person {_id: 1, name: 'Ada'})",
        )
        .await
        .unwrap();
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["subject"], "ada", "{body}");

    let (status, body) = cluster
        .tx_with_bearer(
            cluster.writer_url(),
            cluster.token(),
            "INSERT (:Person {_id: 2, name: 'Bob'})",
        )
        .await
        .unwrap();
    assert_eq!(status, StatusCode::UNAUTHORIZED, "{body}");
    assert_eq!(body["code"], "unauthorized", "{body}");
}
