#![cfg(feature = "tls")]

use std::{path::PathBuf, sync::Arc};
use varve::{Db, ProbeVerdict};
use varve_config::{BuildContext, Config};
use varve_server::{
    readiness_channel, static_auth, FrontendContext, PrometheusMetrics, ServerRegistries, Shutdown,
};

#[tokio::test]
async fn rustls_frontend_serves_health_and_shuts_down() {
    let fixtures = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let toml = format!(
        "[node]\nroles=['query']\n[server]\nbackend='http'\n[server.http]\nlisten='127.0.0.1:0'\ntls_cert={:?}\ntls_key={:?}",
        fixtures.join("tls-cert.pem"),
        fixtures.join("tls-key.pem")
    );
    let config =
        Config::from_toml_str(&toml).unwrap_or_else(|error| panic!("config must parse: {error}"));
    let db = Db::open(config.clone())
        .await
        .unwrap_or_else(|error| panic!("db must open: {error}"));
    let probe = db
        .probe_capabilities()
        .await
        .unwrap_or_else(|error| panic!("probe must run: {error}"));
    assert!(matches!(probe.verdict, ProbeVerdict::Supported));
    let mut build_context = BuildContext::empty();
    build_context.insert(db.clone());
    let server = config
        .section("server")
        .unwrap_or_else(varve_config::ConfigSection::empty);
    let frontend = ServerRegistries::with_builtins()
        .unwrap_or_else(|error| panic!("registries must build: {error}"))
        .frontend
        .build("http", &server, &build_context)
        .unwrap_or_else(|error| panic!("frontend must build: {error}"));
    let (reporter, mut readiness) = readiness_channel();
    let (shutdown_trigger, shutdown) = Shutdown::channel();
    let context = FrontendContext {
        db,
        authenticator: static_auth(&[("test", "secret")])
            .unwrap_or_else(|error| panic!("auth must build: {error}")),
        metrics: Arc::new(
            PrometheusMetrics::new().unwrap_or_else(|error| panic!("metrics must build: {error}")),
        ),
        probe,
        readiness: reporter,
    };
    let task = tokio::spawn(async move { frontend.serve(context, shutdown).await });
    let address = readiness
        .wait()
        .await
        .unwrap_or_else(|error| panic!("server must listen: {error}"));
    let certificate = reqwest::Certificate::from_pem(
        &std::fs::read(fixtures.join("tls-cert.pem"))
            .unwrap_or_else(|error| panic!("certificate must read: {error}")),
    )
    .unwrap_or_else(|error| panic!("certificate must parse: {error}"));
    let response = reqwest::Client::builder()
        .add_root_certificate(certificate)
        .build()
        .unwrap_or_else(|error| panic!("client must build: {error}"))
        .get(format!("https://{address}/healthz"))
        .send()
        .await
        .unwrap_or_else(|error| panic!("HTTPS request must succeed: {error}"));
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    shutdown_trigger.shutdown();
    task.await
        .unwrap_or_else(|error| panic!("serve task must join: {error}"))
        .unwrap_or_else(|error| panic!("serve must stop cleanly: {error}"));
}
