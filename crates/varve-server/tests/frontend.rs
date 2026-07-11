use varve::Db;
use varve_config::{BuildContext, Config, RegistryError};
use varve_server::ServerRegistries;

async fn build_frontend(toml: &str) -> Result<(), RegistryError> {
    let config = Config::from_toml_str(toml)?;
    let db = Db::open(config.clone())
        .await
        .map_err(|source| RegistryError::Build {
            kind: "protocol-frontend",
            name: "http".into(),
            source: Box::new(source),
        })?;
    let mut context = BuildContext::empty();
    context.insert(db);
    let section = config
        .section("server")
        .unwrap_or_else(varve_config::ConfigSection::empty);
    ServerRegistries::with_builtins()?
        .frontend
        .build("http", &section, &context)?;
    Ok(())
}

#[test]
fn builtin_frontend_names_include_http() {
    let names = ServerRegistries::with_builtins()
        .unwrap_or_else(|error| panic!("registries must build: {error}"))
        .frontend
        .names();
    assert_eq!(names, vec!["http"]);
}

#[tokio::test]
async fn invalid_socket_address_fails_build() {
    assert!(build_frontend(
        "[node]\nroles=['query']\n[server]\nbackend='http'\n[server.http]\nlisten='bad address'"
    )
    .await
    .is_err());
}

#[tokio::test]
async fn numeric_max_body_bytes_fails_build() {
    assert!(build_frontend("[node]\nroles=['query']\n[server]\nbackend='http'\n[server.http]\nlisten='127.0.0.1:0'\nmax_body_bytes=8388608")
        .await
        .is_err());
}

#[tokio::test]
async fn query_node_may_omit_advertised_address() {
    build_frontend(
        "[node]\nroles=['query']\n[server]\nbackend='http'\n[server.http]\nlisten='127.0.0.1:0'",
    )
    .await
    .unwrap_or_else(|error| panic!("query frontend must build: {error}"));
}

#[tokio::test]
async fn writer_node_requires_valid_http_advertised_address() {
    assert!(
        build_frontend("[server]\nbackend='http'\n[server.http]\nlisten='127.0.0.1:0'")
            .await
            .is_err()
    );
    assert!(build_frontend("[server]\nbackend='http'\n[server.http]\nlisten='127.0.0.1:0'\nadvertised_address='writer.example'")
        .await
        .is_err());
    build_frontend("[server]\nbackend='http'\n[server.http]\nlisten='127.0.0.1:0'\nadvertised_address='https://writer.example:8443'")
        .await
        .unwrap_or_else(|error| panic!("valid writer frontend must build: {error}"));
}

#[tokio::test]
async fn missing_database_context_is_a_build_error() {
    let config = Config::from_toml_str(
        "[server]\nbackend='http'\n[server.http]\nlisten='127.0.0.1:0'\nadvertised_address='http://writer.example'",
    )
    .unwrap_or_else(|error| panic!("config must parse: {error}"));
    let section = config
        .section("server")
        .unwrap_or_else(varve_config::ConfigSection::empty);
    let error = ServerRegistries::with_builtins()
        .unwrap_or_else(|error| panic!("registries must build: {error}"))
        .frontend
        .build("http", &section, &BuildContext::empty())
        .err()
        .unwrap_or_else(|| panic!("missing db must fail"));
    assert!(matches!(error, RegistryError::Build { .. }));
}

#[tokio::test]
async fn exactly_one_tls_path_is_invalid() {
    assert!(build_frontend("[node]\nroles=['query']\n[server]\nbackend='http'\n[server.http]\nlisten='127.0.0.1:0'\ntls_cert='cert.pem'")
        .await
        .is_err());
    assert!(build_frontend("[node]\nroles=['query']\n[server]\nbackend='http'\n[server.http]\nlisten='127.0.0.1:0'\ntls_key='key.pem'")
        .await
        .is_err());
}
