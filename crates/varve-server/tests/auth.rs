use varve_config::{BuildContext, Config};
use varve_server::{static_auth, AuthError, ServerRegistries};

#[test]
fn static_auth_accepts_exact_tokens_and_rejects_absent_or_near_matches() {
    let auth = static_auth(&[("alice", "correct-horse-battery-staple")])
        .unwrap_or_else(|error| panic!("valid static auth must build: {error}"));
    assert_eq!(
        auth.authenticate(Some("correct-horse-battery-staple"))
            .unwrap()
            .subject,
        "alice"
    );
    assert!(matches!(auth.authenticate(None), Err(AuthError::Missing)));
    assert!(matches!(
        auth.authenticate(Some("correct-horse-battery-staplef")),
        Err(AuthError::Invalid)
    ));
}

#[test]
fn public_static_auth_rejects_invalid_entries() {
    assert!(static_auth(&[]).is_err());
    assert!(static_auth(&[("", "secret")]).is_err());
    assert!(static_auth(&[("alice", "")]).is_err());
    assert!(static_auth(&[("alice", "same"), ("bob", "same")]).is_err());
}

fn build_static(tokens: &str) -> Result<(), varve_config::RegistryError> {
    let toml = format!("[auth]\nbackend='static'\n[auth.static]\n{tokens}");
    let config = Config::from_toml_str(&toml)?;
    let auth = config
        .section("auth")
        .ok_or_else(|| varve_config::RegistryError::Build {
            kind: "authenticator",
            name: "static".into(),
            source: Box::new(std::io::Error::other("test auth section is missing")),
        })?;
    ServerRegistries::with_builtins()?.authenticator.build(
        "static",
        &auth,
        &BuildContext::empty(),
    )?;
    Ok(())
}

#[test]
fn static_auth_config_rejects_empty_and_duplicate_tokens() {
    assert!(build_static("tokens = []").is_err());
    assert!(
        build_static("tokens = [{subject='a',token='same'},{subject='b',token='same'}]").is_err()
    );
}

#[test]
fn static_auth_config_rejects_empty_subjects_and_tokens() {
    assert!(build_static("tokens = [{subject='',token='secret'}]").is_err());
    assert!(build_static("tokens = [{subject='alice',token=''}]").is_err());
}
