use std::path::Path;

#[test]
fn committed_configuration_page_matches_the_generator() {
    let page =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/book/src/ops/configuration.md");
    let committed = std::fs::read_to_string(&page)
        .expect("docs/book/src/ops/configuration.md must exist — run `just docs-gen`");
    assert_eq!(
        committed,
        varve_testkit::config_reference::render(),
        "configuration.md drifted — run `just docs-gen` and commit the result"
    );
}
