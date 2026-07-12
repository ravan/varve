//! Prints the generated `docs/book/src/ops/configuration.md` content to
//! stdout. `just docs-gen` redirects this into the committed file; the
//! `committed_configuration_page_matches_the_generator` test in
//! `varve-testkit/tests/config_reference_doc.rs` fails if that file drifts
//! from [`varve_testkit::config_reference::render`].
fn main() {
    print!("{}", varve_testkit::config_reference::render());
}
