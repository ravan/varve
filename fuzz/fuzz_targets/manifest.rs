#![no_main]

use libfuzzer_sys::fuzz_target;
use varve_storage::{BlockManifest, TrieCatalog};

fuzz_target!(|data: &[u8]| {
    let Ok(manifest) = BlockManifest::from_wire(data) else {
        return;
    };
    // prost decode is not byte-canonical; semantic round-trip must hold.
    let reparsed = BlockManifest::from_wire(&manifest.to_wire()).expect("re-decode");
    assert_eq!(reparsed, manifest);
    for entry in manifest.trie_entries() {
        let _ = entry.scoped_trie_key().parse_trie_key(); // Result, never panic
    }
    let _ = TrieCatalog::from_manifests(std::slice::from_ref(&manifest));
});
