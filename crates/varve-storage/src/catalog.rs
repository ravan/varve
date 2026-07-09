use crate::keys::{Recency, ScopedTrieKey, TrieKey};
use crate::{BlockManifest, StorageError, TrieEntry};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TrieState {
    Nascent,
    Live,
    Garbage,
}

#[derive(Clone)]
struct CatalogEntry {
    entry: TrieEntry,
    state: TrieState,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ScopedTrieEntry {
    pub scoped_key: ScopedTrieKey,
    pub entry: TrieEntry,
}

#[derive(Default)]
pub struct TrieCatalog {
    entries: BTreeMap<ScopedTrieKey, CatalogEntry>,
}

impl TrieCatalog {
    pub fn from_manifests(manifests: &[BlockManifest]) -> Result<TrieCatalog, StorageError> {
        let mut catalog = TrieCatalog::default();
        let Some(latest) = manifests.iter().max_by_key(|manifest| manifest.block_id) else {
            return Ok(catalog);
        };

        let latest_entries = parsed_manifest_entries(latest)?;
        let latest_keys: BTreeSet<_> = latest_entries
            .iter()
            .map(|entry| entry.catalog_key())
            .collect();

        for manifest in manifests {
            for entry in parsed_manifest_entries(manifest)? {
                if manifest.block_id == latest.block_id
                    || latest_keys.contains(&entry.catalog_key())
                {
                    continue;
                }
                catalog.entries.insert(
                    entry.catalog_key(),
                    CatalogEntry {
                        entry: entry.entry,
                        state: TrieState::Garbage,
                    },
                );
            }
        }

        for entry in &latest_entries {
            let state = classify_latest(entry, &latest_entries);
            catalog.entries.insert(
                entry.catalog_key(),
                CatalogEntry {
                    entry: entry.entry.clone(),
                    state,
                },
            );
        }

        Ok(catalog)
    }

    pub fn live_entries(&self) -> Vec<ScopedTrieEntry> {
        self.entries
            .iter()
            .filter(|(_, entry)| entry.state == TrieState::Live)
            .map(|(key, entry)| ScopedTrieEntry {
                scoped_key: key.clone(),
                entry: entry.entry.clone(),
            })
            .collect()
    }
}

struct ParsedEntry {
    scoped_key: ScopedTrieKey,
    entry: TrieEntry,
    key: TrieKey,
}

impl ParsedEntry {
    fn catalog_key(&self) -> ScopedTrieKey {
        self.scoped_key.clone()
    }

    fn same_scope(&self, other: &ParsedEntry) -> bool {
        self.scoped_key.scope == other.scoped_key.scope
    }
}

fn parsed_manifest_entries(manifest: &BlockManifest) -> Result<Vec<ParsedEntry>, StorageError> {
    let mut out = Vec::new();
    for manifest_entry in manifest.trie_entries() {
        let scoped_key = manifest_entry.scoped_trie_key();
        let key = scoped_key.parse_trie_key()?;
        out.push(ParsedEntry {
            scoped_key,
            entry: manifest_entry.entry.clone(),
            key,
        });
    }
    Ok(out)
}

fn classify_latest(entry: &ParsedEntry, latest: &[ParsedEntry]) -> TrieState {
    match (&entry.key.recency, entry.key.level) {
        (Recency::Current, 0) => TrieState::Live,
        (Recency::Week { .. }, 1) => {
            if latest.iter().any(|candidate| {
                entry.same_scope(candidate)
                    && candidate.key.level == 1
                    && candidate.key.recency == Recency::Current
                    && candidate.key.block == entry.key.block
            }) {
                TrieState::Live
            } else {
                TrieState::Nascent
            }
        }
        (Recency::Current, 2) => {
            if !entry.key.part.is_empty() && has_all_sibling_partitions(entry, latest) {
                TrieState::Live
            } else {
                TrieState::Nascent
            }
        }
        _ => TrieState::Live,
    }
}

fn has_all_sibling_partitions(entry: &ParsedEntry, latest: &[ParsedEntry]) -> bool {
    let Some((_, parent)) = entry.key.part.split_last() else {
        return false;
    };
    (0..crate::keys::TRIE_BRANCH_FACTOR).all(|bucket| {
        latest.iter().any(|candidate| {
            entry.same_scope(candidate)
                && candidate.key.level == entry.key.level
                && candidate.key.recency == entry.key.recency
                && candidate.key.block == entry.key.block
                && candidate.key.part.len() == parent.len() + 1
                && candidate.key.part.starts_with(parent)
                && candidate.key.part.last() == Some(&bucket)
        })
    })
}
