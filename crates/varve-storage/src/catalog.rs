use crate::keys::{Recency, TrieKey};
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
    graph: String,
    table: String,
    family: String,
    entry: TrieEntry,
    state: TrieState,
}

#[derive(Default)]
pub struct TrieCatalog {
    entries: BTreeMap<(String, String, String, String), CatalogEntry>,
}

impl TrieCatalog {
    pub fn from_manifests(manifests: &[BlockManifest]) -> Result<TrieCatalog, StorageError> {
        let mut catalog = TrieCatalog::default();
        let Some(latest) = manifests.iter().max_by_key(|manifest| manifest.block_id) else {
            return Ok(catalog);
        };

        let latest_entries = manifest_entries(latest)?;
        let latest_keys: BTreeSet<_> = latest_entries
            .iter()
            .map(|entry| entry.catalog_key())
            .collect();

        for manifest in manifests {
            for entry in manifest_entries(manifest)? {
                if manifest.block_id == latest.block_id
                    || latest_keys.contains(&entry.catalog_key())
                {
                    continue;
                }
                catalog.entries.insert(
                    entry.catalog_key(),
                    CatalogEntry {
                        graph: entry.graph,
                        table: entry.table,
                        family: entry.family,
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
                    graph: entry.graph.clone(),
                    table: entry.table.clone(),
                    family: entry.family.clone(),
                    entry: entry.entry.clone(),
                    state,
                },
            );
        }

        Ok(catalog)
    }

    pub fn live_entries(&self) -> Vec<(String, String, String, TrieEntry)> {
        self.entries
            .values()
            .filter(|entry| entry.state == TrieState::Live)
            .map(|entry| {
                (
                    entry.graph.clone(),
                    entry.table.clone(),
                    entry.family.clone(),
                    entry.entry.clone(),
                )
            })
            .collect()
    }
}

#[derive(Clone)]
struct ParsedEntry {
    graph: String,
    table: String,
    family: String,
    entry: TrieEntry,
    key: TrieKey,
}

impl ParsedEntry {
    fn catalog_key(&self) -> (String, String, String, String) {
        (
            self.graph.clone(),
            self.table.clone(),
            self.family.clone(),
            self.entry.trie_key.clone(),
        )
    }

    fn same_scope(&self, other: &ParsedEntry) -> bool {
        self.graph == other.graph && self.table == other.table && self.family == other.family
    }
}

fn manifest_entries(manifest: &BlockManifest) -> Result<Vec<ParsedEntry>, StorageError> {
    let mut out = Vec::new();
    for table in &manifest.tables {
        for entry in &table.tries {
            out.push(ParsedEntry {
                graph: table.graph.clone(),
                table: table.table.clone(),
                family: table.family.clone(),
                entry: entry.clone(),
                key: TrieKey::parse(&entry.trie_key)?,
            });
        }
    }
    Ok(out)
}

fn classify_latest(entry: &ParsedEntry, latest: &[ParsedEntry]) -> TrieState {
    match (&entry.key.recency, entry.key.level) {
        (_, 0) => TrieState::Live,
        (Recency::Week { .. }, 1) => {
            if latest.iter().any(|candidate| {
                entry.same_scope(candidate)
                    && candidate.key.level == 1
                    && candidate.key.recency == Recency::Current
                    && candidate.key.block >= entry.key.block
            }) {
                TrieState::Live
            } else {
                TrieState::Nascent
            }
        }
        (_, level) if level >= 2 && !entry.key.part.is_empty() => {
            if has_all_sibling_partitions(entry, latest) {
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
