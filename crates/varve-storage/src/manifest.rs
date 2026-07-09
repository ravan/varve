//! Block manifest — protobuf envelope (spec §9). The manifest is the
//! ATOMIC COMMIT POINT of a flush: a data file without a manifest entry
//! is invisible garbage. Beyond the FULL trie inventory that log replay
//! would otherwise have to reconstruct, it carries the watermark and
//! tx-id/clock floors the log alone can no longer provide once trimmed.

use crate::keys::{manifest_block_id, manifest_key, MANIFEST_PREFIX};
use crate::store::{ObjectStore, StorageError};
use prost::Message;

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TrieEntry {
    #[prost(string, tag = "1")]
    pub trie_key: String,
    #[prost(uint64, tag = "2")]
    pub row_count: u64,
    #[prost(uint64, tag = "3")]
    pub data_len: u64,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableTries {
    #[prost(string, tag = "1")]
    pub graph: String,
    #[prost(string, tag = "2")]
    pub table: String,
    #[prost(message, repeated, tag = "3")]
    pub tries: Vec<TrieEntry>,
    /// Adjacency family (slice 6): `""` = the primary iid-sorted table,
    /// [`crate::ADJ_OUT`]/[`crate::ADJ_IN`] = the edge adjacency families.
    /// Proto3 tag 4: an empty string encodes to zero bytes, so pre-slice-6
    /// golden wire bytes are unchanged.
    #[prost(string, tag = "4")]
    pub family: String,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BlockManifest {
    #[prost(uint64, tag = "1")]
    pub block_id: u64,
    #[prost(uint64, tag = "2")]
    pub watermark: u64,
    #[prost(uint64, tag = "3")]
    pub max_tx_id: u64,
    #[prost(int64, tag = "4")]
    pub max_system_time_us: i64,
    #[prost(message, repeated, tag = "5")]
    pub tables: Vec<TableTries>,
}

#[derive(Clone, Copy, Debug)]
pub struct ManifestTrieEntry<'a> {
    pub graph: &'a str,
    pub table: &'a str,
    pub family: &'a str,
    pub entry: &'a TrieEntry,
}

impl BlockManifest {
    pub fn trie_entries(&self) -> impl Iterator<Item = ManifestTrieEntry<'_>> {
        self.tables.iter().flat_map(|table| {
            table.tries.iter().map(|entry| ManifestTrieEntry {
                graph: &table.graph,
                table: &table.table,
                family: &table.family,
                entry,
            })
        })
    }

    /// Protobuf wire bytes (the object body stored at `keys::manifest_key`).
    pub fn to_wire(&self) -> Vec<u8> {
        self.encode_to_vec()
    }

    pub fn from_wire(bytes: &[u8]) -> Result<BlockManifest, StorageError> {
        Ok(<BlockManifest as Message>::decode(bytes)?)
    }
}

/// Finds the newest committed manifest under `v1/blocks/` (lex-hex sorts
/// lexicographically in numeric order, but we parse-and-max explicitly
/// rather than rely on that, since foreign/stray keys under the prefix
/// must be ignored rather than corrupt the ordering).
pub async fn latest_manifest(
    store: &dyn ObjectStore,
) -> Result<Option<BlockManifest>, StorageError> {
    let keys = store.list(MANIFEST_PREFIX).await?;
    let latest = keys.iter().filter_map(|k| manifest_block_id(k)).max();
    let Some(latest) = latest else {
        return Ok(None);
    };
    let bytes = store.get(&manifest_key(latest)).await?;
    Ok(Some(BlockManifest::from_wire(&bytes)?))
}

pub async fn manifest_history(store: &dyn ObjectStore) -> Result<Vec<BlockManifest>, StorageError> {
    let mut ids: Vec<u64> = store
        .list(MANIFEST_PREFIX)
        .await?
        .iter()
        .filter_map(|key| manifest_block_id(key))
        .collect();
    ids.sort_unstable();

    let mut manifests = Vec::with_capacity(ids.len());
    for id in ids {
        let bytes = store.get(&manifest_key(id)).await?;
        manifests.push(BlockManifest::from_wire(&bytes)?);
    }
    Ok(manifests)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory_store;
    use bytes::Bytes;

    fn sample() -> BlockManifest {
        BlockManifest {
            block_id: 1,
            watermark: 5,
            max_tx_id: 3,
            max_system_time_us: 7,
            tables: vec![TableTries {
                graph: "default".into(),
                table: "nodes".into(),
                family: String::new(),
                tries: vec![TrieEntry {
                    trie_key: "l00-rc-b00".into(),
                    row_count: 2,
                    data_len: 9,
                }],
            }],
        }
    }

    #[test]
    fn wire_round_trips() {
        let m = sample();
        assert_eq!(BlockManifest::from_wire(&m.to_wire()).unwrap(), m);
    }

    #[test]
    fn trie_entries_iterates_table_scopes() {
        let m = sample();
        let entries = m.trie_entries().collect::<Vec<_>>();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].graph, "default");
        assert_eq!(entries[0].table, "nodes");
        assert_eq!(entries[0].family, "");
        assert_eq!(entries[0].entry.trie_key, "l00-rc-b00");
    }

    #[test]
    fn table_tries_family_round_trips() {
        let m = BlockManifest {
            block_id: 1,
            watermark: 0,
            max_tx_id: 1,
            max_system_time_us: 1,
            tables: vec![TableTries {
                graph: "default".into(),
                table: "edges".into(),
                family: "adj-out".into(),
                tries: vec![],
            }],
        };
        let back = BlockManifest::from_wire(&m.to_wire()).unwrap();
        assert_eq!(back.tables[0].family, "adj-out");
    }

    #[test]
    fn wire_golden_bytes() {
        // Pins field numbers and wire types (protobuf wire format is stable,
        // so exact bytes are safe to golden-test — slice-3 LogRecord pattern).
        #[rustfmt::skip]
        let expected: Vec<u8> = vec![
            0x08, 0x01,             // 1: block_id = 1
            0x10, 0x05,             // 2: watermark = 5
            0x18, 0x03,             // 3: max_tx_id = 3
            0x20, 0x07,             // 4: max_system_time_us = 7
            0x2A, 0x22,             // 5: tables[0], 34 bytes
            0x0A, 0x07, b'd', b'e', b'f', b'a', b'u', b'l', b't', // graph
            0x12, 0x05, b'n', b'o', b'd', b'e', b's',             // table
            0x1A, 0x10,             // tries[0], 16 bytes
            0x0A, 0x0A, b'l', b'0', b'0', b'-', b'r', b'c', b'-', b'b', b'0', b'0',
            0x10, 0x02,             // row_count = 2
            0x18, 0x09,             // data_len = 9
        ];
        assert_eq!(sample().to_wire(), expected);
    }

    #[test]
    fn from_wire_rejects_garbage() {
        assert!(matches!(
            BlockManifest::from_wire(&[0xFF, 0xFF, 0xFF]),
            Err(StorageError::Decode(_))
        ));
    }

    #[tokio::test]
    async fn latest_manifest_none_when_empty() {
        let store = memory_store();
        assert_eq!(latest_manifest(store.as_ref()).await.unwrap(), None);
    }

    #[tokio::test]
    async fn latest_manifest_picks_the_highest_block_id() {
        let store = memory_store();
        for block_id in [0u64, 1] {
            let m = BlockManifest {
                block_id,
                ..sample()
            };
            store
                .put(
                    &crate::keys::manifest_key(block_id),
                    Bytes::from(m.to_wire()),
                )
                .await
                .unwrap();
        }
        // A foreign key under the prefix is ignored, not an error.
        store
            .put("v1/blocks/stray.tmp", Bytes::from_static(b"x"))
            .await
            .unwrap();
        let latest = latest_manifest(store.as_ref()).await.unwrap().unwrap();
        assert_eq!(latest.block_id, 1);
    }

    #[tokio::test]
    async fn latest_manifest_surfaces_corruption() {
        let store = memory_store();
        store
            .put(
                &crate::keys::manifest_key(0),
                Bytes::from_static(b"\xFF\xFF"),
            )
            .await
            .unwrap();
        assert!(matches!(
            latest_manifest(store.as_ref()).await,
            Err(StorageError::Decode(_))
        ));
    }
}
