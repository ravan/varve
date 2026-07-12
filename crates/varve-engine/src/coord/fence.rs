//! Epoch fences (spec §12): durable "epoch E ends here" markers written by
//! the failover coordinator once it has seized an epoch abandoned by a dead
//! writer. A record at position `(e, o)` is dead iff a fence exists for `e`
//! at or before `o` — the successor epoch's writer reassigns those tx ids,
//! so recovery must fold the record's effects into nothing (`db.rs::recover`).

use crate::db::EngineError;
use std::collections::BTreeMap;
use varve_storage::{keys, ObjectStore};
use varve_types::LogPosition;

/// epoch → fence_offset. A record at `(e, o)` is dead iff `fences[e] <= o`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct FenceMap(BTreeMap<u16, u64>);

impl FenceMap {
    /// `false` iff `position` sits at or behind its epoch's fence.
    pub fn is_live(&self, position: LogPosition) -> bool {
        match self.0.get(&position.epoch()) {
            Some(fence) => position.offset() < *fence,
            None => true,
        }
    }

    /// If `cursor` sits at/behind a fence in its epoch, the position where a
    /// reader continues: `(cursor.epoch() + 1, 0)`. `None` when unfenced.
    pub fn jump(&self, cursor: LogPosition) -> Result<Option<LogPosition>, EngineError> {
        match self.0.get(&cursor.epoch()) {
            Some(fence) if cursor.offset() >= *fence => {
                let next_epoch = cursor
                    .epoch()
                    .checked_add(1)
                    .ok_or(EngineError::EpochExhausted)?;
                Ok(Some(LogPosition::new(next_epoch, 0)?))
            }
            _ => Ok(None),
        }
    }

    #[cfg(test)]
    pub fn from_pairs(pairs: &[(u16, u64)]) -> FenceMap {
        FenceMap(pairs.iter().copied().collect())
    }
}

/// One epoch's fence document (spec §12): written once, by the coordinator
/// that seized the epoch after detecting the prior writer's heartbeat had
/// gone stale.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct FenceDoc {
    pub epoch: u16,
    pub fence_offset: u64,
    /// node_id of the seizing writer (spec §12 identity, Task 1).
    pub fenced_by: String,
    pub fenced_at_us: i64,
}

/// Every fence document under [`keys::EPOCH_FENCE_PREFIX`]. Foreign keys
/// under the prefix are ignored (the `parse_log_key`/`manifest_block_id`
/// policy).
pub(crate) async fn load_fences(store: &dyn ObjectStore) -> Result<FenceMap, EngineError> {
    let mut map = BTreeMap::new();
    for key in store.list(keys::EPOCH_FENCE_PREFIX).await? {
        if keys::parse_epoch_fence_key(&key).is_none() {
            continue;
        }
        let bytes = store.get(&key).await?;
        let doc: FenceDoc = serde_json::from_slice(&bytes)?;
        map.insert(doc.epoch, doc.fence_offset);
    }
    Ok(FenceMap(map))
}

// Write-side of the fence protocol: called by the `cas-failover` coordinator
// (`coord::cas`) once it has seized an abandoned epoch.
#[cfg_attr(not(feature = "cas-failover"), allow(dead_code))]
pub(crate) async fn write_fence(
    store: &dyn ObjectStore,
    doc: &FenceDoc,
) -> Result<(), EngineError> {
    let bytes = serde_json::to_vec(doc)?;
    store
        .put(&keys::epoch_fence_key(doc.epoch), bytes::Bytes::from(bytes))
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use varve_types::LogPosition;

    #[test]
    fn liveness_and_jump_follow_the_fence() {
        let fences = FenceMap::from_pairs(&[(0, 5)]);
        let live = LogPosition::new(0, 4).unwrap();
        let dead = LogPosition::new(0, 5).unwrap();
        let later = LogPosition::new(1, 0).unwrap();
        assert!(fences.is_live(live));
        assert!(!fences.is_live(dead));
        assert!(fences.is_live(later));
        assert_eq!(fences.jump(live).unwrap(), None);
        assert_eq!(fences.jump(dead).unwrap(), Some(later));
    }

    #[tokio::test]
    async fn fences_round_trip_through_the_store() {
        let store = varve_storage::memory_store();
        write_fence(
            store.as_ref(),
            &FenceDoc {
                epoch: 0,
                fence_offset: 7,
                fenced_by: "n1".into(),
                fenced_at_us: 1,
            },
        )
        .await
        .unwrap();
        let fences = load_fences(store.as_ref()).await.unwrap();
        assert!(!fences.is_live(LogPosition::new(0, 7).unwrap()));
        assert!(fences.is_live(LogPosition::new(0, 6).unwrap()));
    }
}
