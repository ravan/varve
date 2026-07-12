use serde::Deserialize;
use std::collections::BTreeSet;
use varve_types::LogPosition;

use crate::TxReceipt;

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd)]
#[serde(rename_all = "kebab-case")]
pub enum NodeRole {
    Writer,
    Query,
    Compactor,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeRoles(BTreeSet<NodeRole>);

impl NodeRoles {
    pub(crate) fn all() -> Self {
        Self(BTreeSet::from([
            NodeRole::Writer,
            NodeRole::Query,
            NodeRole::Compactor,
        ]))
    }

    pub fn contains(&self, role: NodeRole) -> bool {
        self.0.contains(&role)
    }

    pub fn iter(&self) -> impl Iterator<Item = NodeRole> + '_ {
        self.0.iter().copied()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AppliedProgress {
    pub tx_id: u64,
    pub log_position: LogPosition,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BasisToken {
    TxId(u64),
    At(LogPosition),
}

impl From<TxReceipt> for BasisToken {
    fn from(receipt: TxReceipt) -> Self {
        Self::TxId(receipt.tx_id)
    }
}

impl From<&TxReceipt> for BasisToken {
    fn from(receipt: &TxReceipt) -> Self {
        Self::TxId(receipt.tx_id)
    }
}

impl From<LogPosition> for BasisToken {
    fn from(position: LogPosition) -> Self {
        Self::At(position)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeStatus {
    pub roles: NodeRoles,
    pub applied: AppliedProgress,
    pub manifest_block_id: Option<u64>,
    pub manifest_watermark: LogPosition,
    /// Latest known log head (Task 12, spec §12): the writer publishes its
    /// own durable watermark (lag 0 by construction); the follower publishes
    /// `max(last read position + 1, manifest watermark seen in the gap
    /// check)`, or the jumped cursor on a fence jump.
    pub log_head: LogPosition,
    pub follower_error: Option<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct ProgressState {
    pub applied: AppliedProgress,
    pub log_head: LogPosition,
    pub follower_error: Option<String>,
}

impl ProgressState {
    pub fn running(tx_id: u64, log_position: LogPosition, log_head: LogPosition) -> Self {
        Self {
            applied: AppliedProgress {
                tx_id,
                log_position,
            },
            log_head,
            follower_error: None,
        }
    }
}

/// Log lag in records (Task 12, spec §12): the same-epoch case is an exact
/// offset difference; a cross-epoch head is a transient condition (the
/// applied cursor hasn't yet observed the new epoch) approximated as
/// `head.offset() + 1` since the prior epoch's remaining length is unknown
/// without an I/O round trip.
pub fn log_lag_records(applied: LogPosition, head: LogPosition) -> u64 {
    if applied.epoch() == head.epoch() {
        head.offset().saturating_sub(applied.offset())
    } else {
        head.offset() + 1
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct NodeTuning {
    #[serde(default = "default_roles")]
    roles: Vec<NodeRole>,
    #[serde(default = "default_tail_poll_interval_ms")]
    pub tail_poll_interval_ms: u64,
    #[serde(default = "default_tail_batch_records")]
    pub tail_batch_records: u64,
    #[serde(default = "default_basis_timeout_ms")]
    pub basis_timeout_ms: u64,
    /// Bounded capacity of the writer's submission queue (slice 10):
    /// `Db::try_execute_as` returns `EngineError::Backpressure` immediately
    /// once this many submissions are already queued, rather than waiting.
    #[serde(default = "default_submission_queue_len")]
    pub submission_queue_len: usize,
}

impl NodeTuning {
    pub fn validate(self) -> Result<(NodeRoles, Self), String> {
        let roles = NodeRoles(self.roles.iter().copied().collect());
        if roles.0.is_empty() {
            return Err("[node] roles must not be empty".into());
        }
        if self.tail_batch_records == 0 {
            return Err("[node] tail_batch_records must be greater than zero".into());
        }
        if roles.contains(NodeRole::Compactor) && !roles.contains(NodeRole::Writer) {
            return Err("[node] compactor role requires writer role".into());
        }
        if self.submission_queue_len == 0 {
            return Err("[node] submission_queue_len must be greater than zero".into());
        }
        Ok((roles, self))
    }
}

fn default_roles() -> Vec<NodeRole> {
    NodeRoles::all().iter().collect()
}

fn default_tail_poll_interval_ms() -> u64 {
    50
}

fn default_tail_batch_records() -> u64 {
    1024
}

fn default_basis_timeout_ms() -> u64 {
    5000
}

fn default_submission_queue_len() -> usize {
    256
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tuning(submission_queue_len: usize) -> NodeTuning {
        NodeTuning {
            roles: default_roles(),
            tail_poll_interval_ms: default_tail_poll_interval_ms(),
            tail_batch_records: default_tail_batch_records(),
            basis_timeout_ms: default_basis_timeout_ms(),
            submission_queue_len,
        }
    }

    #[test]
    fn a_zero_submission_queue_len_is_rejected() {
        let err = tuning(0)
            .validate()
            .expect_err("submission_queue_len = 0 must be rejected");
        assert!(
            err.contains("submission_queue_len"),
            "expected the error to name submission_queue_len, got {err:?}"
        );
    }

    #[test]
    fn a_nonzero_submission_queue_len_is_accepted() {
        let (_, tuning) = tuning(1).validate().expect("nonzero queue len is valid");
        assert_eq!(tuning.submission_queue_len, 1);
    }

    #[test]
    fn log_lag_records_is_an_offset_difference_within_the_same_epoch() {
        let applied = LogPosition::new(0, 3).unwrap_or_else(|e| panic!("{e}"));
        let head = LogPosition::new(0, 7).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(log_lag_records(applied, head), 4);
    }

    #[test]
    fn log_lag_records_approximates_across_an_epoch_boundary() {
        let applied = LogPosition::new(0, 3).unwrap_or_else(|e| panic!("{e}"));
        let head = LogPosition::new(1, 2).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(log_lag_records(applied, head), 3);
    }
}
