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
    pub follower_error: Option<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct ProgressState {
    pub applied: AppliedProgress,
    pub follower_error: Option<String>,
}

impl ProgressState {
    pub fn running(tx_id: u64, log_position: LogPosition) -> Self {
        Self {
            applied: AppliedProgress {
                tx_id,
                log_position,
            },
            follower_error: None,
        }
    }
}

#[derive(Deserialize)]
pub(crate) struct NodeTuning {
    #[serde(default = "default_roles")]
    roles: Vec<NodeRole>,
    #[serde(default = "default_tail_poll_interval_ms")]
    pub tail_poll_interval_ms: u64,
    #[serde(default = "default_tail_batch_records")]
    pub tail_batch_records: u64,
    #[serde(default = "default_basis_timeout_ms")]
    pub basis_timeout_ms: u64,
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
