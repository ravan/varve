use varve_types::{Doc, Iid, Instant};

/// The operation carried by an event (spec §5.2). Node labels ride in the Put
/// alongside the document (spec §5.1 label set).
#[derive(Debug, Clone, PartialEq)]
pub enum Op {
    Put { labels: Vec<String>, doc: Doc },
    Delete,
    Erase,
}

/// Every mutation becomes an immutable event; `_system_to` and effective
/// valid ranges are never stored — always derived at read time (spec §5.2).
/// `Op::Erase` events carry `valid_from: Instant::MIN, valid_to:
/// Instant::END_OF_TIME` by convention (an erase removes the whole entity).
#[derive(Debug, Clone, PartialEq)]
pub struct Event {
    pub iid: Iid,
    pub system_from: Instant,
    pub valid_from: Instant,
    pub valid_to: Instant,
    pub op: Op,
}
