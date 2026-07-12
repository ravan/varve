use varve_types::{Doc, Iid, Instant, Value};

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
///
/// `src`/`dst` are the edge endpoints (spec §5.2): `Some` on EVERY event of
/// an edges table — including Delete/Erase, so adjacency families co-locate
/// an edge's full history under its endpoint sort keys — and `None` on node
/// events. Endpoints are immutable per edge `_id`.
#[derive(Debug, Clone, PartialEq)]
pub struct Event {
    pub iid: Iid,
    pub system_from: Instant,
    pub valid_from: Instant,
    pub valid_to: Instant,
    pub src: Option<Iid>,
    pub dst: Option<Iid>,
    pub op: Op,
}

fn doc_approx_bytes(doc: &Doc) -> usize {
    doc.iter()
        .map(|(key, value)| {
            key.len()
                + match value {
                    Value::Str(s) => s.len(),
                    Value::Bytes(b) => b.len(),
                    Value::Null | Value::Bool(_) | Value::Int(_) | Value::Float(_) => 8,
                }
        })
        .sum()
}

impl Event {
    /// Rough in-memory footprint: fixed overhead + label and doc byte
    /// lengths. Used only for the flush watermark — never for correctness
    /// (block encoding, query results, and replay all ignore this value).
    pub fn approx_bytes(&self) -> usize {
        let payload = match &self.op {
            Op::Put { labels, doc } => {
                labels.iter().map(String::len).sum::<usize>() + doc_approx_bytes(doc)
            }
            Op::Delete | Op::Erase => 0,
        };
        64 + payload
    }
}
