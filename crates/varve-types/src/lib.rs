pub mod iid;
pub mod position;
pub mod temporal;
pub mod trie;
pub mod value;
pub use iid::Iid;
pub use position::{LogPosition, TypeError};
pub use temporal::{Instant, TemporalBounds, TemporalDimension};
pub use trie::{Bucketer, MAX_TRIE_LEVELS, PAGE_LIMIT, TRIE_BRANCH_FACTOR, TRIE_LEVEL_BITS};
pub use value::{decode_doc, encode_doc, Doc, Value};

/// Persistence promised by a backend after a successful write.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Durability {
    /// Contents survive process restart.
    Durable,
    /// Contents live only for the process lifetime.
    Volatile,
    /// The implementation has not declared its persistence contract.
    Unknown,
}
