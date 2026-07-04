use xxhash_rust::xxh3::Xxh3;

/// 16-byte internal entity id: xxh3-128 over length-prefixed (graph, table, user id bytes).
/// Spec §5.3.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Iid([u8; 16]);

impl Iid {
    pub fn derive(graph: &str, table: &str, user_id: &[u8]) -> Self {
        let mut h = Xxh3::new();
        for part in [graph.as_bytes(), table.as_bytes(), user_id] {
            h.update(&(part.len() as u64).to_le_bytes());
            h.update(part);
        }
        Iid(h.digest128().to_be_bytes())
    }

    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Iid(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_inputs_same_iid() {
        let a = Iid::derive("default", "nodes", b"42");
        let b = Iid::derive("default", "nodes", b"42");
        assert_eq!(a, b);
    }

    #[test]
    fn different_table_different_iid() {
        let a = Iid::derive("default", "nodes", b"42");
        let b = Iid::derive("default", "edges", b"42");
        assert_ne!(a, b);
    }

    #[test]
    fn no_concat_ambiguity() {
        // ("ab","c") must differ from ("a","bc") — length-prefixed hashing
        let a = Iid::derive("g", "ab", b"c");
        let b = Iid::derive("g", "a", b"bc");
        assert_ne!(a, b);
    }

    #[test]
    fn round_trips_bytes() {
        let a = Iid::derive("g", "t", b"x");
        assert_eq!(Iid::from_bytes(*a.as_bytes()), a);
    }

    #[test]
    fn golden_known_answer() {
        // Pins the exact on-disk IID byte format: xxh3-128 over
        // length-prefixed (little-endian u64 length + bytes) segments of
        // (graph, table, user_id), rendered big-endian. Changing this value
        // means the on-disk entity-id format changed and is a breaking change.
        let a = Iid::derive("default", "nodes", b"42");
        assert_eq!(
            *a.as_bytes(),
            [2, 233, 141, 102, 215, 51, 20, 26, 240, 188, 33, 129, 132, 140, 110, 218]
        );
    }
}
