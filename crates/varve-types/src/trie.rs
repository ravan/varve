use crate::Iid;

pub const TRIE_LEVEL_BITS: u8 = 2;
pub const TRIE_BRANCH_FACTOR: u8 = 1 << TRIE_LEVEL_BITS;
pub const MAX_TRIE_LEVELS: usize = 128 / TRIE_LEVEL_BITS as usize;
pub const PAGE_LIMIT: usize = 1024;

pub struct Bucketer;

impl Bucketer {
    pub fn bucket(iid: &Iid, level: usize) -> Option<u8> {
        if level >= MAX_TRIE_LEVELS {
            return None;
        }
        let bit_idx = level * TRIE_LEVEL_BITS as usize;
        let byte_idx = bit_idx / 8;
        let bit_offset = bit_idx % 8;
        let shift = 8 - TRIE_LEVEL_BITS as usize - bit_offset;
        Some((iid.as_bytes()[byte_idx] >> shift) & (TRIE_BRANCH_FACTOR - 1))
    }

    pub fn path(iid: &Iid, levels: usize) -> Option<Vec<u8>> {
        if levels > MAX_TRIE_LEVELS {
            return None;
        }
        (0..levels)
            .map(|level| Bucketer::bucket(iid, level))
            .collect()
    }

    pub fn contains(path: &[u8], iid: &Iid) -> bool {
        if path.len() > MAX_TRIE_LEVELS || path.iter().any(|bucket| *bucket >= TRIE_BRANCH_FACTOR) {
            return false;
        }
        path.iter()
            .copied()
            .enumerate()
            .all(|(level, bucket)| Bucketer::bucket(iid, level) == Some(bucket))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucketer_matches_xtdb_known_bit_patterns() {
        let iid = Iid::from_bytes([
            0b0001_1011,
            0b1110_0100,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
        ]);

        assert_eq!(TRIE_LEVEL_BITS, 2);
        assert_eq!(TRIE_BRANCH_FACTOR, 4);
        assert_eq!(Bucketer::bucket(&iid, 0), Some(0));
        assert_eq!(Bucketer::bucket(&iid, 1), Some(1));
        assert_eq!(Bucketer::bucket(&iid, 2), Some(2));
        assert_eq!(Bucketer::bucket(&iid, 3), Some(3));
        assert_eq!(Bucketer::bucket(&iid, 4), Some(3));
        assert_eq!(Bucketer::bucket(&iid, 5), Some(2));
        assert_eq!(Bucketer::path(&iid, 6), Some(vec![0, 1, 2, 3, 3, 2]));
        assert!(Bucketer::contains(&[0, 1, 2, 3], &iid));
        assert!(!Bucketer::contains(&[0, 1, 2, 2], &iid));
    }

    #[test]
    fn bucketer_rejects_paths_beyond_the_iid() {
        let iid = Iid::from_bytes([0; 16]);

        assert_eq!(Bucketer::bucket(&iid, MAX_TRIE_LEVELS - 1), Some(0));
        assert_eq!(Bucketer::bucket(&iid, MAX_TRIE_LEVELS), None);
        assert_eq!(Bucketer::path(&iid, MAX_TRIE_LEVELS + 1), None);
        assert!(!Bucketer::contains(&[0; MAX_TRIE_LEVELS + 1], &iid));
        assert!(!Bucketer::contains(&[TRIE_BRANCH_FACTOR], &iid));
    }
}
