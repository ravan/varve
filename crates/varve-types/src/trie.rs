use crate::Iid;

pub const TRIE_LEVEL_BITS: u8 = 2;
pub const TRIE_BRANCH_FACTOR: u8 = 1 << TRIE_LEVEL_BITS;

pub struct Bucketer;

impl Bucketer {
    pub fn bucket(iid: &Iid, level: usize) -> u8 {
        let bit_idx = level * TRIE_LEVEL_BITS as usize;
        assert!(bit_idx < 128, "trie level {level} exceeds 128-bit IID");
        let byte_idx = bit_idx / 8;
        let bit_offset = bit_idx % 8;
        let shift = 8 - TRIE_LEVEL_BITS as usize - bit_offset;
        (iid.as_bytes()[byte_idx] >> shift) & (TRIE_BRANCH_FACTOR - 1)
    }

    pub fn path(iid: &Iid, levels: usize) -> Vec<u8> {
        (0..levels)
            .map(|level| Bucketer::bucket(iid, level))
            .collect()
    }

    pub fn contains(path: &[u8], iid: &Iid) -> bool {
        path.iter()
            .copied()
            .enumerate()
            .all(|(level, bucket)| Bucketer::bucket(iid, level) == bucket)
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
        assert_eq!(Bucketer::bucket(&iid, 0), 0);
        assert_eq!(Bucketer::bucket(&iid, 1), 1);
        assert_eq!(Bucketer::bucket(&iid, 2), 2);
        assert_eq!(Bucketer::bucket(&iid, 3), 3);
        assert_eq!(Bucketer::bucket(&iid, 4), 3);
        assert_eq!(Bucketer::bucket(&iid, 5), 2);
        assert_eq!(Bucketer::path(&iid, 6), vec![0, 1, 2, 3, 3, 2]);
        assert!(Bucketer::contains(&[0, 1, 2, 3], &iid));
        assert!(!Bucketer::contains(&[0, 1, 2, 2], &iid));
    }
}
