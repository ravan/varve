//! Per-block sort-key membership filter: a Bloom filter over every row's sort
//! key (`_iid` for the primary table, `src`/`dst` for the adjacency
//! families). A point lookup asks each block "may you hold this key?" and
//! skips the block on a definite no. Without it, every block owns one page
//! whose key range covers any random iid, so a point read costs one page per
//! block and grows with block count.
//!
//! False positives only cost a page read; false negatives are impossible.
//! ~10 bits per key with 7 probes gives roughly a 1% false-positive rate.

use crate::live::IndexError;
use varve_types::Iid;

const MAGIC: &[u8; 4] = b"VKF1";
const HEADER_LEN: usize = 16;
const BITS_PER_KEY: usize = 10;
const PROBES: u8 = 7;
const MIN_BITS: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyFilter {
    probes: u8,
    bits: Vec<u8>,
}

impl KeyFilter {
    pub fn build(keys: &[Iid]) -> KeyFilter {
        let bit_count = (keys.len() * BITS_PER_KEY)
            .max(MIN_BITS)
            .next_power_of_two();
        let mut filter = KeyFilter {
            probes: PROBES,
            bits: vec![0; bit_count / 8],
        };
        for key in keys {
            filter.insert(key);
        }
        filter
    }

    fn insert(&mut self, key: &Iid) {
        let mask = self.bit_count() - 1;
        let (h1, h2) = hashes(key);
        for i in 0..self.probes as u64 {
            let bit = (h1.wrapping_add(i.wrapping_mul(h2)) & mask) as usize;
            self.bits[bit / 8] |= 1 << (bit % 8);
        }
    }

    /// `false` means the key is definitely absent from the block.
    pub fn may_contain(&self, key: &Iid) -> bool {
        let mask = self.bit_count() - 1;
        let (h1, h2) = hashes(key);
        (0..self.probes as u64).all(|i| {
            let bit = (h1.wrapping_add(i.wrapping_mul(h2)) & mask) as usize;
            self.bits[bit / 8] & (1 << (bit % 8)) != 0
        })
    }

    fn bit_count(&self) -> u64 {
        (self.bits.len() * 8) as u64
    }

    pub fn approx_bytes(&self) -> usize {
        self.bits.len()
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(HEADER_LEN + self.bits.len());
        out.extend_from_slice(MAGIC);
        out.push(self.probes);
        out.extend_from_slice(&[0; 3]);
        out.extend_from_slice(&self.bit_count().to_le_bytes());
        out.extend_from_slice(&self.bits);
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<KeyFilter, IndexError> {
        if bytes.len() < HEADER_LEN || &bytes[..4] != MAGIC {
            return Err(IndexError::Codec("key filter header mismatch".into()));
        }
        let probes = bytes[4];
        let mut len = [0u8; 8];
        len.copy_from_slice(&bytes[8..16]);
        let bit_count = u64::from_le_bytes(len);
        let byte_count = (bit_count / 8) as usize;
        if probes == 0
            || bit_count < MIN_BITS as u64
            || !bit_count.is_power_of_two()
            || bytes.len() != HEADER_LEN + byte_count
        {
            return Err(IndexError::Codec("key filter length mismatch".into()));
        }
        Ok(KeyFilter {
            probes,
            bits: bytes[HEADER_LEN..].to_vec(),
        })
    }
}

/// Two independent 64-bit hashes from the key's halves. Iids are already
/// hash-derived, but the finalizer keeps the filter honest for any input.
fn hashes(key: &Iid) -> (u64, u64) {
    let bytes = key.as_bytes();
    let (mut lo, mut hi) = ([0u8; 8], [0u8; 8]);
    lo.copy_from_slice(&bytes[..8]);
    hi.copy_from_slice(&bytes[8..]);
    let (lo, hi) = (u64::from_le_bytes(lo), u64::from_le_bytes(hi));
    (mix(lo), mix(hi) | 1)
}

fn mix(mut x: u64) -> u64 {
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn iid(n: u64) -> Iid {
        Iid::derive("g", "t", &n.to_le_bytes())
    }

    #[test]
    fn contains_every_inserted_key() {
        let keys: Vec<Iid> = (0..10_000).map(iid).collect();
        let filter = KeyFilter::build(&keys);
        assert!(keys.iter().all(|k| filter.may_contain(k)));
    }

    #[test]
    fn false_positive_rate_is_low() {
        let keys: Vec<Iid> = (0..10_000).map(iid).collect();
        let filter = KeyFilter::build(&keys);
        let hits = (10_000..110_000)
            .map(iid)
            .filter(|k| filter.may_contain(k))
            .count();
        assert!(hits < 3_000, "false positives: {hits} of 100000");
    }

    #[test]
    fn empty_filter_rejects_everything() {
        let filter = KeyFilter::build(&[]);
        assert!(!filter.may_contain(&iid(1)));
    }

    #[test]
    fn wire_round_trips_and_rejects_garbage() {
        let filter = KeyFilter::build(&(0..100).map(iid).collect::<Vec<_>>());
        assert_eq!(KeyFilter::decode(&filter.encode()).unwrap(), filter);
        assert!(KeyFilter::decode(b"garbage").is_err());
        let mut short = filter.encode();
        short.pop();
        assert!(KeyFilter::decode(&short).is_err());
    }
}
