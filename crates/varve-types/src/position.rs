use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum TypeError {
    #[error("log offset {0} exceeds 48 bits")]
    OffsetOverflow(u64),
}

const OFFSET_BITS: u32 = 48;
const OFFSET_MASK: u64 = (1 << OFFSET_BITS) - 1;

/// Position in the transaction log: epoch (high 16 bits) | offset (low 48 bits).
/// Epoch-major packing keeps u64 ordering == logical ordering. Spec §6.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct LogPosition(u64);

impl LogPosition {
    pub fn new(epoch: u16, offset: u64) -> Result<Self, TypeError> {
        if offset > OFFSET_MASK {
            return Err(TypeError::OffsetOverflow(offset));
        }
        Ok(LogPosition(((epoch as u64) << OFFSET_BITS) | offset))
    }

    pub fn epoch(&self) -> u16 {
        (self.0 >> OFFSET_BITS) as u16
    }

    pub fn offset(&self) -> u64 {
        self.0 & OFFSET_MASK
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }

    pub fn from_u64(v: u64) -> Self {
        LogPosition(v)
    }

    pub fn next(&self) -> Result<Self, TypeError> {
        Self::new(self.epoch(), self.offset() + 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packs_and_unpacks() {
        let p = LogPosition::new(3, 0x0000_ABCD_EF01_2345).unwrap();
        assert_eq!(p.epoch(), 3);
        assert_eq!(p.offset(), 0x0000_ABCD_EF01_2345);
        assert_eq!(LogPosition::from_u64(p.as_u64()), p);
    }

    #[test]
    fn epoch_major_ordering() {
        let old = LogPosition::new(1, u64::MAX >> 16).unwrap(); // max 48-bit offset
        let new = LogPosition::new(2, 0).unwrap();
        assert!(new > old);
        assert!(new.as_u64() > old.as_u64());
    }

    #[test]
    fn rejects_offset_over_48_bits() {
        assert!(LogPosition::new(0, 1u64 << 48).is_err());
    }

    #[test]
    fn next_increments_offset() {
        let p = LogPosition::new(0, 7).unwrap();
        assert_eq!(p.next().unwrap(), LogPosition::new(0, 8).unwrap());
    }

    #[test]
    fn golden_known_answer() {
        // Pins the exact on-disk packed representation: epoch in the high 16
        // bits, offset in the low 48 bits. A change here means the on-disk
        // LogPosition encoding changed and is a breaking change.
        let p = LogPosition::new(3, 0x0000_ABCD_EF01_2345).unwrap();
        assert_eq!(p.as_u64(), (3u64 << 48) | 0x0000_ABCD_EF01_2345);
    }

    #[test]
    fn next_fails_at_max_offset() {
        // The max 48-bit offset cannot be incremented without overflowing
        // into the epoch bits; next() must fail loudly rather than corrupt
        // the epoch.
        let p = LogPosition::new(0, (1u64 << 48) - 1).unwrap();
        assert!(p.next().is_err());
    }
}
