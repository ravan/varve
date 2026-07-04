use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use varve_types::Instant;

/// Strictly increasing wall-clock µs source — the writer's transaction-time
/// authority (spec §5.2: system_from is "assigned by the writer, monotonic
/// per log"). A pluggable `Clock` interface (spec §4) arrives with
/// durability; this is the v0 single-process implementation.
#[derive(Default)]
pub struct MonotonicClock {
    last_us: AtomicI64,
}

fn wall_us() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_micros()).unwrap_or(i64::MAX))
        .unwrap_or(0) // pre-1970 clock: fall back to the monotonic counter
}

impl MonotonicClock {
    pub fn new() -> Self {
        Self::default()
    }

    /// Next transaction time: max(wall, last + 1). One call per tx.
    pub fn next(&self) -> Instant {
        let wall = wall_us();
        let mut last = self.last_us.load(Ordering::SeqCst);
        loop {
            let candidate = wall.max(last + 1);
            match self
                .last_us
                .compare_exchange(last, candidate, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => return Instant::from_micros(candidate),
                Err(actual) => last = actual,
            }
        }
    }

    /// max(wall, last) WITHOUT advancing the clock — query-time "now". It is
    /// always >= any already-assigned tx time, so `at(watermark())` sees all
    /// applied events.
    pub fn watermark(&self) -> Instant {
        Instant::from_micros(wall_us().max(self.last_us.load(Ordering::SeqCst)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn next_is_strictly_increasing_under_bursts() {
        let clock = MonotonicClock::new();
        let mut prev = clock.next();
        for _ in 0..10_000 {
            let t = clock.next();
            assert!(t > prev);
            prev = t;
        }
    }

    #[test]
    fn watermark_does_not_advance_and_is_at_least_the_last_assigned_time() {
        let clock = MonotonicClock::new();
        let assigned = clock.next();
        assert!(clock.watermark() >= assigned);
        let w1 = clock.watermark();
        let w2 = clock.watermark();
        // watermark() is read-only: repeated calls never move the clock
        // backwards, and neither call assigns a new tx time.
        assert!(w2 >= w1);
        assert!(clock.next() > assigned);
    }
}
