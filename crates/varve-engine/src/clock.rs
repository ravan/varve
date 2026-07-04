use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use varve_config::{ComponentFactory, ConfigSection, RegistryError};
use varve_types::Instant;

/// Transaction-time source (spec §4 `Clock`). `next()` is called once per
/// transaction by the writer loop only and is strictly increasing; caller
/// `watermark()` is a read-only "now" that is >= every already-assigned tx
/// time; `advance_to(floor)` raises the floor so future `next()` calls are
/// strictly greater than `floor` (recovery: replayed events stay in the
/// past).
pub trait Clock: Send + Sync {
    fn next(&self) -> Instant;
    fn watermark(&self) -> Instant;
    fn advance_to(&self, floor: Instant);
}

/// Strictly increasing wall-clock µs source — the builtin `system` clock
/// (spec §5.2: system_from is "assigned by the writer, monotonic per log").
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
}

impl Clock for MonotonicClock {
    /// Next transaction time: max(wall, last + 1). One call per tx.
    fn next(&self) -> Instant {
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
    fn watermark(&self) -> Instant {
        Instant::from_micros(wall_us().max(self.last_us.load(Ordering::SeqCst)))
    }

    /// Raises the floor so future `next()` calls are strictly greater than
    /// `floor`; never moves the clock backwards (`fetch_max`).
    fn advance_to(&self, floor: Instant) {
        self.last_us.fetch_max(floor.as_micros(), Ordering::SeqCst);
    }
}

/// Registry factory: `[clock] backend = "system"` (the default).
pub struct SystemClockFactory;

impl ComponentFactory<dyn Clock> for SystemClockFactory {
    fn name(&self) -> &'static str {
        "system"
    }

    fn build(&self, _cfg: &ConfigSection) -> Result<Arc<dyn Clock>, RegistryError> {
        Ok(Arc::new(MonotonicClock::new()))
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

    #[test]
    fn advance_to_floors_future_ticks() {
        let clock = MonotonicClock::new();
        let far_future = Instant::from_micros(i64::MAX - 10);
        clock.advance_to(far_future);
        assert!(clock.next() > far_future);
        assert!(clock.watermark() > far_future);
    }

    #[test]
    fn advance_to_never_moves_backwards() {
        let clock = MonotonicClock::new();
        let t = clock.next();
        clock.advance_to(Instant::from_micros(0)); // long in the past
        assert!(clock.next() > t);
    }

    #[test]
    fn system_factory_builds_a_clock() {
        use varve_config::{ComponentFactory, ConfigSection};
        let clock = SystemClockFactory.build(&ConfigSection::empty()).unwrap();
        assert!(clock.next() > Instant::from_micros(0));
    }
}
