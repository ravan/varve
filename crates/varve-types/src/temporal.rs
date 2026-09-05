use crate::position::TypeError;
use std::fmt;

/// Microseconds since the Unix epoch, UTC — the only timestamp representation
/// in Varve (Global Constraint: Timestamp(µs, UTC)).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Instant(i64);

impl Instant {
    /// "Beginning of time" sentinel.
    pub const MIN: Instant = Instant(i64::MIN);
    /// "Forever" sentinel — unset `_valid_to`, unsuperseded `_system_to` (spec §5.2).
    pub const END_OF_TIME: Instant = Instant(i64::MAX);

    pub const fn from_micros(us: i64) -> Self {
        Instant(us)
    }

    pub const fn as_micros(self) -> i64 {
        self.0
    }

    /// Whether a calendar renderer can format this instant. The sentinels sit
    /// outside chrono's range, so both `Display` here and the JSON row encoder
    /// (`varve::rows`) fall back to raw µs for them — callers that must produce
    /// a date, or choose a spelling, ask this first.
    pub fn is_calendar_renderable(self) -> bool {
        self.as_calendar().is_some()
    }

    fn as_calendar(self) -> Option<chrono::DateTime<chrono::Utc>> {
        chrono::DateTime::<chrono::Utc>::from_timestamp_micros(self.0)
    }

    /// RFC 3339 timestamp, e.g. `2020-01-01T00:00:00Z`; offsets normalized to UTC.
    pub fn parse_rfc3339(s: &str) -> Result<Self, TypeError> {
        chrono::DateTime::parse_from_rfc3339(s)
            .map(|dt| Instant(dt.with_timezone(&chrono::Utc).timestamp_micros()))
            .or_else(|err| {
                chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.fZ")
                    .map(|dt| Instant(dt.and_utc().timestamp_micros()))
                    .map_err(|_| err)
            })
            .map_err(|e| TypeError::InvalidTimestamp(format!("{s}: {e}")))
    }

    /// Calendar date `YYYY-MM-DD` as midnight UTC.
    pub fn parse_date(s: &str) -> Result<Self, TypeError> {
        let date = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
            .map_err(|e| TypeError::InvalidTimestamp(format!("{s}: {e}")))?;
        let midnight = date
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| TypeError::InvalidTimestamp(s.to_string()))?;
        Ok(Instant(midnight.and_utc().timestamp_micros()))
    }
}

impl fmt::Display for Instant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Sentinels and instants beyond chrono's range render as raw µs.
        match self.as_calendar() {
            Some(dt) => write!(
                f,
                "{}",
                dt.to_rfc3339_opts(chrono::SecondsFormat::Micros, true)
            ),
            None => write!(f, "{}us", self.0),
        }
    }
}

/// Half-open range `[lower, upper)` on one temporal axis.
/// Semantics ported from XTDB's `TemporalBounds.kt`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct TemporalDimension {
    lower: Instant,
    upper: Instant,
}

impl TemporalDimension {
    /// `AS OF t` — the single instant `t`.
    pub fn at(t: Instant) -> Self {
        Self {
            lower: t,
            upper: Instant(t.0.saturating_add(1)),
        }
    }

    /// `FROM a TO b` — `[a, b)`. Empty when `from >= to`.
    pub fn in_range(from: Instant, to: Instant) -> Self {
        Self {
            lower: from,
            upper: to,
        }
    }

    /// `BETWEEN a AND b` — `[a, b]` (closed upper, SQL:2011 style).
    /// Reversed bounds are empty; END_OF_TIME remains an exclusive sentinel.
    pub fn between(from: Instant, to: Instant) -> Self {
        Self {
            lower: from,
            upper: Instant(to.0.saturating_add(1)),
        }
    }

    pub fn all() -> Self {
        Self {
            lower: Instant::MIN,
            upper: Instant::END_OF_TIME,
        }
    }

    pub fn lower(&self) -> Instant {
        self.lower
    }

    pub fn upper(&self) -> Instant {
        self.upper
    }

    /// Empty ranges contain no instants and never intersect another range.
    pub fn is_empty(&self) -> bool {
        self.lower >= self.upper
    }

    pub fn intersects(&self, lower: Instant, upper: Instant) -> bool {
        !self.is_empty() && lower < upper && self.lower < upper && lower < self.upper
    }

    /// A one-microsecond window — what [`Self::at`] builds, and what every
    /// omitted `FOR` clause defaults to. Worth naming because a point window
    /// makes coincidence across a pattern free: anything passing the per-row
    /// temporal filter necessarily coexisted with everything else that did,
    /// so the pattern needs no cross-element intersection.
    pub fn is_point(&self) -> bool {
        !self.is_empty() && self.lower.0.checked_add(1) == Some(self.upper.0)
    }
}

impl Default for TemporalDimension {
    fn default() -> Self {
        Self::all()
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct TemporalBounds {
    pub valid: TemporalDimension,
    pub system: TemporalDimension,
}

impl TemporalBounds {
    pub fn intersects(
        &self,
        valid_from: Instant,
        valid_to: Instant,
        system_from: Instant,
        system_to: Instant,
    ) -> bool {
        self.valid.intersects(valid_from, valid_to)
            && self.system.intersects(system_from, system_to)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn us(n: i64) -> Instant {
        Instant::from_micros(n)
    }

    #[test]
    fn parse_rfc3339_known_answer() {
        assert_eq!(
            Instant::parse_rfc3339("2020-01-01T00:00:00Z")
                .unwrap()
                .as_micros(),
            1_577_836_800_000_000
        );
    }

    #[test]
    fn parse_normalizes_offsets_to_utc() {
        assert_eq!(
            Instant::parse_rfc3339("2020-01-01T02:00:00+02:00").unwrap(),
            Instant::parse_rfc3339("2020-01-01T00:00:00Z").unwrap()
        );
    }

    #[test]
    fn parse_date_is_midnight_utc() {
        assert_eq!(
            Instant::parse_date("2020-01-01").unwrap(),
            Instant::parse_rfc3339("2020-01-01T00:00:00Z").unwrap()
        );
    }

    #[test]
    fn parse_errors_are_reported() {
        assert!(Instant::parse_rfc3339("not a time").is_err());
        assert!(Instant::parse_date("2020-13-01").is_err());
    }

    #[test]
    fn display_round_trips() {
        let t = Instant::parse_rfc3339("2024-06-01T12:34:56.789012Z").unwrap();
        assert_eq!(Instant::parse_rfc3339(&t.to_string()).unwrap(), t);
    }

    #[test]
    fn sentinels_display_without_panicking() {
        assert!(!Instant::END_OF_TIME.to_string().is_empty());
        assert!(!Instant::MIN.to_string().is_empty());
    }

    /// Callers pick a spelling from `is_calendar_renderable` and expect `Display`
    /// to agree — the JSON row encoder masks exactly the cells this rejects, so a
    /// disagreement would either corrupt a response or drop a real date.
    #[test]
    fn calendar_renderability_agrees_with_display() {
        for instant in [
            Instant::MIN,
            Instant::END_OF_TIME,
            us(0),
            us(-1),
            us(1_639_131_300_000_000),
            Instant::from_micros(i64::MAX / 2),
            Instant::from_micros(i64::MIN / 2),
        ] {
            let rendered = instant.to_string();
            assert_eq!(
                instant.is_calendar_renderable(),
                !rendered.ends_with("us"),
                "{} rendered as {rendered}",
                instant.as_micros()
            );
        }
    }

    #[test]
    fn sentinels_are_not_calendar_renderable() {
        assert!(!Instant::END_OF_TIME.is_calendar_renderable());
        assert!(!Instant::MIN.is_calendar_renderable());
        assert!(us(1_639_131_300_000_000).is_calendar_renderable());
    }

    #[test]
    fn ordering_and_sentinels() {
        assert!(Instant::MIN < us(0));
        assert!(us(0) < Instant::END_OF_TIME);
    }

    #[test]
    fn dimension_at_is_a_single_instant() {
        let d = TemporalDimension::at(us(5));
        assert!(d.intersects(us(5), us(6)));
        assert!(d.intersects(us(0), us(6)));
        assert!(!d.intersects(us(6), us(10))); // starts after the point
        assert!(!d.intersects(us(0), us(5))); // half-open: ends exactly at the point
    }

    #[test]
    fn dimension_in_range_is_half_open() {
        let d = TemporalDimension::in_range(us(3), us(7));
        assert!(d.intersects(us(6), us(9)));
        assert!(!d.intersects(us(7), us(9))); // adjacency is not overlap
    }

    #[test]
    fn dimension_between_is_closed_at_the_top() {
        let d = TemporalDimension::between(us(3), us(7));
        assert!(d.intersects(us(7), us(9)));
        assert_eq!(
            TemporalDimension::between(us(3), Instant::END_OF_TIME).upper,
            Instant::END_OF_TIME // saturating +1
        );
    }

    #[test]
    fn empty_and_reversed_ranges_never_intersect() {
        for dim in [
            TemporalDimension::in_range(us(5), us(5)),
            TemporalDimension::in_range(us(7), us(3)),
            TemporalDimension::between(us(7), us(3)),
            TemporalDimension::at(Instant::END_OF_TIME),
        ] {
            assert!(dim.is_empty());
            assert!(!dim.is_point());
            assert!(!dim.intersects(Instant::MIN, Instant::END_OF_TIME));
        }
        let dim = TemporalDimension::all();
        assert!(!dim.intersects(us(5), us(5)));
        assert!(!dim.intersects(us(7), us(3)));
    }

    #[test]
    fn dimension_all_and_default() {
        assert_eq!(TemporalDimension::default(), TemporalDimension::all());
        assert!(TemporalDimension::all().intersects(Instant::MIN, us(0)));
    }

    #[test]
    fn bounds_require_both_axes_to_intersect() {
        let b = TemporalBounds {
            valid: TemporalDimension::at(us(5)),
            system: TemporalDimension::at(us(10)),
        };
        assert!(b.intersects(us(0), us(9), us(8), us(12)));
        assert!(!b.intersects(us(6), us(9), us(8), us(12))); // valid misses
        assert!(!b.intersects(us(0), us(9), us(11), us(12))); // system misses
    }
}
