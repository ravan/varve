use varve_types::Instant;

/// Binary search in a DESCENDING slice. `Ok(idx)` on match, `Err(insertion)`
/// otherwise. Port of XTDB `Ceiling.kt` `binarySearch` (its `-left - 1`
/// not-found encoding becomes `Err`).
fn binary_search_desc(xs: &[Instant], needle: Instant) -> Result<usize, usize> {
    let (mut left, mut right) = (0usize, xs.len());
    while left < right {
        let mid = (left + right) / 2;
        match xs[mid].cmp(&needle) {
            std::cmp::Ordering::Equal => return Ok(mid),
            std::cmp::Ordering::Greater => left = mid + 1,
            std::cmp::Ordering::Less => right = mid,
        }
    }
    Err(left)
}

/// The descending staircase of "system time above which this valid range is
/// superseded", maintained while scanning one entity's events newest-first
/// (spec §7). Port of XTDB `Ceiling.kt`.
///
/// `valid_times` is a descending boundary list bounded by the sentinels
/// `END_OF_TIME … MIN`; `sys_time_ceilings[i]` is the ceiling of the interval
/// `[valid_times[i + 1], valid_times[i])`. Range indices used by the public
/// accessors count from the OLDEST valid time upward (Kotlin `reverseIdx`).
pub struct Ceiling {
    valid_times: Vec<Instant>,
    sys_time_ceilings: Vec<Instant>,
}

impl Default for Ceiling {
    fn default() -> Self {
        Self::new()
    }
}

impl Ceiling {
    pub fn new() -> Self {
        let mut ceiling = Ceiling {
            valid_times: Vec::new(),
            sys_time_ceilings: Vec::new(),
        };
        ceiling.reset();
        ceiling
    }

    pub fn reset(&mut self) {
        self.valid_times.clear();
        self.valid_times
            .extend([Instant::END_OF_TIME, Instant::MIN]);
        self.sys_time_ceilings.clear();
        self.sys_time_ceilings.push(Instant::END_OF_TIME);
    }

    fn reverse_idx(&self, idx: usize) -> usize {
        self.valid_times.len() - 1 - idx
    }

    pub fn valid_to(&self, range_idx: usize) -> Instant {
        self.valid_times[self.reverse_idx(range_idx + 1)]
    }

    pub fn system_time(&self, range_idx: usize) -> Instant {
        self.sys_time_ceilings[self.reverse_idx(range_idx) - 1]
    }

    /// Index of the range containing `valid_time` (in oldest-upward order).
    pub fn ceiling_index(&self, valid_time: Instant) -> usize {
        let mut idx = match binary_search_desc(&self.valid_times, valid_time) {
            Ok(i) | Err(i) => i,
        };
        if idx < self.valid_times.len() - 1 && valid_time < self.valid_times[idx] {
            idx += 1;
        }
        if idx == self.valid_times.len() {
            idx -= 1;
        }
        self.reverse_idx(idx)
    }

    /// Record that `[valid_from, valid_to)` is superseded above `system_from`.
    /// Port of `Ceiling.applyLog` — same case analysis, same order of operations.
    pub fn apply_log(&mut self, system_from: Instant, valid_from: Instant, valid_to: Instant) {
        if valid_from >= valid_to {
            return;
        }

        let (end, inserted_end) = match binary_search_desc(&self.valid_times, valid_to) {
            Ok(i) => (i, false),
            Err(i) => (i, true),
        };
        let (mut start, inserted_start) = match binary_search_desc(&self.valid_times, valid_from) {
            Ok(i) => (i, false),
            Err(i) => (i, true),
        };

        match (inserted_end, inserted_start) {
            (false, false) => {
                self.sys_time_ceilings[end] = system_from;
            }
            (false, true) => {
                self.valid_times.insert(start, valid_from);
                self.sys_time_ceilings.insert(end, system_from);
            }
            (true, false) => {
                self.valid_times.insert(end, valid_to);
                self.sys_time_ceilings.insert(end, system_from);
                start += 1;
            }
            (true, true) if end == start => {
                self.valid_times.insert(end, valid_to);
                self.sys_time_ceilings.insert(end, system_from);
                start += 1;
                self.valid_times.insert(start, valid_from);
                // end >= 1 always: valid_to can never insert above the
                // END_OF_TIME sentinel at index 0.
                let above = self.sys_time_ceilings[end - 1];
                self.sys_time_ceilings.insert(start, above);
            }
            (true, true) => {
                self.valid_times.insert(end, valid_to);
                self.sys_time_ceilings.insert(end, system_from);
                self.valid_times[start] = valid_from;
            }
        }

        // Collapse boundaries swallowed by [valid_from, valid_to).
        self.valid_times.drain(end + 1..start);
        self.sys_time_ceilings.drain(end + 1..start);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(super) const EOT: Instant = Instant::END_OF_TIME;
    pub(super) const TMIN: Instant = Instant::MIN;

    pub(super) fn us(n: i64) -> Instant {
        Instant::from_micros(n)
    }

    fn ts(ns: &[i64]) -> Vec<Instant> {
        ns.iter().copied().map(Instant::from_micros).collect()
    }

    #[test]
    fn binary_search_descending() {
        let list = ts(&[10, 8, 6, 4, 2]);
        assert_eq!(binary_search_desc(&list, us(10)), Ok(0));
        assert_eq!(binary_search_desc(&list, us(6)), Ok(2));
        assert_eq!(binary_search_desc(&list, us(2)), Ok(4));
        assert_eq!(binary_search_desc(&list, us(9)), Err(1));
        assert_eq!(binary_search_desc(&list, us(11)), Err(0));
        assert_eq!(binary_search_desc(&list, us(3)), Err(4));
        assert_eq!(binary_search_desc(&list, us(1)), Err(5));
    }

    #[test]
    fn ceiling_index_selects_the_covering_range() {
        // XTDB CeilingTest.testGetCeilingIndex: only valid_times matters here.
        let ceiling = Ceiling {
            valid_times: ts(&[10, 8, 6, 4, 2]),
            sys_time_ceilings: vec![],
        };
        assert_eq!(ceiling.ceiling_index(us(1)), 0);
        assert_eq!(ceiling.ceiling_index(us(2)), 0);
        assert_eq!(ceiling.ceiling_index(us(10)), 4);
        assert_eq!(ceiling.ceiling_index(us(11)), 4);
        assert_eq!(ceiling.ceiling_index(us(5)), 1);
    }

    #[test]
    fn applies_logs() {
        // XTDB CeilingTest.testAppliesLogs, step by step.
        let mut ceiling = Ceiling::new();
        assert_eq!(ceiling.valid_times, vec![EOT, TMIN]);
        assert_eq!(ceiling.sys_time_ceilings, vec![EOT]);

        ceiling.apply_log(us(4), us(4), EOT);
        assert_eq!(ceiling.valid_times, vec![EOT, us(4), TMIN]);
        assert_eq!(ceiling.sys_time_ceilings, vec![us(4), EOT]);

        // lower the whole ceiling
        ceiling.apply_log(us(3), us(2), EOT);
        assert_eq!(ceiling.valid_times, vec![EOT, us(2), TMIN]);
        assert_eq!(ceiling.sys_time_ceilings, vec![us(3), EOT]);

        // lower part of the ceiling
        ceiling.apply_log(us(2), us(1), us(4));
        assert_eq!(ceiling.valid_times, vec![EOT, us(4), us(1), TMIN]);
        assert_eq!(ceiling.sys_time_ceilings, vec![us(3), us(2), EOT]);

        // replace a range exactly
        ceiling.apply_log(us(1), us(1), us(4));
        assert_eq!(ceiling.valid_times, vec![EOT, us(4), us(1), TMIN]);
        assert_eq!(ceiling.sys_time_ceilings, vec![us(3), us(1), EOT]);

        // replace the whole middle section
        ceiling.apply_log(us(0), us(0), us(6));
        assert_eq!(ceiling.valid_times, vec![EOT, us(6), us(0), TMIN]);
        assert_eq!(ceiling.sys_time_ceilings, vec![us(3), us(0), EOT]);
    }

    #[test]
    fn replace_within_a_range() {
        // XTDB CeilingTest."test replace within a range"
        let mut ceiling = Ceiling::new();
        ceiling.apply_log(us(4), us(4), us(6));
        assert_eq!(ceiling.valid_times, vec![EOT, us(6), us(4), TMIN]);
        assert_eq!(ceiling.sys_time_ceilings, vec![EOT, us(4), EOT]);
    }

    #[test]
    fn empty_valid_range_is_a_no_op() {
        let mut ceiling = Ceiling::new();
        ceiling.apply_log(us(4), us(5), us(5));
        assert_eq!(ceiling.valid_times, vec![EOT, TMIN]);
        assert_eq!(ceiling.sys_time_ceilings, vec![EOT]);
    }
}
