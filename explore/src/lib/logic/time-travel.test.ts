import { describe, expect, it } from 'vitest';

import {
  buildTimeTravelGql,
  clampTime,
  customRange,
  DEFAULT_TIME_TRAVEL_FILTER,
  formatInstant,
  formatRangeSummary,
  fractionOfTime,
  isValidRange,
  MIN_RANGE_SPAN_MS,
  RELATIVE_INTERVALS,
  relativeRange,
  timeAtFraction,
  timelineTicks,
  zoomRange,
} from './time-travel';

const HOUR = 3_600_000;

describe('relative intervals', () => {
  it('offers presets from five minutes through seven days', () => {
    expect(RELATIVE_INTERVALS[0]).toEqual({ label: 'Last 5 minutes', durationMs: 300_000 });
    expect(RELATIVE_INTERVALS.at(-1)).toEqual({
      label: 'Last 7 days',
      durationMs: 7 * 24 * HOUR,
    });
  });

  it('anchors a relative range at now', () => {
    const range = relativeRange(RELATIVE_INTERVALS[3], 10 * HOUR);
    expect(range).toEqual({ startMs: 9 * HOUR, endMs: 10 * HOUR });
    expect(isValidRange(range)).toBe(true);
  });
});

describe('range math', () => {
  const range = { startMs: 1_000_000, endMs: 2_000_000 };

  it('maps fractions to times and back', () => {
    expect(timeAtFraction(range, 0)).toBe(1_000_000);
    expect(timeAtFraction(range, 0.5)).toBe(1_500_000);
    expect(timeAtFraction(range, 2)).toBe(2_000_000);
    expect(fractionOfTime(range, 1_250_000)).toBe(0.25);
    expect(fractionOfTime(range, 5_000_000)).toBe(1);
  });

  it('clamps times into the range', () => {
    expect(clampTime(range, 0)).toBe(1_000_000);
    expect(clampTime(range, 1_700_000)).toBe(1_700_000);
    expect(clampTime(range, 9_000_000)).toBe(2_000_000);
  });

  it('zooms to a drag selection regardless of drag direction', () => {
    expect(zoomRange(range, 0.75, 0.25)).toEqual({ startMs: 1_250_000, endMs: 1_750_000 });
  });

  it('never zooms below the minimum span', () => {
    const zoomed = zoomRange(range, 0.5, 0.5001);
    expect(zoomed.endMs - zoomed.startMs).toBe(MIN_RANGE_SPAN_MS);
    expect(isValidRange(zoomed)).toBe(true);
  });

  it('validates custom intervals', () => {
    expect(customRange(2_000, 1_000)).toEqual({
      ok: false,
      error: 'The interval end must be after its start.',
    });
    expect(customRange(Number.NaN, 1_000).ok).toBe(false);
    expect(customRange(0, 5_000).ok).toBe(false);
    expect(customRange(0, 60_000)).toEqual({ ok: true, range: { startMs: 0, endMs: 60_000 } });
  });
});

describe('timelineTicks', () => {
  it('produces evenly stepped ticks inside the range', () => {
    const start = new Date(2026, 6, 16, 12, 28).getTime();
    const ticks = timelineTicks({ startMs: start, endMs: start + 3 * HOUR }, 6);

    expect(ticks.length).toBeGreaterThanOrEqual(5);
    expect(ticks.length).toBeLessThanOrEqual(7);
    const steps = new Set(ticks.slice(1).map((tick, index) => tick.timeMs - ticks[index].timeMs));
    expect(steps.size).toBe(1);
    expect(steps.has(30 * 60_000)).toBe(true);
    for (const tick of ticks) {
      expect(tick.fraction).toBeGreaterThanOrEqual(0);
      expect(tick.fraction).toBeLessThanOrEqual(1);
      expect(tick.label).toMatch(/^\d{2}:\d{2}$/);
    }
  });

  it('labels day-scale ticks with dates and sub-minute ticks with seconds', () => {
    const start = new Date(2026, 0, 1, 0, 0).getTime();
    const dayTicks = timelineTicks({ startMs: start, endMs: start + 6 * 24 * HOUR }, 6);
    expect(dayTicks[0].label).toMatch(/^[A-Z][a-z]{2} \d{1,2}$/);

    const secondTicks = timelineTicks({ startMs: start, endMs: start + 60_000 }, 6);
    expect(secondTicks[0].label).toMatch(/^\d{2}:\d{2}:\d{2}$/);
  });

  it('returns no ticks for an empty range', () => {
    expect(timelineTicks({ startMs: 5, endMs: 5 })).toEqual([]);
  });
});

describe('formatting', () => {
  it('formats instants as local wall-clock time', () => {
    expect(formatInstant(new Date(2026, 6, 16, 14, 20, 26).getTime())).toBe('14:20:26');
  });

  it('summarizes same-day and cross-day ranges', () => {
    const start = new Date(2026, 6, 16, 12, 28).getTime();
    expect(formatRangeSummary({ startMs: start, endMs: start + 3 * HOUR })).toBe('12:28 - 15:28');
    expect(formatRangeSummary({ startMs: start, endMs: start + 24 * HOUR })).toBe(
      'Jul 16 12:28 - Jul 17 12:28',
    );
  });
});

describe('buildTimeTravelGql', () => {
  const at = Date.UTC(2026, 6, 16, 14, 20, 26);

  it('prefixes the filter with a valid-time clause', () => {
    const built = buildTimeTravelGql(DEFAULT_TIME_TRAVEL_FILTER, at, 'valid');
    expect(built).toEqual({
      ok: true,
      gql: `FOR VALID_TIME AS OF TIMESTAMP '2026-07-16T14:20:26.000Z'\n${DEFAULT_TIME_TRAVEL_FILTER}`,
    });
  });

  it('supports the system-time axis', () => {
    const built = buildTimeTravelGql('MATCH (n:Person) RETURN n', at, 'system');
    expect(built.ok && built.gql.startsWith('FOR SYSTEM_TIME AS OF TIMESTAMP')).toBe(true);
  });

  it('rejects empty filters, writes, and explicit temporal clauses', () => {
    expect(buildTimeTravelGql('   ', at, 'valid').ok).toBe(false);
    expect(buildTimeTravelGql("INSERT (:Person {_id: 1, name: 'Ada'})", at, 'valid').ok).toBe(
      false,
    );
    expect(buildTimeTravelGql('FOR VALID_TIME ALL MATCH (n) RETURN n', at, 'valid').ok).toBe(false);
    expect(
      buildTimeTravelGql("for system_time as of DATE '2024-01-01' MATCH (n) RETURN n", at, 'valid')
        .ok,
    ).toBe(false);
    expect(buildTimeTravelGql('MATCH (n) RETURN n', Number.NaN, 'valid').ok).toBe(false);
  });

  it('does not reject property names that merely contain FOR', () => {
    const built = buildTimeTravelGql('MATCH (n) WHERE n.platform = 1 RETURN n', at, 'valid');
    expect(built.ok).toBe(true);
  });
});
