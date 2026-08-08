import { describe, expect, it } from 'vitest';

import type { NormalizedRow } from './results';
import {
  buildTimeTravelGql,
  clampTime,
  customRange,
  datasetExtent,
  DEFAULT_TIME_TRAVEL_FILTER,
  fitRangeToExtent,
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

function row(values: Record<string, unknown>): NormalizedRow {
  return Object.fromEntries(
    Object.entries(values).map(([column, value]) => [column, { kind: 'value' as const, value }]),
  );
}

const DEC_08 = Date.parse('2021-12-08T00:00:00Z');
const DEC_10 = Date.parse('2021-12-10T10:15:00Z');

describe('datasetExtent', () => {
  it('spans the instants of columns naming the active axis', () => {
    const rows = [
      row({ cve_valid_from: '2021-12-10T10:15:00Z', product_valid_from: '2021-12-08T00:00:00Z' }),
      row({ cve_valid_from: '2021-12-10T10:15:00Z', product_valid_from: '2021-12-09T00:00:00Z' }),
    ];
    expect(datasetExtent(rows, 'valid')).toEqual({ minMs: DEC_08, maxMs: DEC_10 });
  });

  it('reads only the axis in play, so the two clocks never merge into one span', () => {
    const rows = [
      row({
        became_valid_from: '2021-12-10T10:15:00Z',
        ingest_system_from: '2026-07-28T17:46:30Z',
      }),
    ];
    expect(datasetExtent(rows, 'valid')).toEqual({ minMs: DEC_10, maxMs: DEC_10 });
    expect(datasetExtent(rows, 'system')).toEqual({
      minMs: Date.parse('2026-07-28T17:46:30Z'),
      maxMs: Date.parse('2026-07-28T17:46:30Z'),
    });
  });

  it('returns null when no column names the axis, or when rows are empty', () => {
    expect(datasetExtent([row({ 'cv.timeScanned': '2021-12-10T10:15:00Z' })], 'valid')).toBeNull();
    expect(datasetExtent([], 'valid')).toBeNull();
  });

  it('ignores values that are not ISO instants and unbounded end sentinels', () => {
    expect(datasetExtent([row({ x_valid_from: 'Dec 10 2021' })], 'valid')).toBeNull();
    expect(datasetExtent([row({ x_valid_from: 1_639_130_100_000 })], 'valid')).toBeNull();
    expect(datasetExtent([row({ x_valid_to: '9999-12-31T23:59:59Z' })], 'valid')).toBeNull();
    expect(datasetExtent([row({ x_valid_from: null })], 'valid')).toBeNull();
  });

  it('skips missing cells', () => {
    expect(datasetExtent([{ x_valid_from: { kind: 'missing' } }], 'valid')).toBeNull();
  });
});

describe('fitRangeToExtent', () => {
  const range = { startMs: 0, endMs: 10 * HOUR };

  it('leaves the range alone when the dataset already fits inside it', () => {
    expect(fitRangeToExtent(range, { minMs: HOUR, maxMs: 9 * HOUR })).toBeNull();
    expect(fitRangeToExtent(range, { minMs: 0, maxMs: 10 * HOUR })).toBeNull();
  });

  it('frames an out-of-range dataset with padding on both sides', () => {
    const fitted = fitRangeToExtent(range, { minMs: DEC_08, maxMs: DEC_10 });
    const span = DEC_10 - DEC_08;
    expect(fitted).toEqual({ startMs: DEC_08 - span * 0.1, endMs: DEC_10 + span * 0.1 });
  });

  it('re-centres at the current zoom when the dataset is a single instant', () => {
    const fitted = fitRangeToExtent(range, { minMs: DEC_10, maxMs: DEC_10 });
    expect(fitted).toEqual({ startMs: DEC_10 - 5 * HOUR, endMs: DEC_10 + 5 * HOUR });
  });

  it('never fits below the minimum usable span', () => {
    const fitted = fitRangeToExtent(
      { startMs: 0, endMs: MIN_RANGE_SPAN_MS },
      {
        minMs: DEC_10,
        maxMs: DEC_10 + 1_000,
      },
    );
    expect(fitted).not.toBeNull();
    expect(fitted!.endMs - fitted!.startMs).toBe(MIN_RANGE_SPAN_MS);
  });

  it('rejects a reversed or non-finite extent', () => {
    expect(fitRangeToExtent(range, { minMs: DEC_10, maxMs: DEC_08 })).toBeNull();
    expect(fitRangeToExtent(range, { minMs: Number.NaN, maxMs: DEC_10 })).toBeNull();
  });
});
