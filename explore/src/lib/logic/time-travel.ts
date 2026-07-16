import { classifyGql } from './gql';

export type TemporalAxis = 'valid' | 'system';

export interface TimeRange {
  readonly startMs: number;
  readonly endMs: number;
}

export interface RelativeInterval {
  readonly label: string;
  readonly durationMs: number;
}

export interface TimelineTick {
  readonly timeMs: number;
  readonly fraction: number;
  readonly label: string;
}

export type TimeTravelGql =
  | { readonly ok: true; readonly gql: string }
  | { readonly ok: false; readonly error: string };

const SECOND = 1_000;
const MINUTE = 60 * SECOND;
const HOUR = 60 * MINUTE;
const DAY = 24 * HOUR;

// Varve v1 scans by label and requires typed edge patterns, so the broadest
// filter that parses everywhere is a bare node scan; richer topology filters
// come from the observed schema (see the empty-state suggestions).
export const DEFAULT_TIME_TRAVEL_FILTER = 'MATCH (n) RETURN n LIMIT 100';

export const RELATIVE_INTERVALS: readonly RelativeInterval[] = [
  { label: 'Last 5 minutes', durationMs: 5 * MINUTE },
  { label: 'Last 15 minutes', durationMs: 15 * MINUTE },
  { label: 'Last 30 minutes', durationMs: 30 * MINUTE },
  { label: 'Last 1 hour', durationMs: HOUR },
  { label: 'Last 3 hours', durationMs: 3 * HOUR },
  { label: 'Last 6 hours', durationMs: 6 * HOUR },
  { label: 'Last 12 hours', durationMs: 12 * HOUR },
  { label: 'Last 24 hours', durationMs: DAY },
  { label: 'Last 2 days', durationMs: 2 * DAY },
  { label: 'Last 7 days', durationMs: 7 * DAY },
];

const TICK_STEPS_MS = [
  SECOND,
  5 * SECOND,
  15 * SECOND,
  30 * SECOND,
  MINUTE,
  5 * MINUTE,
  15 * MINUTE,
  30 * MINUTE,
  HOUR,
  3 * HOUR,
  6 * HOUR,
  12 * HOUR,
  DAY,
  2 * DAY,
  7 * DAY,
  14 * DAY,
  30 * DAY,
] as const;

export const MIN_RANGE_SPAN_MS = 10 * SECOND;

export function isValidRange(range: TimeRange): boolean {
  return (
    Number.isFinite(range.startMs) &&
    Number.isFinite(range.endMs) &&
    range.endMs - range.startMs >= MIN_RANGE_SPAN_MS
  );
}

export function relativeRange(interval: RelativeInterval, nowMs: number): TimeRange {
  return { startMs: nowMs - interval.durationMs, endMs: nowMs };
}

export function clampTime(range: TimeRange, timeMs: number): number {
  return Math.min(range.endMs, Math.max(range.startMs, timeMs));
}

export function timeAtFraction(range: TimeRange, fraction: number): number {
  const bounded = Math.min(1, Math.max(0, fraction));
  return Math.round(range.startMs + (range.endMs - range.startMs) * bounded);
}

export function fractionOfTime(range: TimeRange, timeMs: number): number {
  const span = range.endMs - range.startMs;
  if (span <= 0) return 0;
  return Math.min(1, Math.max(0, (timeMs - range.startMs) / span));
}

/**
 * Zooms the range to the sub-range selected by two drag fractions. The
 * fractions may arrive in either order; the result never shrinks below
 * MIN_RANGE_SPAN_MS so the timeline stays usable.
 */
export function zoomRange(range: TimeRange, fromFraction: number, toFraction: number): TimeRange {
  const low = Math.min(fromFraction, toFraction);
  const high = Math.max(fromFraction, toFraction);
  let startMs = timeAtFraction(range, low);
  let endMs = timeAtFraction(range, high);

  if (endMs - startMs < MIN_RANGE_SPAN_MS) {
    const center = (startMs + endMs) / 2;
    startMs = Math.round(center - MIN_RANGE_SPAN_MS / 2);
    endMs = startMs + MIN_RANGE_SPAN_MS;
  }

  return { startMs, endMs };
}

export function customRange(
  startMs: number,
  endMs: number,
):
  | { readonly ok: true; readonly range: TimeRange }
  | { readonly ok: false; readonly error: string } {
  if (!Number.isFinite(startMs) || !Number.isFinite(endMs)) {
    return { ok: false, error: 'Both interval bounds are required.' };
  }
  if (endMs <= startMs) {
    return { ok: false, error: 'The interval end must be after its start.' };
  }
  if (endMs - startMs < MIN_RANGE_SPAN_MS) {
    return { ok: false, error: 'The interval must span at least ten seconds.' };
  }
  return { ok: true, range: { startMs, endMs } };
}

export function timelineTicks(range: TimeRange, targetCount = 6): TimelineTick[] {
  const span = range.endMs - range.startMs;
  if (span <= 0 || targetCount < 1) return [];

  const step =
    TICK_STEPS_MS.find((candidate) => span / candidate <= targetCount) ??
    Math.ceil(span / targetCount / (30 * DAY)) * 30 * DAY;
  const first = alignTick(range.startMs, step);
  const ticks: TimelineTick[] = [];

  for (let timeMs = first; timeMs <= range.endMs; timeMs += step) {
    ticks.push({
      timeMs,
      fraction: fractionOfTime(range, timeMs),
      label: formatTickLabel(timeMs, step),
    });
  }

  return ticks;
}

/** Aligns the first tick to a step boundary counted from the local midnight of the range start. */
function alignTick(startMs: number, stepMs: number): number {
  const midnight = new Date(startMs);
  midnight.setHours(0, 0, 0, 0);
  const baseMs = midnight.getTime();
  return baseMs + Math.ceil((startMs - baseMs) / stepMs) * stepMs;
}

function formatTickLabel(timeMs: number, stepMs: number): string {
  const date = new Date(timeMs);
  if (stepMs >= DAY) return formatDayLabel(date);
  const clock = `${pad(date.getHours())}:${pad(date.getMinutes())}`;
  if (stepMs < MINUTE) return `${clock}:${pad(date.getSeconds())}`;
  return clock;
}

const MONTH_LABELS = [
  'Jan',
  'Feb',
  'Mar',
  'Apr',
  'May',
  'Jun',
  'Jul',
  'Aug',
  'Sep',
  'Oct',
  'Nov',
  'Dec',
] as const;

function formatDayLabel(date: Date): string {
  return `${MONTH_LABELS[date.getMonth()]} ${date.getDate()}`;
}

export function formatInstant(timeMs: number): string {
  const date = new Date(timeMs);
  return `${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`;
}

export function formatInstantWithDate(timeMs: number): string {
  const date = new Date(timeMs);
  return `${formatDayLabel(date)}, ${formatInstant(timeMs)}`;
}

export function formatRangeSummary(range: TimeRange): string {
  const start = new Date(range.startMs);
  const end = new Date(range.endMs);
  const sameDay =
    start.getFullYear() === end.getFullYear() &&
    start.getMonth() === end.getMonth() &&
    start.getDate() === end.getDate();
  const startLabel = `${pad(start.getHours())}:${pad(start.getMinutes())}`;
  const endLabel = `${pad(end.getHours())}:${pad(end.getMinutes())}`;
  if (sameDay) return `${startLabel} - ${endLabel}`;
  return `${formatDayLabel(start)} ${startLabel} - ${formatDayLabel(end)} ${endLabel}`;
}

function pad(value: number): string {
  return String(value).padStart(2, '0');
}

const TEMPORAL_CLAUSE_PATTERN = /\bFOR\s+(?:VALID_TIME|SYSTEM_TIME)\b/i;

/**
 * Wraps a read filter in the temporal clause for the selected instant. The
 * view owns the temporal clause, so filters that already position an axis are
 * rejected instead of silently doubled (a duplicate axis is a parse error in
 * Varve).
 */
export function buildTimeTravelGql(
  filter: string,
  atMs: number,
  axis: TemporalAxis,
): TimeTravelGql {
  const trimmed = filter.trim();
  if (trimmed.length === 0) {
    return { ok: false, error: 'The topology filter must contain a MATCH query.' };
  }
  if (TEMPORAL_CLAUSE_PATTERN.test(trimmed)) {
    return {
      ok: false,
      error:
        'Remove the FOR VALID_TIME / FOR SYSTEM_TIME clause; the timeline supplies the temporal position.',
    };
  }
  if (classifyGql(trimmed) === 'write') {
    return { ok: false, error: 'Time travel runs read queries only.' };
  }
  if (!Number.isFinite(atMs)) {
    return { ok: false, error: 'The selected time is invalid.' };
  }

  const keyword = axis === 'system' ? 'SYSTEM_TIME' : 'VALID_TIME';
  const timestamp = new Date(atMs).toISOString();
  return { ok: true, gql: `FOR ${keyword} AS OF TIMESTAMP '${timestamp}'\n${trimmed}` };
}
