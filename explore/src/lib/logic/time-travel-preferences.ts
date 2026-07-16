import type { GroupingMode } from './clustering';
import { clampClusterSize, DEFAULT_CLUSTER_SIZE } from './clustering';
import type { StorageLike } from './workspace';
import type { TemporalAxis, TimeRange } from './time-travel';
import { DEFAULT_TIME_TRAVEL_FILTER, isValidRange } from './time-travel';

export interface TimeTravelPreferences {
  readonly filter: string;
  readonly axis: TemporalAxis;
  readonly grouping: GroupingMode;
  readonly clusterSize: number;
  readonly recentRanges: readonly TimeRange[];
}

export const TIME_TRAVEL_STORAGE_KEY = 'varve-explorer.time-travel.v1';

const MAX_RECENT_RANGES = 5;
const MAX_FILTER_LENGTH = 20_000;

export const DEFAULT_TIME_TRAVEL_PREFERENCES: TimeTravelPreferences = {
  filter: DEFAULT_TIME_TRAVEL_FILTER,
  axis: 'valid',
  grouping: 'auto',
  clusterSize: DEFAULT_CLUSTER_SIZE,
  recentRanges: [],
};

export function loadTimeTravelPreferences(storage: StorageLike): TimeTravelPreferences {
  try {
    const raw = storage.getItem(TIME_TRAVEL_STORAGE_KEY);
    if (raw === null) return DEFAULT_TIME_TRAVEL_PREFERENCES;
    return decodePreferences(JSON.parse(raw));
  } catch {
    return DEFAULT_TIME_TRAVEL_PREFERENCES;
  }
}

export function saveTimeTravelPreferences(
  storage: StorageLike,
  preferences: TimeTravelPreferences,
): void {
  try {
    storage.setItem(TIME_TRAVEL_STORAGE_KEY, JSON.stringify(preferences));
  } catch {
    // Storage can be unavailable or full; preferences then last for the session only.
  }
}

export function rememberRange(
  preferences: TimeTravelPreferences,
  range: TimeRange,
): TimeTravelPreferences {
  if (!isValidRange(range)) return preferences;
  const recentRanges = [
    range,
    ...preferences.recentRanges.filter(
      (recent) => recent.startMs !== range.startMs || recent.endMs !== range.endMs,
    ),
  ].slice(0, MAX_RECENT_RANGES);
  return { ...preferences, recentRanges };
}

function decodePreferences(value: unknown): TimeTravelPreferences {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    return DEFAULT_TIME_TRAVEL_PREFERENCES;
  }
  const record = value as Record<string, unknown>;

  return {
    filter:
      typeof record.filter === 'string' &&
      record.filter.trim().length > 0 &&
      record.filter.length <= MAX_FILTER_LENGTH
        ? record.filter
        : DEFAULT_TIME_TRAVEL_FILTER,
    axis: record.axis === 'system' ? 'system' : 'valid',
    grouping:
      record.grouping === 'none' || record.grouping === 'type' || record.grouping === 'auto'
        ? record.grouping
        : 'auto',
    clusterSize:
      typeof record.clusterSize === 'number'
        ? clampClusterSize(record.clusterSize)
        : DEFAULT_CLUSTER_SIZE,
    recentRanges: decodeRanges(record.recentRanges),
  };
}

function decodeRanges(value: unknown): readonly TimeRange[] {
  if (!Array.isArray(value)) return [];
  const ranges: TimeRange[] = [];
  for (const candidate of value.slice(0, MAX_RECENT_RANGES)) {
    if (typeof candidate !== 'object' || candidate === null) continue;
    const { startMs, endMs } = candidate as Record<string, unknown>;
    if (typeof startMs !== 'number' || typeof endMs !== 'number') continue;
    const range = { startMs, endMs };
    if (isValidRange(range)) ranges.push(range);
  }
  return ranges;
}
