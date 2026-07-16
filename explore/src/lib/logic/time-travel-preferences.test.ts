import { describe, expect, it } from 'vitest';

import type { StorageLike } from './workspace';
import { DEFAULT_TIME_TRAVEL_FILTER } from './time-travel';
import {
  DEFAULT_TIME_TRAVEL_PREFERENCES,
  loadTimeTravelPreferences,
  rememberRange,
  saveTimeTravelPreferences,
  TIME_TRAVEL_STORAGE_KEY,
} from './time-travel-preferences';

function memoryStorage(initial: Record<string, string> = {}): StorageLike {
  const items = new Map(Object.entries(initial));
  return {
    getItem: (key) => items.get(key) ?? null,
    setItem: (key, value) => void items.set(key, value),
    removeItem: (key) => void items.delete(key),
  };
}

describe('time travel preferences', () => {
  it('round-trips through storage', () => {
    const storage = memoryStorage();
    const preferences = {
      ...DEFAULT_TIME_TRAVEL_PREFERENCES,
      axis: 'system' as const,
      grouping: 'type' as const,
      clusterSize: 25,
      recentRanges: [{ startMs: 0, endMs: 60_000 }],
    };

    saveTimeTravelPreferences(storage, preferences);
    expect(loadTimeTravelPreferences(storage)).toEqual(preferences);
  });

  it('falls back to defaults for missing or corrupt storage', () => {
    expect(loadTimeTravelPreferences(memoryStorage())).toEqual(DEFAULT_TIME_TRAVEL_PREFERENCES);
    expect(
      loadTimeTravelPreferences(memoryStorage({ [TIME_TRAVEL_STORAGE_KEY]: 'not json' })),
    ).toEqual(DEFAULT_TIME_TRAVEL_PREFERENCES);
    expect(
      loadTimeTravelPreferences(memoryStorage({ [TIME_TRAVEL_STORAGE_KEY]: '[1,2]' })),
    ).toEqual(DEFAULT_TIME_TRAVEL_PREFERENCES);
  });

  it('sanitizes hostile field values individually', () => {
    const storage = memoryStorage({
      [TIME_TRAVEL_STORAGE_KEY]: JSON.stringify({
        filter: '   ',
        axis: 'both',
        grouping: 'giant',
        clusterSize: -3,
        recentRanges: [{ startMs: 9, endMs: 1 }, { startMs: 0, endMs: 60_000 }, 'junk'],
      }),
    });

    expect(loadTimeTravelPreferences(storage)).toEqual({
      filter: DEFAULT_TIME_TRAVEL_FILTER,
      axis: 'valid',
      grouping: 'auto',
      clusterSize: 2,
      recentRanges: [{ startMs: 0, endMs: 60_000 }],
    });
  });

  it('remembers recent ranges most-recent-first without duplicates, capped at five', () => {
    let preferences = DEFAULT_TIME_TRAVEL_PREFERENCES;
    for (let index = 0; index < 7; index += 1) {
      preferences = rememberRange(preferences, {
        startMs: index * 100_000,
        endMs: index * 100_000 + 60_000,
      });
    }
    preferences = rememberRange(preferences, { startMs: 500_000, endMs: 560_000 });

    expect(preferences.recentRanges).toHaveLength(5);
    expect(preferences.recentRanges[0]).toEqual({ startMs: 500_000, endMs: 560_000 });
    expect(rememberRange(preferences, { startMs: 5, endMs: 6 })).toBe(preferences);
  });
});
