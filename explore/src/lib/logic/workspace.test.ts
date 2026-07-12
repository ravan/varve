import { expect, it } from 'vitest';
import type { QueryParameters } from '../types';
import {
  addFavorite,
  addFrame,
  clearHistory,
  clearWorkspace,
  deleteFavorite,
  deserializeWorkspace,
  duplicateFavorite,
  emptyWorkspace,
  isSensitiveParameterKey,
  observeExecution,
  recordHistory,
  removeFrame,
  replaceFrame,
  serializeWorkspace,
  updateFavorite,
  updateSettings,
  type ExecutionFrame,
  type Favorite,
  type HistoryEntry,
} from './workspace';

it.each([
  'token',
  'authToken',
  'SESSION_ID',
  'Authorization',
  'auth-orization',
  'api-credential',
  'creden_tial',
  'clientSecret',
])('identifies sensitive parameter key %s despite case or separators', (key) => {
  expect(isSensitiveParameterKey(key)).toBe(true);
});

it.each(['name', 'position', 'basis', 'accountId'])(
  'does not identify safe parameter key %s as sensitive',
  (key) => {
    expect(isSensitiveParameterKey(key)).toBe(false);
  },
);

function historyEntry(overrides: Partial<HistoryEntry> = {}): HistoryEntry {
  return {
    gql: 'RETURN 1',
    mode: 'read',
    params: {},
    finishedAt: 1,
    durationMs: 10,
    rowCount: 1,
    effectCount: 0,
    outcome: 'success',
    runCount: 1,
    ...overrides,
  };
}

function completedFrame(id: string, overrides: Partial<ExecutionFrame> = {}): ExecutionFrame {
  return {
    id,
    gql: `RETURN ${id}`,
    mode: 'read',
    params: {},
    parameterSummary: '{}',
    state: 'success',
    startedAt: Number(id) || 1,
    finishedAt: (Number(id) || 1) + 1,
    durationMs: 1,
    pinned: false,
    response: { rows: [{ value: id }] },
    rawResponse: { token: `raw-${id}` },
    ...overrides,
  };
}

function favorite(overrides: Partial<Favorite> = {}): Favorite {
  return {
    id: 'favorite-1',
    name: 'One',
    gql: 'RETURN 1',
    mode: 'read',
    params: {},
    notes: 'A note',
    createdAt: 1,
    updatedAt: 1,
    ...overrides,
  };
}

it('provides safe default settings', () => {
  const state = emptyWorkspace();

  expect(state.settings).toEqual({
    theme: 'system',
    graphMotion: true,
    defaultResultTab: 'graph',
    historyEnabled: true,
    confirmBeforeClear: true,
  });
  expect(state).toEqual({
    frames: [],
    history: [],
    favorites: [],
    observedSchema: { labels: {}, relationshipTypes: {} },
    settings: state.settings,
  });
});

it('coalesces consecutive identical history and caps it at 100', () => {
  let state = emptyWorkspace();
  state = recordHistory(state, historyEntry({ gql: 'RETURN 1', finishedAt: 1 }));
  state = recordHistory(state, historyEntry({ gql: 'RETURN 1', finishedAt: 2 }));
  expect(state.history).toHaveLength(1);
  expect(state.history[0].runCount).toBe(2);
  expect(state.history[0].finishedAt).toBe(2);

  for (let index = 0; index < 101; index += 1) {
    state = recordHistory(state, historyEntry({ gql: `RETURN ${index}`, finishedAt: index + 3 }));
  }

  expect(state.history).toHaveLength(100);
  expect(state.history[0].gql).toBe('RETURN 100');
  expect(state.history.at(-1)?.gql).toBe('RETURN 1');
});

it('only coalesces consecutive history entries with identical submissions', () => {
  const params: QueryParameters = { value: 1 };
  let state = recordHistory(emptyWorkspace(), historyEntry({ params }));
  state = recordHistory(state, historyEntry({ gql: 'RETURN 2' }));
  state = recordHistory(state, historyEntry({ params, finishedAt: 3 }));

  expect(state.history).toHaveLength(3);
  expect(state.history[0].runCount).toBe(1);
});

it('does not record history when history is disabled', () => {
  const state = updateSettings(emptyWorkspace(), { historyEnabled: false });

  expect(recordHistory(state, historyEntry())).toBe(state);
});

it('evicts the oldest completed unpinned frame above 25', () => {
  const state = Array.from({ length: 26 }, (_, id) => completedFrame(String(id))).reduce(
    (current, frame) => addFrame(current, frame),
    emptyWorkspace(),
  );
  expect(state.frames).toHaveLength(25);
  expect(state.frames.some((frame) => frame.id === '0')).toBe(false);
});

it('retains pinned and active frames while evicting completed unpinned frames', () => {
  let state = addFrame(emptyWorkspace(), completedFrame('0', { pinned: true }));
  state = addFrame(state, completedFrame('active', { state: 'running', finishedAt: undefined }));
  for (let index = 1; index <= 26; index += 1) {
    state = addFrame(state, completedFrame(String(index)));
  }

  expect(state.frames.some((frame) => frame.id === '0')).toBe(true);
  expect(state.frames.some((frame) => frame.id === 'active')).toBe(true);
  expect(state.frames.filter((frame) => frame.state !== 'running' && !frame.pinned)).toHaveLength(
    25,
  );
});

it('enforces the completed frame cap when an active frame completes', () => {
  let state = addFrame(emptyWorkspace(), completedFrame('active', { state: 'running' }));
  for (let index = 1; index <= 25; index += 1) {
    state = addFrame(state, completedFrame(String(index)));
  }

  state = replaceFrame(state, completedFrame('active', { startedAt: 0 }));

  expect(state.frames).toHaveLength(25);
  expect(state.frames.some((frame) => frame.id === 'active')).toBe(false);
});

it('removes frames by id without changing state for a missing id', () => {
  const state = addFrame(emptyWorkspace(), completedFrame('one'));

  expect(removeFrame(state, 'missing')).toBe(state);
  expect(removeFrame(state, 'one').frames).toEqual([]);
});

it('keeps source state and nested inputs immutable across transitions', () => {
  const params: QueryParameters = { nested: 'value' };
  const before = emptyWorkspace();
  const after = recordHistory(before, historyEntry({ params }));

  expect(before.history).toEqual([]);
  expect(after).not.toBe(before);
  expect(after.history).not.toBe(before.history);
  expect(() => {
    (after.history[0].params as Record<string, unknown>).nested = 'changed';
  }).toThrow();
});

it('supports immutable favorites create, edit, duplicate, and delete', () => {
  const empty = emptyWorkspace();
  const created = addFavorite(empty, favorite());
  const edited = updateFavorite(created, 'favorite-1', {
    name: 'Edited',
    notes: undefined,
    updatedAt: 2,
  });
  const duplicated = duplicateFavorite(edited, 'favorite-1', 'favorite-2', 3);
  const removed = deleteFavorite(duplicated, 'favorite-1');

  expect(empty.favorites).toEqual([]);
  expect(created.favorites[0].name).toBe('One');
  expect(edited.favorites[0]).toMatchObject({ name: 'Edited', updatedAt: 2 });
  expect(edited.favorites[0]).not.toHaveProperty('notes');
  expect(duplicated.favorites[1]).toMatchObject({
    id: 'favorite-2',
    name: 'Edited copy',
    createdAt: 3,
    updatedAt: 3,
  });
  expect(removed.favorites.map(({ id }) => id)).toEqual(['favorite-2']);
});

it('requires explicit confirmation before clearing workspace data', () => {
  const state = updateSettings(addFavorite(emptyWorkspace(), favorite()), {
    confirmBeforeClear: false,
  });

  expect(clearWorkspace(state, false)).toBe(state);
  expect(clearWorkspace(state, true)).toEqual(emptyWorkspace());
});

it('requires explicit confirmation before clearing history', () => {
  const state = recordHistory(emptyWorkspace(), historyEntry());

  expect(clearHistory(state, false)).toBe(state);
  expect(clearHistory(state, true).history).toEqual([]);
});

it('observes schema from successful executions and favorites only', () => {
  let state = observeExecution(
    emptyWorkspace(),
    'MATCH (p:Person)-[:KNOWS]->(q:Person) RETURN p',
    'error',
    1,
  );
  expect(state.observedSchema).toEqual({ labels: {}, relationshipTypes: {} });

  state = observeExecution(state, 'MATCH (p:Person) RETURN p', 'success', 2);
  state = addFavorite(
    state,
    favorite({
      gql: 'MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a',
      createdAt: 3,
      updatedAt: 3,
    }),
  );

  expect(state.observedSchema.labels.Person).toMatchObject({
    count: 3,
    firstSeen: 2,
    lastSeen: 3,
  });
  expect(state.observedSchema.relationshipTypes.KNOWS).toMatchObject({
    count: 1,
    firstSeen: 3,
    lastSeen: 3,
  });
});

it('serializes schema version 1 without any frame response or raw body', () => {
  let state = addFrame(emptyWorkspace(), completedFrame('complete'));
  state = addFrame(
    state,
    completedFrame('active', {
      state: 'running',
      finishedAt: undefined,
      response: { token: 'active-body-secret' },
      rawResponse: { token: 'active-raw-secret' },
    }),
  );

  const serialized = serializeWorkspace(state);
  const value = JSON.parse(serialized) as {
    version: number;
    workspace: { frames: Record<string, unknown>[] };
  };

  expect(value.version).toBe(1);
  expect(value.workspace.frames[0]).not.toHaveProperty('response');
  expect(value.workspace.frames[0]).not.toHaveProperty('rawResponse');
  expect(value.workspace.frames[1]).not.toHaveProperty('response');
  expect(value.workspace.frames[1]).not.toHaveProperty('rawResponse');
  expect(serialized).not.toContain('active-body-secret');
  expect(serialized).not.toContain('active-raw-secret');
  expect(serialized).not.toContain('raw-complete');
});

it('serializes the exact read basis and timeout in the safe frame snapshot', () => {
  const frame = completedFrame('read-request', {
    readBasis: 'at:9007199254740991',
    basisTimeoutMs: 12_345,
  });
  const state = addFrame(emptyWorkspace(), frame);

  const record = JSON.parse(serializeWorkspace(state)) as {
    workspace: { frames: Record<string, unknown>[] };
  };

  expect(record.workspace.frames[0]).toMatchObject({
    readBasis: 'at:9007199254740991',
    basisTimeoutMs: 12_345,
  });
});

it('restores the exact read basis and timeout from a strict workspace record', () => {
  const record = JSON.parse(
    serializeWorkspace(addFrame(emptyWorkspace(), completedFrame('read-request'))),
  ) as { workspace: { frames: Record<string, unknown>[] } };
  record.workspace.frames[0].readBasis = 42;
  record.workspace.frames[0].basisTimeoutMs = 30_001;

  const restored = deserializeWorkspace(JSON.stringify(record));

  expect(restored.frames[0]).toMatchObject({ readBasis: 42, basisTimeoutMs: 30_001 });
});

it('decodes a valid v1 workspace record', () => {
  let original = recordHistory(emptyWorkspace(), historyEntry());
  original = addFavorite(original, favorite());
  original = updateSettings(original, { theme: 'dark', defaultResultTab: 'table' });

  expect(deserializeWorkspace(serializeWorkspace(original))).toEqual(original);
});

it('restores v1 storage within frame and history limits', () => {
  const oversized = {
    ...emptyWorkspace(),
    frames: Array.from({ length: 26 }, (_, id) => completedFrame(String(id))),
    history: Array.from({ length: 101 }, (_, id) => historyEntry({ gql: `RETURN ${id}` })),
  };

  const restored = deserializeWorkspace(serializeWorkspace(oversized));

  expect(restored.frames).toHaveLength(25);
  expect(restored.frames.some(({ id }) => id === '0')).toBe(false);
  expect(restored.history).toHaveLength(100);
});

it('resets the whole workspace when v1 storage contains unsafe fields', () => {
  const workspace = {
    ...emptyWorkspace(),
    frames: [
      completedFrame('stored', {
        response: { rows: [], authToken: 'stored-token' },
        rawResponse: { secret: 'stored-raw' },
      }),
    ],
  };

  const restored = deserializeWorkspace(JSON.stringify({ version: 1, workspace }));

  expect(restored).toEqual(emptyWorkspace());
});

it.each(['token', 'RAW', 'rawResponse', 'Authorization', 'credential', 'clientSecret'])(
  'resets v1 storage containing nested forbidden key %s',
  (key) => {
    const record = JSON.parse(
      serializeWorkspace(recordHistory(emptyWorkspace(), historyEntry())),
    ) as {
      workspace: Record<string, unknown>;
    };
    record.workspace.extra = { nested: { [key]: 'unsafe' } };

    expect(deserializeWorkspace(JSON.stringify(record))).toEqual(emptyWorkspace());
  },
);

it.each(['__proto__', 'constructor', 'prototype'])(
  'resets v1 storage containing prototype-pollution key %s',
  (key) => {
    const record = JSON.parse(
      serializeWorkspace(recordHistory(emptyWorkspace(), historyEntry())),
    ) as {
      workspace: { history: { params: Record<string, unknown> }[] };
    };
    Object.defineProperty(record.workspace.history[0].params, key, {
      enumerable: true,
      value: 'unsafe',
    });

    expect(deserializeWorkspace(JSON.stringify(record))).toEqual(emptyWorkspace());
  },
);

it('drops entries with invalid Varve parameters instead of serializing arbitrary values', () => {
  const validHistory = historyEntry({ gql: 'RETURN $value', params: { value: 1 } });
  const invalidHistory = historyEntry({
    gql: 'RETURN $value',
    params: { value: ['arbitrary'] } as unknown as QueryParameters,
  });
  const nonFiniteHistory = historyEntry({
    gql: 'RETURN $value',
    params: { value: Number.NaN },
  });
  const invalidFavorite = favorite({
    params: { bytes: { $bytes: 'not base64' } } as QueryParameters,
  });
  const state = {
    ...emptyWorkspace(),
    history: [validHistory, invalidHistory, nonFiniteHistory],
    favorites: [invalidFavorite],
  };

  const record = JSON.parse(serializeWorkspace(state)) as {
    workspace: { history: HistoryEntry[]; favorites: Favorite[]; frames: ExecutionFrame[] };
  };

  expect(record.workspace.history).toEqual([validHistory]);
  expect(record.workspace.favorites).toEqual([]);
  expect(record.workspace.frames).toEqual([]);
});

it('resets safely for incompatible, malformed, or invalid storage', () => {
  expect(() => deserializeWorkspace('{')).not.toThrow();
  expect(deserializeWorkspace('{')).toEqual(emptyWorkspace());
  expect(deserializeWorkspace(JSON.stringify({ version: 2, workspace: {} }))).toEqual(
    emptyWorkspace(),
  );
  expect(
    deserializeWorkspace(JSON.stringify({ version: 1, workspace: { history: 'invalid' } })),
  ).toEqual(emptyWorkspace());
  expect(deserializeWorkspace(null)).toEqual(emptyWorkspace());
});
