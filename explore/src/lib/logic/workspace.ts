import type { Basis, ExecutionMode, JsonScalar, QueryParameters } from '../types';
import { extractQueryShape } from './gql';
import { extractObservedSchema, mergeObservedSchema, type ObservedSchema } from './schema';
import { parseBasis, validateParameters } from './validation';

export const WORKSPACE_STORAGE_VERSION = 1 as const;
export const WORKSPACE_STORAGE_KEY = 'varve-explorer-workspace';

const HISTORY_LIMIT = 100;
const COMPLETED_UNPINNED_FRAME_LIMIT = 25;

export interface StorageLike {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
  removeItem(key: string): void;
}

export type FrameState = 'running' | 'success' | 'error' | 'cancelled';
export type HistoryOutcome = Exclude<FrameState, 'running'>;
export type ResultTab = 'graph' | 'table' | 'raw';
export type ThemeSetting = 'system' | 'light' | 'dark';

export interface ExecutionFrame {
  readonly id: string;
  readonly gql: string;
  readonly mode: ExecutionMode;
  readonly params: QueryParameters;
  readonly readBasis?: Basis;
  readonly basisTimeoutMs?: number;
  readonly parameterSummary: string;
  readonly state: FrameState;
  readonly startedAt: number;
  readonly finishedAt?: number;
  readonly durationMs?: number;
  readonly pinned: boolean;
  readonly response?: unknown;
  readonly rawResponse?: unknown;
}

export interface HistoryEntry {
  readonly gql: string;
  readonly mode: ExecutionMode;
  readonly params: QueryParameters;
  readonly finishedAt: number;
  readonly durationMs: number;
  readonly rowCount: number;
  readonly effectCount: number;
  readonly outcome: HistoryOutcome;
  readonly runCount: number;
}

export interface Favorite {
  readonly id: string;
  readonly name: string;
  readonly gql: string;
  readonly mode: ExecutionMode;
  readonly params: QueryParameters;
  readonly notes?: string;
  readonly createdAt: number;
  readonly updatedAt: number;
}

export interface WorkspaceSettings {
  readonly theme: ThemeSetting;
  readonly graphMotion: boolean;
  readonly defaultResultTab: ResultTab;
  readonly historyEnabled: boolean;
  readonly confirmBeforeClear: boolean;
}

export interface WorkspaceState {
  readonly frames: readonly ExecutionFrame[];
  readonly history: readonly HistoryEntry[];
  readonly favorites: readonly Favorite[];
  readonly observedSchema: ObservedSchema;
  readonly settings: WorkspaceSettings;
}

interface PersistedWorkspaceV1 {
  readonly version: typeof WORKSPACE_STORAGE_VERSION;
  readonly workspace: unknown;
}

const DEFAULT_SETTINGS: WorkspaceSettings = {
  theme: 'system',
  graphMotion: true,
  defaultResultTab: 'graph',
  historyEnabled: true,
  confirmBeforeClear: true,
};

export function emptyWorkspace(): WorkspaceState {
  return freezeWorkspace({
    frames: [],
    history: [],
    favorites: [],
    observedSchema: { labels: {}, relationshipTypes: {} },
    settings: DEFAULT_SETTINGS,
  });
}

export function addFrame(state: WorkspaceState, frame: ExecutionFrame): WorkspaceState {
  const frames = limitFrames([...state.frames, cloneAndFreeze(frame)]);
  return freezeWorkspace({ ...state, frames });
}

export function replaceFrame(state: WorkspaceState, frame: ExecutionFrame): WorkspaceState {
  const index = state.frames.findIndex(({ id }) => id === frame.id);
  if (index === -1) return addFrame(state, frame);
  const frames = state.frames.map((current, currentIndex) =>
    currentIndex === index ? cloneAndFreeze(frame) : current,
  );
  return freezeWorkspace({ ...state, frames: limitFrames(frames) });
}

export function removeFrame(state: WorkspaceState, id: string): WorkspaceState {
  const frames = state.frames.filter((frame) => frame.id !== id);
  return frames.length === state.frames.length ? state : freezeWorkspace({ ...state, frames });
}

export function recordHistory(state: WorkspaceState, entry: HistoryEntry): WorkspaceState {
  if (!state.settings.historyEnabled) return state;

  const next = normalizeHistoryEntry(entry);
  const latest = state.history[0];
  const history =
    latest && sameSubmission(latest, next)
      ? [
          cloneAndFreeze({
            ...next,
            runCount: latest.runCount + Math.max(1, next.runCount),
          }),
          ...state.history.slice(1),
        ]
      : [next, ...state.history];

  return freezeWorkspace({ ...state, history: history.slice(0, HISTORY_LIMIT) });
}

export function clearHistory(state: WorkspaceState, confirmed: boolean): WorkspaceState {
  if (!confirmed || state.history.length === 0) return state;
  return freezeWorkspace({ ...state, history: [] });
}

export function addFavorite(state: WorkspaceState, input: Favorite): WorkspaceState {
  const favorite = normalizeFavorite(input);
  const withoutExisting = state.favorites.filter(({ id }) => id !== favorite.id);
  const withFavorite = freezeWorkspace({
    ...state,
    favorites: [...withoutExisting, favorite],
  });
  return observeFavorite(withFavorite, favorite);
}

export function updateFavorite(
  state: WorkspaceState,
  id: string,
  changes: Partial<Omit<Favorite, 'id' | 'createdAt'>>,
): WorkspaceState {
  const existing = state.favorites.find((favorite) => favorite.id === id);
  if (!existing) return state;

  const favorite = normalizeFavorite(
    withoutUndefined({
      ...existing,
      ...changes,
      id,
      createdAt: existing.createdAt,
    }) as unknown as Favorite,
  );
  const favorites = state.favorites.map((current) => (current.id === id ? favorite : current));
  const updated = freezeWorkspace({ ...state, favorites });
  return changes.gql === undefined ? updated : observeFavorite(updated, favorite);
}

export function duplicateFavorite(
  state: WorkspaceState,
  id: string,
  duplicateId: string,
  timestamp: number,
): WorkspaceState {
  const source = state.favorites.find((favorite) => favorite.id === id);
  if (!source) return state;
  return addFavorite(state, {
    ...source,
    id: duplicateId,
    name: `${source.name} copy`,
    createdAt: timestamp,
    updatedAt: timestamp,
  });
}

export function deleteFavorite(state: WorkspaceState, id: string): WorkspaceState {
  const favorites = state.favorites.filter((favorite) => favorite.id !== id);
  return favorites.length === state.favorites.length
    ? state
    : freezeWorkspace({ ...state, favorites });
}

export function observeExecution(
  state: WorkspaceState,
  gql: string,
  outcome: HistoryOutcome,
  timestamp: number,
): WorkspaceState {
  if (outcome !== 'success') return state;
  return observeGql(state, gql, timestamp);
}

export function updateSettings(
  state: WorkspaceState,
  changes: Partial<WorkspaceSettings>,
): WorkspaceState {
  return freezeWorkspace({
    ...state,
    settings: { ...state.settings, ...changes },
  });
}

export function clearWorkspace(state: WorkspaceState, confirmed: boolean): WorkspaceState {
  return confirmed ? emptyWorkspace() : state;
}

export function serializeWorkspace(state: WorkspaceState): string {
  const workspace = {
    frames: compactMap(state.frames, encodeFrame),
    history: compactMap(state.history, encodeHistoryEntry),
    favorites: compactMap(state.favorites, encodeFavorite),
    observedSchema: encodeObservedSchema(state.observedSchema),
    settings: encodeSettings(state.settings),
  };
  const record: PersistedWorkspaceV1 = {
    version: WORKSPACE_STORAGE_VERSION,
    workspace,
  };
  return JSON.stringify(record);
}

export function deserializeWorkspace(serialized: string | null): WorkspaceState {
  if (serialized === null) return emptyWorkspace();

  try {
    const record: unknown = JSON.parse(serialized);
    if (containsForbiddenKey(record)) return emptyWorkspace();
    const workspace = decodePersistedWorkspace(record);
    if (workspace === null) return emptyWorkspace();
    return freezeWorkspace({
      ...workspace,
      frames: limitFrames([...workspace.frames]),
      history: workspace.history.slice(0, HISTORY_LIMIT),
    });
  } catch {
    return emptyWorkspace();
  }
}

function observeFavorite(state: WorkspaceState, favorite: Favorite): WorkspaceState {
  return observeGql(state, favorite.gql, favorite.updatedAt);
}

function observeGql(state: WorkspaceState, gql: string, timestamp: number): WorkspaceState {
  try {
    const observation = extractObservedSchema(extractQueryShape(gql), timestamp);
    if (
      Object.keys(observation.labels).length === 0 &&
      Object.keys(observation.relationshipTypes).length === 0
    ) {
      return state;
    }
    return freezeWorkspace({
      ...state,
      observedSchema: mergeObservedSchema(state.observedSchema, observation),
    });
  } catch {
    return state;
  }
}

function encodeFrame(frame: ExecutionFrame): ExecutionFrame | null {
  return decodeFrame({
    id: frame.id,
    gql: frame.gql,
    mode: frame.mode,
    params: frame.params,
    ...(frame.readBasis === undefined ? {} : { readBasis: frame.readBasis }),
    ...(frame.basisTimeoutMs === undefined ? {} : { basisTimeoutMs: frame.basisTimeoutMs }),
    parameterSummary: frame.parameterSummary,
    state: frame.state,
    startedAt: frame.startedAt,
    pinned: frame.pinned,
    ...(frame.finishedAt === undefined ? {} : { finishedAt: frame.finishedAt }),
    ...(frame.durationMs === undefined ? {} : { durationMs: frame.durationMs }),
  });
}

function encodeHistoryEntry(entry: HistoryEntry): HistoryEntry | null {
  return decodeHistoryEntry({
    gql: entry.gql,
    mode: entry.mode,
    params: entry.params,
    finishedAt: entry.finishedAt,
    durationMs: entry.durationMs,
    rowCount: entry.rowCount,
    effectCount: entry.effectCount,
    outcome: entry.outcome,
    runCount: entry.runCount,
  });
}

function encodeFavorite(favorite: Favorite): Favorite | null {
  return decodeFavorite({
    id: favorite.id,
    name: favorite.name,
    gql: favorite.gql,
    mode: favorite.mode,
    params: favorite.params,
    createdAt: favorite.createdAt,
    updatedAt: favorite.updatedAt,
    ...(favorite.notes === undefined ? {} : { notes: favorite.notes }),
  });
}

function encodeSettings(settings: WorkspaceSettings): WorkspaceSettings {
  return (
    decodeSettings({
      theme: settings.theme,
      graphMotion: settings.graphMotion,
      defaultResultTab: settings.defaultResultTab,
      historyEnabled: settings.historyEnabled,
      confirmBeforeClear: settings.confirmBeforeClear,
    }) ?? DEFAULT_SETTINGS
  );
}

function encodeObservedSchema(schema: ObservedSchema): ObservedSchema {
  return {
    labels: encodeObservedRecord(schema.labels),
    relationshipTypes: encodeObservedRecord(schema.relationshipTypes),
  };
}

function encodeObservedRecord(
  source: Readonly<Record<string, unknown>>,
): Record<string, ObservedSchemaEntryValue> {
  const entries: [string, ObservedSchemaEntryValue][] = [];
  for (const [name, value] of Object.entries(source)) {
    if (isForbiddenKey(name)) continue;
    const entry = decodeObservedSchemaEntry(value);
    if (entry !== null) entries.push([name, entry]);
  }
  return createSafeRecord(entries);
}

interface ObservedSchemaEntryValue {
  readonly count: number;
  readonly firstSeen: number;
  readonly lastSeen: number;
  readonly starterGql: string;
}

function decodePersistedWorkspace(record: unknown): WorkspaceState | null {
  if (!hasExactKeys(record, ['version', 'workspace'])) return null;
  if (record.version !== WORKSPACE_STORAGE_VERSION || !isRecord(record.workspace)) return null;
  if (
    !hasExactKeys(record.workspace, [
      'frames',
      'history',
      'favorites',
      'observedSchema',
      'settings',
    ])
  ) {
    return null;
  }

  const frames = decodeArray(record.workspace.frames, decodeFrame);
  const history = decodeArray(record.workspace.history, decodeHistoryEntry);
  const favorites = decodeArray(record.workspace.favorites, decodeFavorite);
  const observedSchema = decodeObservedSchema(record.workspace.observedSchema);
  const settings = decodeSettings(record.workspace.settings);
  if (
    frames === null ||
    history === null ||
    favorites === null ||
    observedSchema === null ||
    settings === null
  ) {
    return null;
  }

  return { frames, history, favorites, observedSchema, settings };
}

function decodeFrame(value: unknown): ExecutionFrame | null {
  if (
    !hasExactKeys(
      value,
      ['id', 'gql', 'mode', 'params', 'parameterSummary', 'state', 'startedAt', 'pinned'],
      ['readBasis', 'basisTimeoutMs', 'finishedAt', 'durationMs'],
    )
  ) {
    return null;
  }
  const params = decodeQueryParameters(value.params);
  const readBasis = value.readBasis === undefined ? undefined : decodeBasis(value.readBasis);
  if (
    params === null ||
    readBasis === null ||
    typeof value.id !== 'string' ||
    typeof value.gql !== 'string' ||
    !isMode(value.mode) ||
    typeof value.parameterSummary !== 'string' ||
    !isFrameState(value.state) ||
    !isFiniteNumber(value.startedAt) ||
    (value.basisTimeoutMs !== undefined && !isPositiveInteger(value.basisTimeoutMs)) ||
    (value.mode === 'write' &&
      (value.readBasis !== undefined || value.basisTimeoutMs !== undefined)) ||
    (value.finishedAt !== undefined && !isFiniteNumber(value.finishedAt)) ||
    (value.durationMs !== undefined && !isNonNegativeNumber(value.durationMs)) ||
    typeof value.pinned !== 'boolean'
  ) {
    return null;
  }

  return {
    id: value.id,
    gql: value.gql,
    mode: value.mode,
    params,
    ...(readBasis === undefined ? {} : { readBasis }),
    ...(value.basisTimeoutMs === undefined ? {} : { basisTimeoutMs: value.basisTimeoutMs }),
    parameterSummary: value.parameterSummary,
    state: value.state,
    startedAt: value.startedAt,
    pinned: value.pinned,
    ...(value.finishedAt === undefined ? {} : { finishedAt: value.finishedAt }),
    ...(value.durationMs === undefined ? {} : { durationMs: value.durationMs }),
  };
}

function decodeBasis(value: unknown): Basis | null {
  if (typeof value !== 'number' && typeof value !== 'string') return null;
  const result = parseBasis(String(value));
  return result.ok && result.value !== undefined ? result.value : null;
}

function decodeHistoryEntry(value: unknown): HistoryEntry | null {
  if (
    !hasExactKeys(value, [
      'gql',
      'mode',
      'params',
      'finishedAt',
      'durationMs',
      'rowCount',
      'effectCount',
      'outcome',
      'runCount',
    ])
  ) {
    return null;
  }
  const params = decodeQueryParameters(value.params);
  if (
    params === null ||
    typeof value.gql !== 'string' ||
    !isMode(value.mode) ||
    !isFiniteNumber(value.finishedAt) ||
    !isNonNegativeNumber(value.durationMs) ||
    !isNonNegativeInteger(value.rowCount) ||
    !isNonNegativeInteger(value.effectCount) ||
    !isHistoryOutcome(value.outcome) ||
    !isPositiveInteger(value.runCount)
  ) {
    return null;
  }
  return {
    gql: value.gql,
    mode: value.mode,
    params,
    finishedAt: value.finishedAt,
    durationMs: value.durationMs,
    rowCount: value.rowCount,
    effectCount: value.effectCount,
    outcome: value.outcome,
    runCount: value.runCount,
  };
}

function decodeFavorite(value: unknown): Favorite | null {
  if (
    !hasExactKeys(
      value,
      ['id', 'name', 'gql', 'mode', 'params', 'createdAt', 'updatedAt'],
      ['notes'],
    )
  ) {
    return null;
  }
  const params = decodeQueryParameters(value.params);
  if (
    params === null ||
    typeof value.id !== 'string' ||
    typeof value.name !== 'string' ||
    typeof value.gql !== 'string' ||
    !isMode(value.mode) ||
    (value.notes !== undefined && typeof value.notes !== 'string') ||
    !isFiniteNumber(value.createdAt) ||
    !isFiniteNumber(value.updatedAt)
  ) {
    return null;
  }
  return {
    id: value.id,
    name: value.name,
    gql: value.gql,
    mode: value.mode,
    params,
    createdAt: value.createdAt,
    updatedAt: value.updatedAt,
    ...(value.notes === undefined ? {} : { notes: value.notes }),
  };
}

function decodeSettings(value: unknown): WorkspaceSettings | null {
  if (
    !hasExactKeys(value, [
      'theme',
      'graphMotion',
      'defaultResultTab',
      'historyEnabled',
      'confirmBeforeClear',
    ]) ||
    (value.theme !== 'system' && value.theme !== 'light' && value.theme !== 'dark') ||
    typeof value.graphMotion !== 'boolean' ||
    (value.defaultResultTab !== 'graph' &&
      value.defaultResultTab !== 'table' &&
      value.defaultResultTab !== 'raw') ||
    typeof value.historyEnabled !== 'boolean' ||
    typeof value.confirmBeforeClear !== 'boolean'
  ) {
    return null;
  }
  return {
    theme: value.theme,
    graphMotion: value.graphMotion,
    defaultResultTab: value.defaultResultTab,
    historyEnabled: value.historyEnabled,
    confirmBeforeClear: value.confirmBeforeClear,
  };
}

function decodeObservedSchema(value: unknown): ObservedSchema | null {
  if (!hasExactKeys(value, ['labels', 'relationshipTypes'])) return null;
  const labels = decodeObservedRecord(value.labels);
  const relationshipTypes = decodeObservedRecord(value.relationshipTypes);
  return labels === null || relationshipTypes === null ? null : { labels, relationshipTypes };
}

function decodeObservedRecord(value: unknown): Record<string, ObservedSchemaEntryValue> | null {
  if (!isRecord(value) || !hasSafePrototype(value)) return null;
  const entries: [string, ObservedSchemaEntryValue][] = [];
  for (const [name, entryValue] of Object.entries(value)) {
    if (isForbiddenKey(name)) return null;
    const entry = decodeObservedSchemaEntry(entryValue);
    if (entry === null) return null;
    entries.push([name, entry]);
  }
  return createSafeRecord(entries);
}

function decodeObservedSchemaEntry(value: unknown): ObservedSchemaEntryValue | null {
  if (
    !hasExactKeys(value, ['count', 'firstSeen', 'lastSeen', 'starterGql']) ||
    !isNonNegativeInteger(value.count) ||
    !isFiniteNumber(value.firstSeen) ||
    !isFiniteNumber(value.lastSeen) ||
    typeof value.starterGql !== 'string'
  ) {
    return null;
  }
  return {
    count: value.count,
    firstSeen: value.firstSeen,
    lastSeen: value.lastSeen,
    starterGql: value.starterGql,
  };
}

function decodeQueryParameters(value: unknown): QueryParameters | null {
  if (
    !isRecord(value) ||
    !hasSafePrototype(value) ||
    containsForbiddenKey(value) ||
    !Object.values(value).every(isParameterScalarShape)
  ) {
    return null;
  }

  let result: ReturnType<typeof validateParameters>;
  try {
    result = validateParameters(JSON.stringify(value));
  } catch {
    return null;
  }
  if (!result.ok) return null;
  return createSafeRecord(
    Object.entries(result.value).map(([key, scalar]) => [key, cloneJsonScalar(scalar)]),
  );
}

function isParameterScalarShape(value: unknown): boolean {
  if (value === null || typeof value === 'boolean' || typeof value === 'string') {
    return true;
  }
  if (typeof value === 'number') {
    return Number.isFinite(value) && (!Number.isInteger(value) || Number.isSafeInteger(value));
  }
  return (
    hasExactKeys(value, ['$bytes']) && hasSafePrototype(value) && typeof value.$bytes === 'string'
  );
}

function cloneJsonScalar(value: JsonScalar): JsonScalar {
  return typeof value === 'object' && value !== null
    ? (createSafeRecord([['$bytes', value.$bytes]]) as { $bytes: string })
    : value;
}

function freezeWorkspace(state: WorkspaceState): WorkspaceState {
  return cloneAndFreeze(state);
}

function cloneAndFreeze<T>(value: T, seen = new WeakMap<object, unknown>()): T {
  if (value === null || typeof value !== 'object') return value;
  const existing = seen.get(value);
  if (existing !== undefined) return existing as T;

  const clone: unknown = Array.isArray(value) ? [] : {};
  seen.set(value, clone);
  if (Array.isArray(value)) {
    for (const item of value) (clone as unknown[]).push(cloneAndFreeze(item, seen));
  } else {
    for (const [key, item] of Object.entries(value)) {
      (clone as Record<string, unknown>)[key] = cloneAndFreeze(item, seen);
    }
  }
  return Object.freeze(clone) as T;
}

function normalizeHistoryEntry(entry: HistoryEntry): HistoryEntry {
  return cloneAndFreeze({ ...entry, runCount: Math.max(1, entry.runCount) });
}

function normalizeFavorite(input: Favorite): Favorite {
  return cloneAndFreeze(
    withoutUndefined({
      id: input.id,
      name: input.name,
      gql: input.gql,
      mode: input.mode,
      params: input.params,
      notes: input.notes,
      createdAt: input.createdAt,
      updatedAt: input.updatedAt,
    }) as unknown as Favorite,
  );
}

function withoutUndefined(value: Record<string, unknown>): Record<string, unknown> {
  return Object.fromEntries(Object.entries(value).filter(([, item]) => item !== undefined));
}

function sameSubmission(left: HistoryEntry, right: HistoryEntry): boolean {
  return (
    left.gql === right.gql &&
    left.mode === right.mode &&
    canonicalJson(left.params) === canonicalJson(right.params)
  );
}

function canonicalJson(value: unknown): string {
  if (value === null || typeof value !== 'object') return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`;
  return `{${Object.entries(value)
    .sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0))
    .map(([key, item]) => `${JSON.stringify(key)}:${canonicalJson(item)}`)
    .join(',')}}`;
}

function completedUnpinnedCount(frames: readonly ExecutionFrame[]): number {
  return frames.filter(isCompletedUnpinned).length;
}

function limitFrames(input: ExecutionFrame[]): ExecutionFrame[] {
  const frames = [...input];
  while (completedUnpinnedCount(frames) > COMPLETED_UNPINNED_FRAME_LIMIT) {
    const candidate = frames.reduce(
      (oldest, current, index) => {
        if (!isCompletedUnpinned(current)) return oldest;
        if (oldest === null || current.startedAt < frames[oldest].startedAt) return index;
        return oldest;
      },
      null as number | null,
    );
    if (candidate === null) break;
    frames.splice(candidate, 1);
  }
  return frames;
}

function isCompletedUnpinned(frame: ExecutionFrame): boolean {
  return frame.state !== 'running' && !frame.pinned;
}

function compactMap<T, U>(values: readonly T[], encode: (value: T) => U | null): U[] {
  const result: U[] = [];
  for (const value of values) {
    const encoded = encode(value);
    if (encoded !== null) result.push(encoded);
  }
  return result;
}

function decodeArray<T>(value: unknown, decode: (item: unknown) => T | null): T[] | null {
  if (!Array.isArray(value)) return null;
  const result: T[] = [];
  for (const item of value) {
    const decoded = decode(item);
    if (decoded === null) return null;
    result.push(decoded);
  }
  return result;
}

function containsForbiddenKey(value: unknown): boolean {
  if (Array.isArray(value)) return value.some(containsForbiddenKey);
  if (!isRecord(value)) return false;
  return Object.entries(value).some(
    ([key, item]) => isForbiddenKey(key) || containsForbiddenKey(item),
  );
}

function isForbiddenKey(key: string): boolean {
  const lower = key.toLowerCase();
  if (lower === '__proto__' || lower === 'constructor' || lower === 'prototype') return true;
  const normalized = lower.replaceAll('_', '').replaceAll('-', '');
  return (
    normalized === 'raw' ||
    normalized === 'rawresponse' ||
    normalized.includes('token') ||
    normalized.includes('authorization') ||
    normalized.includes('credential') ||
    normalized.includes('secret')
  );
}

function hasExactKeys(
  value: unknown,
  required: readonly string[],
  optional: readonly string[] = [],
): value is Record<string, unknown> {
  if (!isRecord(value) || !hasSafePrototype(value)) return false;
  const keys = Object.keys(value);
  const allowed = new Set([...required, ...optional]);
  return (
    required.every((key) => Object.hasOwn(value, key)) && keys.every((key) => allowed.has(key))
  );
}

function hasSafePrototype(value: object): boolean {
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

function createSafeRecord<T>(entries: readonly (readonly [string, T])[]): Record<string, T> {
  const record = Object.create(null) as Record<string, T>;
  for (const [key, value] of entries) {
    Object.defineProperty(record, key, {
      configurable: false,
      enumerable: true,
      value,
      writable: false,
    });
  }
  return record;
}

function isMode(value: unknown): value is ExecutionMode {
  return value === 'read' || value === 'write';
}

function isFrameState(value: unknown): value is FrameState {
  return value === 'running' || isHistoryOutcome(value);
}

function isHistoryOutcome(value: unknown): value is HistoryOutcome {
  return value === 'success' || value === 'error' || value === 'cancelled';
}

function isFiniteNumber(value: unknown): value is number {
  return typeof value === 'number' && Number.isFinite(value);
}

function isNonNegativeNumber(value: unknown): value is number {
  return isFiniteNumber(value) && value >= 0;
}

function isNonNegativeInteger(value: unknown): value is number {
  return Number.isSafeInteger(value) && (value as number) >= 0;
}

function isPositiveInteger(value: unknown): value is number {
  return Number.isSafeInteger(value) && (value as number) > 0;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}
