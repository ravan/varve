import type { ExecutionMode, QueryParameters } from '../types';
import { extractQueryShape } from './gql';
import { extractObservedSchema, mergeObservedSchema, type ObservedSchema } from './schema';

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
    frames: state.frames.map(safeFrame),
    history: state.history.map((entry) => safeHistoryEntry(entry)),
    favorites: state.favorites.map((favorite) => safeFavorite(favorite)),
    observedSchema: cloneSafeValue(state.observedSchema),
    settings: safeSettings(state.settings),
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
    if (!isRecord(record) || record.version !== WORKSPACE_STORAGE_VERSION) return emptyWorkspace();
    if (!isWorkspace(record.workspace)) return emptyWorkspace();
    return freezeWorkspace({
      frames: limitFrames(record.workspace.frames.map(safeFrame)),
      history: record.workspace.history.slice(0, HISTORY_LIMIT).map(safeHistoryEntry),
      favorites: record.workspace.favorites.map(safeFavorite),
      observedSchema: cloneSafeValue(record.workspace.observedSchema) as ObservedSchema,
      settings: safeSettings(record.workspace.settings),
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

function safeFrame(frame: ExecutionFrame): ExecutionFrame {
  return {
    id: frame.id,
    gql: frame.gql,
    mode: frame.mode,
    params: cloneSafeValue(frame.params) as QueryParameters,
    parameterSummary: frame.parameterSummary,
    state: frame.state,
    startedAt: frame.startedAt,
    pinned: frame.pinned,
    ...(frame.finishedAt === undefined ? {} : { finishedAt: frame.finishedAt }),
    ...(frame.durationMs === undefined ? {} : { durationMs: frame.durationMs }),
    ...(frame.state === 'running' || frame.response === undefined
      ? {}
      : { response: cloneSafeValue(frame.response) }),
  };
}

function safeHistoryEntry(entry: HistoryEntry): HistoryEntry {
  return {
    gql: entry.gql,
    mode: entry.mode,
    params: cloneSafeValue(entry.params) as QueryParameters,
    finishedAt: entry.finishedAt,
    durationMs: entry.durationMs,
    rowCount: entry.rowCount,
    effectCount: entry.effectCount,
    outcome: entry.outcome,
    runCount: entry.runCount,
  };
}

function safeFavorite(favorite: Favorite): Favorite {
  const safe: Favorite = {
    id: favorite.id,
    name: favorite.name,
    gql: favorite.gql,
    mode: favorite.mode,
    params: cloneSafeValue(favorite.params) as QueryParameters,
    createdAt: favorite.createdAt,
    updatedAt: favorite.updatedAt,
    ...(favorite.notes === undefined ? {} : { notes: favorite.notes }),
  };
  return safe;
}

function safeSettings(settings: WorkspaceSettings): WorkspaceSettings {
  return {
    theme: settings.theme,
    graphMotion: settings.graphMotion,
    defaultResultTab: settings.defaultResultTab,
    historyEnabled: settings.historyEnabled,
    confirmBeforeClear: settings.confirmBeforeClear,
  };
}

function cloneSafeValue(value: unknown, ancestors = new Set<object>()): unknown {
  if (value === null || typeof value !== 'object') return value;
  if (ancestors.has(value)) throw new TypeError('Cannot persist circular workspace data');
  ancestors.add(value);

  let result: unknown;
  if (Array.isArray(value)) {
    result = value.map((item) => cloneSafeValue(item, ancestors));
  } else {
    result = Object.fromEntries(
      Object.entries(value)
        .filter(
          ([key, item]) =>
            !isSecretOrRawKey(key) &&
            item !== undefined &&
            typeof item !== 'function' &&
            typeof item !== 'symbol',
        )
        .map(([key, item]) => [key, cloneSafeValue(item, ancestors)]),
    );
  }
  ancestors.delete(value);
  return result;
}

function isSecretOrRawKey(key: string): boolean {
  const normalized = key.replaceAll('_', '').replaceAll('-', '').toLowerCase();
  return normalized === 'raw' || normalized === 'rawresponse' || normalized.includes('token');
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

function isWorkspace(value: unknown): value is WorkspaceState {
  if (!isRecord(value)) return false;
  if (
    !Array.isArray(value.frames) ||
    !Array.isArray(value.history) ||
    !Array.isArray(value.favorites) ||
    !isObservedSchema(value.observedSchema) ||
    !isSettings(value.settings)
  ) {
    return false;
  }
  return (
    value.frames.every(isFrame) &&
    value.history.every(isHistoryEntry) &&
    value.favorites.every(isFavorite)
  );
}

function isFrame(value: unknown): value is ExecutionFrame {
  return (
    isRecord(value) &&
    typeof value.id === 'string' &&
    typeof value.gql === 'string' &&
    isMode(value.mode) &&
    isRecord(value.params) &&
    typeof value.parameterSummary === 'string' &&
    isFrameState(value.state) &&
    isFiniteNumber(value.startedAt) &&
    (value.finishedAt === undefined || isFiniteNumber(value.finishedAt)) &&
    (value.durationMs === undefined || isNonNegativeNumber(value.durationMs)) &&
    typeof value.pinned === 'boolean'
  );
}

function isHistoryEntry(value: unknown): value is HistoryEntry {
  return (
    isRecord(value) &&
    typeof value.gql === 'string' &&
    isMode(value.mode) &&
    isRecord(value.params) &&
    isFiniteNumber(value.finishedAt) &&
    isNonNegativeNumber(value.durationMs) &&
    isNonNegativeInteger(value.rowCount) &&
    isNonNegativeInteger(value.effectCount) &&
    isHistoryOutcome(value.outcome) &&
    isPositiveInteger(value.runCount)
  );
}

function isFavorite(value: unknown): value is Favorite {
  return (
    isRecord(value) &&
    typeof value.id === 'string' &&
    typeof value.name === 'string' &&
    typeof value.gql === 'string' &&
    isMode(value.mode) &&
    isRecord(value.params) &&
    (value.notes === undefined || typeof value.notes === 'string') &&
    isFiniteNumber(value.createdAt) &&
    isFiniteNumber(value.updatedAt)
  );
}

function isSettings(value: unknown): value is WorkspaceSettings {
  return (
    isRecord(value) &&
    (value.theme === 'system' || value.theme === 'light' || value.theme === 'dark') &&
    typeof value.graphMotion === 'boolean' &&
    (value.defaultResultTab === 'graph' ||
      value.defaultResultTab === 'table' ||
      value.defaultResultTab === 'raw') &&
    typeof value.historyEnabled === 'boolean' &&
    typeof value.confirmBeforeClear === 'boolean'
  );
}

function isObservedSchema(value: unknown): value is ObservedSchema {
  if (!isRecord(value) || !isRecord(value.labels) || !isRecord(value.relationshipTypes)) {
    return false;
  }
  return (
    Object.values(value.labels).every(isObservedSchemaEntry) &&
    Object.values(value.relationshipTypes).every(isObservedSchemaEntry)
  );
}

function isObservedSchemaEntry(value: unknown): boolean {
  return (
    isRecord(value) &&
    isNonNegativeInteger(value.count) &&
    isFiniteNumber(value.firstSeen) &&
    isFiniteNumber(value.lastSeen) &&
    typeof value.starterGql === 'string'
  );
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
