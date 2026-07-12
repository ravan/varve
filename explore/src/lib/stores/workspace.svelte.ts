import type { ObservedSchema } from '$lib/logic/schema';
import type { GraphInspection } from '$lib/logic/graph';
import { classifyGql } from '$lib/logic/gql';
import type { Basis, ExecutionMode, QueryParameters } from '$lib/types';
import {
  areQueryParametersPersistable,
  addFavorite as addFavoriteTransition,
  addFrame as addFrameTransition,
  clearHistory as clearHistoryTransition,
  clearWorkspace as clearWorkspaceTransition,
  deleteFavorite as deleteFavoriteTransition,
  deserializeWorkspace,
  duplicateFavorite as duplicateFavoriteTransition,
  emptyWorkspace,
  observeExecution as observeExecutionTransition,
  recordHistory as recordHistoryTransition,
  removeFrame as removeFrameTransition,
  replaceFrame as replaceFrameTransition,
  serializeWorkspace,
  updateFavorite as updateFavoriteTransition,
  updateSettings as updateSettingsTransition,
  WORKSPACE_STORAGE_KEY,
  type ExecutionFrame,
  type Favorite,
  type HistoryEntry,
  type HistoryOutcome,
  type StorageLike,
  type WorkspaceSettings,
  type WorkspaceState,
} from '$lib/logic/workspace';

export function createWorkspaceStore(storage: StorageLike) {
  const restored = restore(storage);
  let frames = $state<readonly ExecutionFrame[]>(restored.frames);
  let history = $state<readonly HistoryEntry[]>(restored.history);
  let favorites = $state<readonly Favorite[]>(restored.favorites);
  let observedSchema = $state<ObservedSchema>(restored.observedSchema);
  let settings = $state<WorkspaceSettings>(restored.settings);
  let queryDraft = $state('');
  let queryModeOverride = $state<ExecutionMode | null>(null);
  let defaultReadBasis = $state<Basis | undefined>(undefined);
  let inspection = $state<GraphInspection | null>(null);
  let queryParametersDraft = $state<QueryParameters | null>({});

  function snapshot(): WorkspaceState {
    return { frames, history, favorites, observedSchema, settings };
  }

  function apply(state: WorkspaceState): void {
    frames = state.frames;
    history = state.history;
    favorites = state.favorites;
    observedSchema = state.observedSchema;
    settings = state.settings;
  }

  $effect(() => {
    try {
      storage.setItem(WORKSPACE_STORAGE_KEY, serializeWorkspace(snapshot()));
    } catch {
      // Storage can be unavailable or full; the in-memory workspace remains usable.
    }
  });

  return {
    get frames() {
      return frames;
    },
    get history() {
      return history;
    },
    get favorites() {
      return favorites;
    },
    get observedSchema() {
      return observedSchema;
    },
    get settings() {
      return settings;
    },
    get queryDraft() {
      return queryDraft;
    },
    get queryMode(): ExecutionMode {
      return queryModeOverride ?? classifyGql(queryDraft);
    },
    get queryModeOverridden(): boolean {
      return queryModeOverride !== null;
    },
    get defaultReadBasis() {
      return defaultReadBasis;
    },
    get inspection() {
      return inspection;
    },
    get queryParametersDraft() {
      return queryParametersDraft;
    },
    setQueryDraft(gql: string): void {
      queryDraft = gql;
    },
    setQueryMode(mode: ExecutionMode): void {
      queryModeOverride = mode;
    },
    resetQueryMode(): void {
      queryModeOverride = null;
    },
    startNewQuery(): void {
      queryDraft = '';
      queryModeOverride = null;
    },
    setDefaultReadBasis(basis: Basis): void {
      defaultReadBasis = basis;
    },
    inspectGraphElement(value: GraphInspection): void {
      inspection = value;
    },
    clearInspection(sourceId?: string): void {
      if (sourceId === undefined || inspection?.sourceId === sourceId) inspection = null;
    },
    setQueryParametersDraft(params: QueryParameters | null): void {
      queryParametersDraft =
        params === null || !areQueryParametersPersistable(params) ? null : params;
    },
    addFrame(frame: ExecutionFrame): void {
      apply(addFrameTransition(snapshot(), frame));
    },
    replaceFrame(frame: ExecutionFrame): void {
      apply(replaceFrameTransition(snapshot(), frame));
    },
    removeFrame(id: string): void {
      apply(removeFrameTransition(snapshot(), id));
    },
    recordHistory(entry: HistoryEntry): void {
      apply(recordHistoryTransition(snapshot(), entry));
    },
    clearHistory(confirmed: boolean): void {
      apply(clearHistoryTransition(snapshot(), confirmed));
    },
    addFavorite(favorite: Favorite): void {
      apply(addFavoriteTransition(snapshot(), favorite));
    },
    updateFavorite(id: string, changes: Partial<Omit<Favorite, 'id' | 'createdAt'>>): void {
      apply(updateFavoriteTransition(snapshot(), id, changes));
    },
    duplicateFavorite(id: string, duplicateId: string, timestamp: number): void {
      apply(duplicateFavoriteTransition(snapshot(), id, duplicateId, timestamp));
    },
    deleteFavorite(id: string): void {
      apply(deleteFavoriteTransition(snapshot(), id));
    },
    observeExecution(gql: string, outcome: HistoryOutcome, timestamp: number): void {
      apply(observeExecutionTransition(snapshot(), gql, outcome, timestamp));
    },
    updateSettings(changes: Partial<WorkspaceSettings>): void {
      apply(updateSettingsTransition(snapshot(), changes));
    },
    clearWorkspace(confirmed: boolean): void {
      apply(clearWorkspaceTransition(snapshot(), confirmed));
    },
  };
}

export type WorkspaceStore = ReturnType<typeof createWorkspaceStore>;

function restore(storage: StorageLike): WorkspaceState {
  try {
    return deserializeWorkspace(storage.getItem(WORKSPACE_STORAGE_KEY));
  } catch {
    return emptyWorkspace();
  }
}
