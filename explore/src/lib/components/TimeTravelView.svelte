<script lang="ts">
  import { browser } from '$app/environment';
  import GqlEditor from '$lib/components/GqlEditor.svelte';
  import TimeIntervalPicker from '$lib/components/TimeIntervalPicker.svelte';
  import TimelineBar from '$lib/components/TimelineBar.svelte';
  import TimeTravelSettings from '$lib/components/TimeTravelSettings.svelte';
  import { Alert, AlertDescription, AlertTitle } from '$lib/components/ui/alert';
  import { Badge } from '$lib/components/ui/badge';
  import { Button } from '$lib/components/ui/button';
  import { computeClusters, type GroupingMode } from '$lib/logic/clustering';
  import { extractEntityGraph } from '$lib/logic/entity-graph';
  import { extractQueryShape } from '$lib/logic/gql';
  import { extractGraph, type GraphExtraction, type GraphInspection } from '$lib/logic/graph';
  import {
    mountGraphViewport,
    type GraphViewportController,
    type GraphViewportSelection,
  } from '$lib/logic/graph-viewport';
  import {
    isCanonicalBytesObject,
    normalizeQueryResponse,
    type NormalizedCell,
    type NormalizedRow,
  } from '$lib/logic/results';
  import {
    buildTimeTravelGql,
    clampTime,
    formatInstantWithDate,
    RELATIVE_INTERVALS,
    relativeRange,
    type TemporalAxis,
    type TimeRange,
  } from '$lib/logic/time-travel';
  import {
    DEFAULT_TIME_TRAVEL_PREFERENCES,
    loadTimeTravelPreferences,
    rememberRange,
    saveTimeTravelPreferences,
    type TimeTravelPreferences,
  } from '$lib/logic/time-travel-preferences';
  import { buildLabelStarterGql } from '$lib/logic/schema';
  import type { StorageLike } from '$lib/logic/workspace';
  import type { FetchLike } from '$lib/stores/connection.svelte';
  import type { WorkspaceStore } from '$lib/stores/workspace.svelte';
  import Play from '@lucide/svelte/icons/play';
  import Radio from '@lucide/svelte/icons/radio';
  import { onMount } from 'svelte';

  let {
    workspace,
    fetcher = fetch,
  }: {
    workspace: WorkspaceStore;
    fetcher?: FetchLike;
  } = $props();

  const memoryStorage: StorageLike = {
    getItem: () => null,
    setItem: () => undefined,
    removeItem: () => undefined,
  };
  const storage = browser ? window.localStorage : memoryStorage;
  const initialPreferences = loadTimeTravelPreferences(storage);
  const initialRange = relativeRange(RELATIVE_INTERVALS[4], Date.now());

  let preferences = $state<TimeTravelPreferences>(initialPreferences);
  let range = $state<TimeRange>(initialRange);
  let selectedMs = $state(initialRange.endMs);
  let filterDraft = $state(initialPreferences.filter);

  let graphHost = $state<HTMLDivElement | null>(null);
  let viewport: GraphViewportController | null = null;
  let componentMounted = false;
  let activeController: AbortController | null = null;

  let running = $state(false);
  let filterValid = $state(true);
  let queryError = $state<string | null>(null);
  let rows = $state<readonly NormalizedRow[]>([]);
  let extraction = $state<GraphExtraction | null>(null);
  let executedAtMs = $state<number | null>(null);

  let clustering = $derived(
    extraction === null || !extraction.available
      ? computeClusters([], 'none', 1)
      : computeClusters(extraction.nodes, preferences.grouping, preferences.clusterSize),
  );

  $effect(() => {
    saveTimeTravelPreferences(storage, preferences);
  });

  onMount(() => {
    componentMounted = true;
    void runQuery();

    return () => {
      componentMounted = false;
      activeController?.abort();
      activeController = null;
      viewport?.destroy();
      viewport = null;
      workspace.clearInspection('time-travel');
    };
  });

  $effect(() => {
    const host = graphHost;
    const currentExtraction = extraction;
    const currentClustering = clustering;
    const motion = workspace.settings.graphMotion;

    viewport?.destroy();
    viewport = null;
    if (host === null || currentExtraction === null || !currentExtraction.available) return;

    let disposed = false;
    void mountGraphViewport({
      container: host,
      extraction: currentExtraction,
      clustering: currentClustering,
      motion,
      onSelection: inspectSelection,
    }).then((controller) => {
      if (disposed || !componentMounted) controller.destroy();
      else viewport = controller;
    });

    return () => {
      disposed = true;
    };
  });

  async function runQuery(atMs: number = selectedMs): Promise<void> {
    if (!filterValid) {
      queryError = 'The topology filter is not valid GQL; fix it and rerun.';
      return;
    }
    const built = buildTimeTravelGql(filterDraft, atMs, preferences.axis);
    if (!built.ok) {
      queryError = built.error;
      return;
    }

    activeController?.abort();
    const controller = new AbortController();
    activeController = controller;
    running = true;
    queryError = null;
    preferences = { ...preferences, filter: filterDraft };

    try {
      const response = await fetcher('/api/varve/query', {
        method: 'POST',
        headers: { accept: 'application/json', 'content-type': 'application/json' },
        body: JSON.stringify({ gql: built.gql, params: {} }),
        signal: controller.signal,
      });
      const raw: unknown = await response.json();
      if (!componentMounted) return;
      if (!response.ok) {
        queryError = describeFailure(raw, response.status);
        workspace.observeExecution(filterDraft, 'error', Date.now());
        return;
      }

      const normalized = normalizeQueryResponse(raw);
      rows = normalized.rows;
      extraction = extractTopology(filterDraft, normalized.rows);
      executedAtMs = atMs;
      workspace.observeExecution(filterDraft, 'success', Date.now());
    } catch (cause) {
      if (!componentMounted || controller.signal.aborted) return;
      queryError =
        cause instanceof TypeError && (cause.message.includes('response') || cause.message.includes('JSON'))
          ? 'The target returned a malformed or unsupported response.'
          : 'Explorer could not reach Varve. Check the connection and retry.';
    } finally {
      if (activeController === controller) {
        activeController = null;
        running = false;
      }
    }
  }

  const GRAPH_LIMITS = { maxNodes: 2_000, maxEdges: 4_000 };

  // Named-path results prove topology exactly (graph.ts); everything else -
  // including Varve's expanded `RETURN n` entity columns - goes through the
  // engine-column extractor so label scans and a, r, b returns still render.
  function extractTopology(filter: string, resultRows: readonly NormalizedRow[]): GraphExtraction {
    try {
      const proven = extractGraph(extractQueryShape(filter), resultRows, GRAPH_LIMITS);
      if (proven.available) return proven;
    } catch {
      // Fall through to entity-column extraction.
    }
    return extractEntityGraph(resultRows, GRAPH_LIMITS);
  }

  function describeFailure(raw: unknown, status: number): string {
    if (status === 401) return 'Authentication required. Reconnect, then rerun the filter.';
    const record = typeof raw === 'object' && raw !== null ? (raw as Record<string, unknown>) : {};
    if (typeof record.message === 'string' && record.message.length > 0) return record.message;
    if (record.code === 'invalid_request') {
      return 'Varve rejected the query. Adjust the filter and rerun.';
    }
    return 'Varve could not complete the request. The filter remains available to retry.';
  }

  function selectTime(timeMs: number): void {
    selectedMs = timeMs;
    void runQuery(timeMs);
  }

  function zoomToRange(next: TimeRange): void {
    applyRange(next);
  }

  function applyRange(next: TimeRange): void {
    range = next;
    preferences = rememberRange(preferences, next);
    const clamped = clampTime(next, selectedMs);
    if (clamped !== selectedMs) selectTime(clamped);
  }

  function goLive(): void {
    const span = range.endMs - range.startMs;
    const now = Date.now();
    range = { startMs: now - span, endMs: now };
    selectTime(now);
  }

  function setGrouping(grouping: GroupingMode): void {
    preferences = { ...preferences, grouping };
  }

  function setClusterSize(clusterSize: number): void {
    preferences = { ...preferences, clusterSize };
  }

  function setAxis(axis: TemporalAxis): void {
    preferences = { ...preferences, axis };
    void runQuery();
  }

  function applyStarter(starter: string): void {
    filterDraft = `${starter} LIMIT 100`;
    void runQuery();
  }

  // Only label starters: Varve v1 matches nothing for unlabeled endpoints, so
  // a relationship starter without labels would always come back empty.
  let starterSuggestions = $derived(
    Object.keys(workspace.observedSchema.labels).map((label) => ({
      caption: `:${label}`,
      gql: buildLabelStarterGql(label),
    })),
  );

  function inspectSelection(selection: GraphViewportSelection): void {
    const inspection: GraphInspection = {
      sourceId: 'time-travel',
      kind: selection.kind,
      id: selection.id,
      labels: selection.kind === 'node' ? selection.labels : [],
      ...(selection.kind === 'relationship'
        ? {
            ...(selection.relationshipType === undefined
              ? {}
              : { relationshipType: selection.relationshipType }),
            source: selection.source,
            target: selection.target,
          }
        : {}),
      inferred: true,
      ...relatedRows(selection.id),
    };
    workspace.inspectGraphElement(inspection);
  }

  function relatedRows(
    identity: string,
  ): Pick<GraphInspection, 'relatedRowCount' | 'relatedValues'> {
    const matches = rows.filter((row) =>
      Object.values(row).some((cell) => containsIdentity(cell, identity)),
    );
    return {
      relatedRowCount: matches.length,
      relatedValues: matches
        .flatMap((row) => Object.entries(row).map(([column, value]) => ({ column, value })))
        .slice(0, 40),
    };
  }

  function containsIdentity(cell: NormalizedCell, identity: string): boolean {
    if (cell.kind === 'missing') return false;
    return valueContainsIdentity(cell.value, identity);
  }

  function valueContainsIdentity(value: unknown, identity: string): boolean {
    if (value === identity) return true;
    if (isCanonicalBytesObject(value)) return `bytes:${value.$bytes}` === identity;
    if (Array.isArray(value)) return value.some((item) => valueContainsIdentity(item, identity));
    if (typeof value !== 'object' || value === null) return false;
    return Object.values(value).some((item) => valueContainsIdentity(item, identity));
  }
</script>

<section class="flex min-h-0 w-full flex-1 flex-col gap-4" aria-labelledby="time-travel-heading">
  <div class="flex flex-wrap items-end justify-between gap-3">
    <div>
      <p class="text-primary text-xs font-semibold uppercase tracking-[0.18em]">Workspace</p>
      <h1 id="time-travel-heading" class="text-2xl font-semibold tracking-tight">Time travel</h1>
      <p class="text-muted-foreground mt-1 text-sm">
        Explore the topology as it was at any instant on the timeline.
      </p>
    </div>
    <TimeTravelSettings
      grouping={preferences.grouping}
      clusterSize={preferences.clusterSize}
      axis={preferences.axis}
      onGroupingChange={setGrouping}
      onClusterSizeChange={setClusterSize}
      onAxisChange={setAxis}
    />
  </div>

  <div class="overflow-hidden rounded-xl border bg-card shadow-sm">
    <div class="flex items-center justify-between border-b px-3 py-2">
      <span class="text-sm font-medium">Filter topology</span>
      <span class="text-muted-foreground font-mono text-xs">⌘/Ctrl + Enter</span>
    </div>
    <div class="flex items-stretch">
      <div class="min-w-0 flex-1">
        <GqlEditor
          value={filterDraft}
          onChange={(value) => (filterDraft = value)}
          onSubmit={() => void runQuery()}
          schema={() => workspace.observedSchema}
          ariaLabel="Topology filter GQL"
          placeholder="MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a, r, b LIMIT 100"
          compact={true}
          onValidation={(valid) => (filterValid = valid)}
        />
      </div>
      <Button
        class="m-2 self-center"
        disabled={running || !filterValid || filterDraft.trim().length === 0}
        onclick={() => void runQuery()}
      >
        <Play aria-hidden="true" />
        Go
      </Button>
    </div>
  </div>

  {#if queryError !== null}
    <Alert variant="destructive">
      <AlertTitle>Query failed</AlertTitle>
      <AlertDescription>{queryError}</AlertDescription>
    </Alert>
  {/if}

  <div class="flex min-h-0 flex-1 flex-col overflow-hidden rounded-xl border bg-card shadow-sm">
    <div class="flex flex-wrap items-center justify-between gap-2 border-b px-3 py-2">
      <div class="flex flex-wrap items-center gap-2">
        {#if extraction !== null && extraction.available}
          <Badge variant="secondary">{extraction.nodes.length} nodes</Badge>
          <Badge variant="secondary">{extraction.edges.length} relationships</Badge>
          {#if clustering.clusters.length > 0}
            <Badge variant="secondary">{clustering.clusters.length} groups</Badge>
          {/if}
          {#if extraction.truncated}
            <Badge variant="outline">
              Showing {extraction.nodes.length} of {extraction.totalNodes} nodes
            </Badge>
          {/if}
        {/if}
        {#if running}
          <Badge variant="outline">Loading…</Badge>
        {:else if executedAtMs !== null}
          <span class="text-muted-foreground text-xs">
            As of {formatInstantWithDate(executedAtMs)}
            ({preferences.axis === 'valid' ? 'valid time' : 'system time'})
          </span>
        {/if}
      </div>
      <div class="flex items-center gap-2">
        <TimeIntervalPicker {range} recentRanges={preferences.recentRanges} onApply={zoomToRange} />
        <Button variant="outline" size="sm" onclick={goLive}>
          <Radio aria-hidden="true" />
          Go live
        </Button>
      </div>
    </div>

    {#if extraction !== null && !extraction.available}
      <Alert class="m-3 w-auto">
        <AlertTitle>Graph unavailable</AlertTitle>
        <AlertDescription>
          {extraction.reason ?? 'Graph topology is unavailable for this filter.'}
        </AlertDescription>
      </Alert>
    {:else if extraction !== null && extraction.nodes.length === 0 && !running}
      <Alert class="m-3 w-auto">
        <AlertTitle>Nothing here at this instant</AlertTitle>
        <AlertDescription>
          <p>
            No topology matched the filter at the selected time. Travel the timeline, or start
            from something Varve has seen — label scans and typed relationships return data;
            unlabeled patterns do not.
          </p>
          {#if starterSuggestions.length > 0}
            <div class="mt-2 flex flex-wrap gap-1.5">
              {#each starterSuggestions as suggestion (suggestion.gql)}
                <Button
                  variant="outline"
                  size="sm"
                  class="font-mono text-xs"
                  onclick={() => applyStarter(suggestion.gql)}
                >
                  {suggestion.caption}
                </Button>
              {/each}
            </div>
          {/if}
        </AlertDescription>
      </Alert>
    {/if}
    <div
      bind:this={graphHost}
      class="graph-canvas time-travel-canvas"
      role="img"
      aria-label="Time travel graph visualization"
    ></div>

    <div class="border-t px-3 pb-1 pt-2">
      <TimelineBar {range} {selectedMs} onSelect={selectTime} onZoom={zoomToRange} />
    </div>
  </div>
</section>

<style>
  .time-travel-canvas {
    flex: 1;
    height: auto;
    min-height: 18rem;
  }
</style>
