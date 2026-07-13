<script lang="ts">
  import { Alert, AlertDescription, AlertTitle } from '$lib/components/ui/alert';
  import { Badge } from '$lib/components/ui/badge';
  import { Button } from '$lib/components/ui/button';
  import * as Tooltip from '$lib/components/ui/tooltip';
  import type { GraphExtraction, GraphInspection } from '$lib/logic/graph';
  import {
    mountGraphViewport,
    type GraphViewportController,
    type GraphViewportSelection,
  } from '$lib/logic/graph-viewport';
  import { isCanonicalBytesObject, type NormalizedCell, type NormalizedRow } from '$lib/logic/results';
  import type { WorkspaceStore } from '$lib/stores/workspace.svelte';
  import Maximize from '@lucide/svelte/icons/maximize';
  import Minus from '@lucide/svelte/icons/minus';
  import Plus from '@lucide/svelte/icons/plus';
  import RotateCcw from '@lucide/svelte/icons/rotate-ccw';
  import { onMount } from 'svelte';

  let {
    extraction,
    rows,
    sourceId,
    motion,
    workspace,
  }: {
    extraction: GraphExtraction;
    rows: readonly NormalizedRow[];
    sourceId: string;
    motion: boolean;
    workspace: WorkspaceStore;
  } = $props();

  let graphHost = $state<HTMLDivElement | null>(null);
  let viewport: GraphViewportController | null = null;

  onMount(() => {
    const host = graphHost;
    const currentSourceId = sourceId;
    if (host === null || !extraction.available) return;

    let disposed = false;
    void mountGraphViewport({
      container: host,
      extraction,
      motion,
      onSelection: inspectSelection,
    }).then((controller) => {
      if (disposed) controller.destroy();
      else viewport = controller;
    });

    return () => {
      disposed = true;
      viewport?.destroy();
      viewport = null;
      workspace.clearInspection(currentSourceId);
    };
  });

  function inspectSelection(selection: GraphViewportSelection): void {
    const inspection: GraphInspection = {
      sourceId,
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

  function relatedRows(identity: string): Pick<GraphInspection, 'relatedRowCount' | 'relatedValues'> {
    const matches = rows.filter((row) => Object.values(row).some((cell) => containsIdentity(cell, identity)));
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

  function zoom(factor: number): void {
    viewport?.zoomBy(factor);
  }

  function fitGraph(): void {
    viewport?.fit();
  }

  function relayout(): void {
    viewport?.relayout();
  }
</script>

{#if !extraction.available}
  <Alert>
    <AlertTitle>Graph unavailable</AlertTitle>
    <AlertDescription>{extraction.reason ?? 'Graph topology is unavailable for this result.'}</AlertDescription>
  </Alert>
{:else if extraction.nodes.length === 0}
  <Alert>
    <AlertTitle>No graph elements</AlertTitle>
    <AlertDescription>The returned rows did not contain graph elements that could be proven from the GQL.</AlertDescription>
  </Alert>
{:else}
  <section class="graph-result" aria-label="Interactive graph result">
    <div class="graph-toolbar">
      <div class="flex flex-wrap items-center gap-2">
        <Badge variant="secondary">{extraction.nodes.length} nodes</Badge>
        <Badge variant="secondary">{extraction.edges.length} relationships</Badge>
        {#if extraction.truncated}
          <Badge variant="outline">
            Showing {extraction.nodes.length} of {extraction.totalNodes} nodes · {extraction.edges.length} of
            {extraction.totalEdges} relationships
          </Badge>
        {/if}
      </div>
      <div class="flex items-center gap-1">
        <Tooltip.Root>
          <Tooltip.Trigger>
            {#snippet child({ props })}
              <Button {...props} variant="outline" size="icon-sm" aria-label="Zoom in" onclick={() => zoom(1.2)}>
                <Plus aria-hidden="true" />
              </Button>
            {/snippet}
          </Tooltip.Trigger>
          <Tooltip.Content>Zoom in</Tooltip.Content>
        </Tooltip.Root>
        <Tooltip.Root>
          <Tooltip.Trigger>
            {#snippet child({ props })}
              <Button {...props} variant="outline" size="icon-sm" aria-label="Zoom out" onclick={() => zoom(1 / 1.2)}>
                <Minus aria-hidden="true" />
              </Button>
            {/snippet}
          </Tooltip.Trigger>
          <Tooltip.Content>Zoom out</Tooltip.Content>
        </Tooltip.Root>
        <Tooltip.Root>
          <Tooltip.Trigger>
            {#snippet child({ props })}
              <Button {...props} variant="outline" size="icon-sm" aria-label="Fit graph" onclick={fitGraph}>
                <Maximize aria-hidden="true" />
              </Button>
            {/snippet}
          </Tooltip.Trigger>
          <Tooltip.Content>Fit graph</Tooltip.Content>
        </Tooltip.Root>
        <Tooltip.Root>
          <Tooltip.Trigger>
            {#snippet child({ props })}
              <Button {...props} variant="outline" size="icon-sm" aria-label="Relayout" onclick={relayout}>
                <RotateCcw aria-hidden="true" />
              </Button>
            {/snippet}
          </Tooltip.Trigger>
          <Tooltip.Content>Relayout</Tooltip.Content>
        </Tooltip.Root>
      </div>
    </div>
    <div bind:this={graphHost} class="graph-canvas" role="img" aria-label="Graph visualization"></div>
  </section>
{/if}
