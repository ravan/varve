<script lang="ts">
  import { browser } from '$app/environment';
  import { Alert, AlertDescription, AlertTitle } from '$lib/components/ui/alert';
  import { Badge } from '$lib/components/ui/badge';
  import { Button } from '$lib/components/ui/button';
  import * as Tooltip from '$lib/components/ui/tooltip';
  import type { GraphExtraction, GraphInspection } from '$lib/logic/graph';
  import { isCanonicalBytesObject, type NormalizedCell, type NormalizedRow } from '$lib/logic/results';
  import type { WorkspaceStore } from '$lib/stores/workspace.svelte';
  import Maximize from '@lucide/svelte/icons/maximize';
  import Minus from '@lucide/svelte/icons/minus';
  import Plus from '@lucide/svelte/icons/plus';
  import RotateCcw from '@lucide/svelte/icons/rotate-ccw';
  import type { Core, EdgeSingular, ElementDefinition, NodeSingular } from 'cytoscape';

  const GRAPH_ELEMENT_CAP = 1_000;

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
  let graph: Core | null = null;

  let normalizedElements = $derived(capElements(extraction));

  $effect(() => {
    const host = graphHost;
    const elements = normalizedElements;
    const shouldAnimate = motion;
    const currentSourceId = sourceId;
    if (!browser || host === null || !extraction.available) return;

    let disposed = false;
    void import('cytoscape').then(({ default: cytoscape }) => {
      if (disposed) return;
      const reduceMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
      const instance = cytoscape({
        container: host,
        elements,
        layout: layoutOptions(shouldAnimate && !reduceMotion),
        minZoom: 0.1,
        maxZoom: 4,
        wheelSensitivity: 0.22,
        style: [
          {
            selector: 'node',
            style: {
              'background-color': '#d97706',
              'border-color': '#fbbf24',
              'border-width': 2,
              color: '#f8fafc',
              label: 'data(label)',
              'font-size': 10,
              'font-weight': 600,
              shape: 'round-rectangle',
              padding: '10px',
              'text-max-width': '110px',
              'text-overflow-wrap': 'anywhere',
              'text-valign': 'center',
              'text-halign': 'center',
              width: 'label',
              height: 'label',
            },
          },
          {
            selector: 'edge',
            style: {
              width: 2,
              'line-color': '#0891b2',
              'target-arrow-color': '#0891b2',
              'target-arrow-shape': 'triangle',
              'curve-style': 'bezier',
              label: 'data(label)',
              color: '#64748b',
              'font-size': 9,
              'text-background-color': '#f8fafc',
              'text-background-opacity': 0.86,
              'text-background-padding': '3px',
            },
          },
          {
            selector: ':selected',
            style: {
              'border-color': '#0ea5e9',
              'border-width': 4,
              'line-color': '#0ea5e9',
              'target-arrow-color': '#0ea5e9',
            },
          },
        ],
      });
      graph = instance;
      instance.on('select', 'node', (event) => inspectNode(event.target as NodeSingular));
      instance.on('select', 'edge', (event) => inspectEdge(event.target as EdgeSingular));
    });

    return () => {
      disposed = true;
      graph?.destroy();
      graph = null;
      workspace.clearInspection(currentSourceId);
    };
  });

  function capElements(value: GraphExtraction): ElementDefinition[] {
    const nodes = value.nodes.slice(0, GRAPH_ELEMENT_CAP);
    const nodeIds = new Set(nodes.map(({ id }) => id));
    const room = Math.max(0, GRAPH_ELEMENT_CAP - nodes.length);
    const edges = value.edges
      .filter(({ source, target }) => nodeIds.has(source) && nodeIds.has(target))
      .slice(0, room);
    return [
      ...nodes.map((node) => ({
        group: 'nodes' as const,
        data: {
          id: node.id,
          label: node.labels.length > 0 ? node.labels.join(' · ') : node.id,
          labels: [...node.labels],
        },
      })),
      ...edges.map((edge) => ({
        group: 'edges' as const,
        data: {
          id: edge.id,
          source: edge.source,
          target: edge.target,
          label: edge.type ?? '',
          relationshipType: edge.type,
        },
      })),
    ];
  }

  function layoutOptions(animate: boolean) {
    return {
      name: 'cose' as const,
      animate,
      animationDuration: animate ? 450 : 0,
      fit: true,
      padding: 32,
      randomize: true,
    };
  }

  function inspectNode(node: NodeSingular): void {
    const id = node.id();
    workspace.inspectGraphElement({
      sourceId,
      kind: 'node',
      id,
      labels: asStrings(node.data('labels')),
      inferred: true,
      ...relatedRows(id),
    });
  }

  function inspectEdge(edge: EdgeSingular): void {
    const id = edge.id();
    const relationshipType = edge.data('relationshipType');
    const inspection: GraphInspection = {
      sourceId,
      kind: 'relationship',
      id,
      labels: [],
      ...(typeof relationshipType === 'string' && relationshipType !== ''
        ? { relationshipType }
        : {}),
      source: edge.source().id(),
      target: edge.target().id(),
      inferred: true,
      ...relatedRows(id),
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

  function asStrings(value: unknown): string[] {
    return Array.isArray(value) ? value.filter((item): item is string => typeof item === 'string') : [];
  }

  function zoom(factor: number): void {
    if (graph === null) return;
    graph.zoom({ level: graph.zoom() * factor, renderedPosition: { x: graph.width() / 2, y: graph.height() / 2 } });
  }

  function fitGraph(): void {
    graph?.fit(undefined, 32);
  }

  function resetLayout(): void {
    if (graph === null) return;
    const reduceMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
    graph.layout(layoutOptions(motion && !reduceMotion)).run();
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
        {#if extraction.truncated}<Badge variant="outline">Capped at {GRAPH_ELEMENT_CAP}</Badge>{/if}
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
              <Button {...props} variant="outline" size="icon-sm" aria-label="Reset layout" onclick={resetLayout}>
                <RotateCcw aria-hidden="true" />
              </Button>
            {/snippet}
          </Tooltip.Trigger>
          <Tooltip.Content>Reset layout</Tooltip.Content>
        </Tooltip.Root>
      </div>
    </div>
    <div bind:this={graphHost} class="graph-canvas" role="img" aria-label="Graph visualization"></div>
  </section>
{/if}
