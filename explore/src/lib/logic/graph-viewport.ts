import type { EdgeSingular, NodeSingular } from 'cytoscape';

import type { GraphClustering } from './clustering';
import type { GraphExtraction } from './graph';
import {
  createGraphElements,
  createGraphStyles,
  createLayoutPolicy,
  type GraphTheme,
} from './graph-viewport-policy';

export interface GraphViewportNodeSelection {
  readonly kind: 'node';
  readonly id: string;
  readonly labels: readonly string[];
}

export interface GraphViewportRelationshipSelection {
  readonly kind: 'relationship';
  readonly id: string;
  readonly relationshipType?: string;
  readonly source: string;
  readonly target: string;
}

export type GraphViewportSelection =
  | GraphViewportNodeSelection
  | GraphViewportRelationshipSelection;

export interface MountGraphViewportOptions {
  readonly container: HTMLElement;
  readonly extraction: GraphExtraction;
  readonly motion: boolean;
  readonly onSelection: (selection: GraphViewportSelection) => void;
  readonly clustering?: GraphClustering;
}

export interface GraphViewportController {
  zoomBy(factor: number): void;
  fit(): void;
  relayout(): void;
  destroy(): void;
}

let fcoseRegistered = false;

export async function mountGraphViewport(
  options: MountGraphViewportOptions,
): Promise<GraphViewportController> {
  const [{ default: cytoscape }, { default: fcose }] = await Promise.all([
    import('cytoscape'),
    import('cytoscape-fcose'),
  ]);
  if (!fcoseRegistered) {
    cytoscape.use(fcose);
    fcoseRegistered = true;
  }

  const themeMedia = window.matchMedia('(prefers-color-scheme: dark)');
  const motionMedia = window.matchMedia('(prefers-reduced-motion: reduce)');
  const cy = cytoscape({
    container: options.container,
    elements: createGraphElements(options.extraction, options.clustering),
    style: createGraphStyles(currentTheme(themeMedia)),
    layout: { name: 'preset' },
    minZoom: 0.2,
    maxZoom: 5,
    boxSelectionEnabled: false,
  });
  let destroyed = false;

  const updateSemanticZoom = (): void => {
    if (destroyed) return;
    const zoom = cy.zoom();
    cy.batch(() => {
      cy.nodes()
        .not('.cluster-parent')
        .toggleClass('node-caption-hidden', zoom < 0.72);
      cy.edges().toggleClass('edge-caption-hidden', zoom < 1.05);
      cy.$('.focused').removeClass('node-caption-hidden').removeClass('edge-caption-hidden');
    });
  };

  const updateFocus = (): void => {
    if (destroyed) return;
    const selected = cy.$(':selected');
    cy.batch(() => {
      cy.elements().removeClass('focused dimmed');
      if (selected.empty()) return;

      let focus = selected;
      selected.nodes().forEach((node) => {
        focus = focus.union(node.closedNeighborhood());
      });
      selected.edges().forEach((edge) => {
        focus = focus
          .union(edge.source().closedNeighborhood())
          .union(edge.target().closedNeighborhood());
      });
      cy.elements().addClass('dimmed');
      focus.removeClass('dimmed').addClass('focused');
    });
    updateSemanticZoom();
  };

  const runLayout = (): void => {
    if (destroyed) return;
    const layout = cy.layout(
      createLayoutPolicy({
        nodeCount: cy.nodes().length,
        edgeCount: cy.edges().length,
        motion: options.motion,
        reducedMotion: motionMedia.matches,
      }),
    );
    layout.one('layoutstop', updateSemanticZoom);
    layout.run();
  };

  cy.on('zoom', updateSemanticZoom);
  cy.on('select unselect', updateFocus);
  cy.on('select', 'node', (event) => {
    const node = event.target as NodeSingular;
    options.onSelection({
      kind: 'node',
      id: node.id(),
      labels: asStrings(node.data('labels')),
    });
  });
  cy.on('select', 'edge', (event) => {
    const edge = event.target as EdgeSingular;
    const relationshipType = edge.data('relationshipType');
    options.onSelection({
      kind: 'relationship',
      id: edge.id(),
      ...(typeof relationshipType === 'string' && relationshipType !== ''
        ? { relationshipType }
        : {}),
      source: edge.source().id(),
      target: edge.target().id(),
    });
  });

  const applyTheme = (): void => {
    if (!destroyed) cy.style(createGraphStyles(currentTheme(themeMedia)));
  };
  const themeObserver = new MutationObserver(applyTheme);
  themeObserver.observe(document.documentElement, {
    attributes: true,
    attributeFilter: ['class', 'style'],
  });
  themeMedia.addEventListener('change', applyTheme);

  const resize = (): void => {
    if (!destroyed) cy.resize();
  };
  const resizeObserver =
    typeof ResizeObserver === 'undefined' ? null : new ResizeObserver(() => resize());
  resizeObserver?.observe(options.container);
  if (resizeObserver === null) window.addEventListener('resize', resize);

  runLayout();
  updateSemanticZoom();

  return {
    zoomBy(factor): void {
      if (destroyed || !Number.isFinite(factor) || factor <= 0) return;
      const level = Math.min(cy.maxZoom(), Math.max(cy.minZoom(), cy.zoom() * factor));
      cy.zoom({
        level,
        renderedPosition: { x: cy.width() / 2, y: cy.height() / 2 },
      });
    },
    fit(): void {
      if (!destroyed) cy.fit(undefined, 36);
    },
    relayout: runLayout,
    destroy(): void {
      if (destroyed) return;
      destroyed = true;
      themeObserver.disconnect();
      themeMedia.removeEventListener('change', applyTheme);
      resizeObserver?.disconnect();
      if (resizeObserver === null) window.removeEventListener('resize', resize);
      cy.destroy();
    },
  };
}

function currentTheme(media: MediaQueryList): GraphTheme {
  const root = document.documentElement;
  if (root.classList.contains('dark')) return 'dark';
  if (root.classList.contains('light')) return 'light';
  return media.matches ? 'dark' : 'light';
}

function asStrings(value: unknown): string[] {
  return Array.isArray(value)
    ? value.filter((item): item is string => typeof item === 'string')
    : [];
}
