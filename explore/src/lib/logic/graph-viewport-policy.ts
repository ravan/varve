import type { ElementDefinition, LayoutOptions, StylesheetJson } from 'cytoscape';

import type { GraphExtraction } from './graph';

export type GraphTheme = 'light' | 'dark';

export interface LayoutPolicyInput {
  readonly nodeCount: number;
  readonly edgeCount: number;
  readonly motion: boolean;
  readonly reducedMotion: boolean;
}

export type FcoseLayoutOptions = LayoutOptions & {
  readonly name: 'fcose';
  readonly quality: 'default' | 'draft';
  readonly animate: boolean;
  readonly animationDuration: number;
  readonly fit: boolean;
  readonly padding: number;
  readonly randomize: boolean;
  readonly nodeDimensionsIncludeLabels: boolean;
  readonly nodeSeparation: number;
  readonly idealEdgeLength: number;
  readonly nodeRepulsion: number;
};

const NODE_COLORS = [
  '#2563eb',
  '#7c3aed',
  '#c026d3',
  '#db2777',
  '#dc2626',
  '#ea580c',
  '#ca8a04',
  '#16a34a',
  '#059669',
  '#0891b2',
] as const;

export function createGraphElements(extraction: GraphExtraction): ElementDefinition[] {
  return [
    ...extraction.nodes.map((node) => ({
      group: 'nodes' as const,
      data: {
        id: node.id,
        caption: node.caption ?? node.labels[0] ?? node.id,
        colorKey: node.labels[0] ?? node.id,
        labels: [...node.labels],
      },
    })),
    ...extraction.edges.map((edge) => ({
      group: 'edges' as const,
      data: {
        id: edge.id,
        source: edge.source,
        target: edge.target,
        caption: edge.type ?? '',
        relationshipType: edge.type,
      },
    })),
  ];
}

export function createGraphStyles(theme: GraphTheme): StylesheetJson {
  const dark = theme === 'dark';
  const foreground = dark ? '#f8fafc' : '#0f172a';
  const surface = dark ? '#0f172a' : '#ffffff';
  const relationship = dark ? '#94a3b8' : '#475569';
  const selection = dark ? '#38bdf8' : '#0284c7';

  return [
    {
      selector: 'node',
      style: {
        shape: 'ellipse',
        width: 38,
        height: 38,
        'background-color': (node) => stableNodeColor(String(node.data('colorKey') ?? '')),
        'border-color': dark ? '#e2e8f0' : '#ffffff',
        'border-width': 2,
        label: 'data(caption)',
        color: foreground,
        'font-size': 11,
        'font-weight': 600,
        'text-background-color': surface,
        'text-background-opacity': 0.9,
        'text-background-padding': '3px',
        'text-valign': 'bottom',
        'text-margin-y': 8,
        'text-max-width': '128px',
        'text-overflow-wrap': 'anywhere',
        'min-zoomed-font-size': 7,
      },
    },
    {
      selector: 'edge',
      style: {
        width: 2,
        'line-color': relationship,
        'target-arrow-color': relationship,
        'target-arrow-shape': 'triangle',
        'arrow-scale': 1,
        'curve-style': 'bezier',
        'loop-direction': '-45deg',
        'loop-sweep': '70deg',
        label: 'data(caption)',
        color: foreground,
        'font-size': 9,
        'font-weight': 600,
        'text-background-color': surface,
        'text-background-opacity': 0.9,
        'text-background-padding': '3px',
        'text-rotation': 'autorotate',
        'min-zoomed-font-size': 7,
      },
    },
    {
      selector: 'node:selected',
      style: {
        'border-color': selection,
        'border-width': 4,
        'underlay-color': selection,
        'underlay-opacity': 0.24,
        'underlay-padding': 6,
      },
    },
    {
      selector: 'edge:selected',
      style: {
        width: 3,
        'line-color': selection,
        'target-arrow-color': selection,
      },
    },
    {
      selector: '.node-caption-hidden',
      style: { 'text-opacity': 0, 'text-background-opacity': 0 },
    },
    {
      selector: '.edge-caption-hidden',
      style: { 'text-opacity': 0, 'text-background-opacity': 0 },
    },
    {
      selector: '.focused',
      style: { opacity: 1, 'text-opacity': 1, 'text-background-opacity': 0.9, 'z-index': 10 },
    },
    {
      selector: '.dimmed',
      style: { opacity: 0.18, 'text-opacity': 0, 'text-background-opacity': 0 },
    },
  ];
}

export function createLayoutPolicy(input: LayoutPolicyInput): FcoseLayoutOptions {
  const large = input.nodeCount > 500 || input.edgeCount > 1_000;
  const animate = input.motion && !input.reducedMotion && !large;
  return {
    name: 'fcose',
    quality: large ? 'draft' : 'default',
    animate,
    animationDuration: animate ? 450 : 0,
    fit: true,
    padding: 36,
    randomize: true,
    nodeDimensionsIncludeLabels: false,
    nodeSeparation: 38,
    idealEdgeLength: 74,
    nodeRepulsion: 4_500,
  };
}

function stableNodeColor(key: string): string {
  let hash = 0;
  for (let index = 0; index < key.length; index += 1) {
    hash = (hash * 31 + key.charCodeAt(index)) >>> 0;
  }
  return NODE_COLORS[hash % NODE_COLORS.length];
}
