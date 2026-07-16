import type { GraphEdge, GraphExtraction, GraphLimits, GraphNode } from './graph';
import type { NormalizedRow } from './results';

/**
 * Builds a graph from Varve's expanded entity columns. `RETURN n` comes back
 * as `n._iid`, `n._labels`, `n.name`, … columns rather than one opaque value,
 * which the shape-proof extractor in graph.ts cannot use. This extractor keys
 * on the engine's system columns instead: an alias with `_src_iid` and
 * `_dst_iid` is a relationship, an alias with only `_iid` is a node.
 * Relationships are kept when both endpoints appear among the returned nodes.
 */
export function extractEntityGraph(
  rows: readonly NormalizedRow[],
  limits: GraphLimits,
): GraphExtraction {
  const nodesByIid = new Map<string, { labels: Set<string>; captions: Map<string, string> }>();
  const edgesByIid = new Map<string, { source: string; target: string; type?: string }>();

  for (const row of rows) {
    for (const [alias, fields] of groupByAlias(row)) {
      const iid = asString(fields.get('_iid'));
      if (iid === undefined) continue;
      const source = asString(fields.get('_src_iid'));
      const target = asString(fields.get('_dst_iid'));

      if (source !== undefined && target !== undefined) {
        const existing = edgesByIid.get(iid);
        if (existing === undefined) {
          edgesByIid.set(iid, { source, target, type: firstLabel(fields.get('_labels')) });
        }
        continue;
      }

      const node = nodesByIid.get(iid) ?? { labels: new Set<string>(), captions: new Map() };
      for (const label of asLabels(fields.get('_labels'))) node.labels.add(label);
      for (const [property, value] of fields) {
        if (property.startsWith('_')) continue;
        const caption = scalarCaption(value);
        if (caption !== undefined && !node.captions.has(property)) {
          node.captions.set(property, caption);
        }
      }
      nodesByIid.set(iid, node);
      void alias;
    }
  }

  if (nodesByIid.size === 0) {
    return {
      available: false,
      reason:
        'Graph topology is unavailable because the rows contain no returned entities. Return whole variables, for example RETURN a, r, b.',
      nodes: [],
      edges: [],
      totalNodes: 0,
      totalEdges: 0,
      truncated: false,
    };
  }

  const nodes: GraphNode[] = [];
  const included = new Set<string>();
  for (const [iid, node] of nodesByIid) {
    if (nodes.length === limits.maxNodes) break;
    included.add(iid);
    nodes.push({
      id: iid,
      labels: [...node.labels],
      caption: chooseCaption(node.captions, node.labels, iid),
    });
  }

  const edges: GraphEdge[] = [];
  for (const [iid, edge] of edgesByIid) {
    if (edges.length === limits.maxEdges) break;
    if (!included.has(edge.source) || !included.has(edge.target)) continue;
    edges.push({
      id: iid,
      source: edge.source,
      target: edge.target,
      ...(edge.type === undefined ? {} : { type: edge.type }),
      inferred: true,
    });
  }

  return {
    available: true,
    nodes,
    edges,
    totalNodes: nodesByIid.size,
    totalEdges: edgesByIid.size,
    truncated: nodes.length < nodesByIid.size || edges.length < edgesByIid.size,
  };
}

function groupByAlias(row: NormalizedRow): Map<string, Map<string, unknown>> {
  const aliases = new Map<string, Map<string, unknown>>();
  for (const [column, cell] of Object.entries(row)) {
    if (cell.kind !== 'value') continue;
    const separator = column.indexOf('.');
    if (separator <= 0 || separator === column.length - 1) continue;
    const alias = column.slice(0, separator);
    const field = column.slice(separator + 1);
    const fields = aliases.get(alias) ?? new Map<string, unknown>();
    fields.set(field, cell.value);
    aliases.set(alias, fields);
  }
  return aliases;
}

function asString(value: unknown): string | undefined {
  return typeof value === 'string' && value.length > 0 ? value : undefined;
}

function asLabels(value: unknown): string[] {
  return Array.isArray(value)
    ? value.filter((item): item is string => typeof item === 'string')
    : [];
}

function firstLabel(value: unknown): string | undefined {
  return asLabels(value)[0];
}

function scalarCaption(value: unknown): string | undefined {
  return typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean'
    ? String(value)
    : undefined;
}

function chooseCaption(
  captions: Map<string, string>,
  labels: Set<string>,
  fallback: string,
): string {
  for (const property of ['name', 'title', 'label']) {
    const value = captions.get(property);
    if (value !== undefined) return value;
  }
  const firstOther = captions.values().next().value;
  if (firstOther !== undefined) return firstOther;
  const firstEntityLabel = labels.values().next().value;
  return firstEntityLabel ?? fallback;
}
