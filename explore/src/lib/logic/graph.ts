import type {
  NamedPathShape,
  PropertyReturnShape,
  QueryPatternShape,
  QueryShape,
  RelationshipDirection,
} from './gql';
import { isCanonicalBytesObject, type NormalizedCell } from './results';

export interface GraphInspectionValue {
  readonly column: string;
  readonly value: NormalizedCell;
}

export interface GraphInspection {
  readonly sourceId: string;
  readonly kind: 'node' | 'relationship';
  readonly id: string;
  readonly labels: readonly string[];
  readonly relationshipType?: string;
  readonly source?: string;
  readonly target?: string;
  readonly inferred: true;
  readonly relatedRowCount: number;
  readonly relatedValues: readonly GraphInspectionValue[];
}

export interface GraphNode {
  readonly id: string;
  readonly labels: readonly string[];
  readonly caption?: string;
}

export interface GraphEdge {
  readonly id: string;
  readonly source: string;
  readonly target: string;
  readonly type?: string;
  readonly inferred: true;
}

export interface GraphExtraction {
  readonly available: boolean;
  readonly reason?: string;
  readonly nodes: readonly GraphNode[];
  readonly edges: readonly GraphEdge[];
  readonly totalNodes: number;
  readonly totalEdges: number;
  readonly truncated: boolean;
}

export interface GraphLimits {
  readonly maxNodes: number;
  readonly maxEdges: number;
}

type GraphRowValue = unknown | NormalizedCell;
type GraphRow = Readonly<Record<string, GraphRowValue>>;

interface OpaqueIdentity {
  readonly key: string;
  readonly id: string;
}

interface PendingNode {
  readonly identity: OpaqueIdentity;
  readonly labels: string[];
  readonly captionValues: Map<string, string>;
}

interface PendingEdge {
  readonly identity: OpaqueIdentity;
  readonly source: OpaqueIdentity;
  readonly target: OpaqueIdentity;
  readonly type?: string;
}

interface PendingGraph {
  readonly nodes: Map<string, PendingNode>;
  readonly edges: Map<string, PendingEdge>;
  readonly displayIds: Map<string, string>;
  readonly roles: Map<string, 'node' | 'edge'>;
}

class UnsupportedTopology extends Error {}

const unavailable = (reason: string): GraphExtraction => ({
  available: false,
  reason,
  nodes: [],
  edges: [],
  totalNodes: 0,
  totalEdges: 0,
  truncated: false,
});

export function extractGraph(
  shape: QueryShape,
  rows: readonly GraphRow[],
  limits: GraphLimits,
): GraphExtraction {
  if (
    !Number.isSafeInteger(limits.maxNodes) ||
    limits.maxNodes < 0 ||
    !Number.isSafeInteger(limits.maxEdges) ||
    limits.maxEdges < 0
  ) {
    throw new RangeError('Graph limits must be non-negative integers');
  }
  if (shape.ambiguous) {
    return unavailable('Graph topology is unavailable because the query shape is ambiguous.');
  }

  try {
    const returnAliases = indexReturnAliases(shape);
    const captionProjections = indexCaptionProjections(shape);
    const pending: PendingGraph = {
      nodes: new Map(),
      edges: new Map(),
      displayIds: new Map(),
      roles: new Map(),
    };
    const pathPatternIndexes = new Set<number>();
    let mappedTopology = false;

    for (const path of shape.paths) {
      if (!returnAliases.has(path.alias)) continue;
      const column = requireSingleReturnAlias(returnAliases, path.alias, 'named path');
      const patternIndex = findPathPattern(shape.patterns, path);
      pathPatternIndexes.add(patternIndex);
      const pattern = shape.patterns[patternIndex];

      for (const row of rows) {
        const value = readColumn(row, column);
        if (!Array.isArray(value) || value.length !== pattern.relationships.length * 2 + 1) {
          throw new UnsupportedTopology(
            'Named path topology must be an alternating node/relationship sequence.',
          );
        }
        addPath(pending, pattern, value, row, captionProjections);
      }
      mappedTopology = true;
    }

    shape.patterns.forEach((pattern, patternIndex) => {
      if (pathPatternIndexes.has(patternIndex) || !canMapDirectly(pattern, returnAliases)) return;

      for (const row of rows) {
        addDirectPattern(pending, pattern, row, returnAliases, captionProjections);
      }
      mappedTopology = true;
    });

    if (!mappedTopology) {
      return unavailable('Graph topology cannot be proven from the returned query values.');
    }

    return applyLimits(pending, limits);
  } catch (error) {
    if (error instanceof UnsupportedTopology) return unavailable(error.message);
    throw error;
  }
}

function indexReturnAliases(shape: QueryShape): Map<string, string[]> {
  const aliases = new Set<string>();
  const result = new Map<string, string[]>();

  for (const returned of shape.returns) {
    if (aliases.has(returned.alias)) {
      throw new UnsupportedTopology(
        'Graph topology is ambiguous because a return alias is repeated.',
      );
    }
    aliases.add(returned.alias);
    if (returned.kind !== 'entity') continue;
    const existing = result.get(returned.source) ?? [];
    existing.push(returned.alias);
    result.set(returned.source, existing);
  }

  return result;
}

function indexCaptionProjections(shape: QueryShape): Map<string, PropertyReturnShape[]> {
  const result = new Map<string, PropertyReturnShape[]>();
  for (const returned of shape.returns) {
    if (returned.kind !== 'property') continue;
    const projections = result.get(returned.entity) ?? [];
    projections.push(returned);
    result.set(returned.entity, projections);
  }
  return result;
}

function requireSingleReturnAlias(
  aliases: Map<string, string[]>,
  source: string,
  description: string,
): string {
  const matches = aliases.get(source);
  if (matches?.length !== 1) {
    throw new UnsupportedTopology(
      `Graph topology is ambiguous because the ${description} does not have one returned value.`,
    );
  }
  return matches[0];
}

function findPathPattern(patterns: readonly QueryPatternShape[], path: NamedPathShape): number {
  const candidates: number[] = [];
  patterns.forEach((pattern, index) => {
    if (
      arraysEqual(
        pattern.nodes.map((node) => node.variable),
        path.nodes,
      ) &&
      arraysEqual(
        pattern.relationships.map((relationship) => relationship.variable),
        path.relationships,
      ) &&
      arraysEqual(
        pattern.relationships.map((relationship) => relationship.direction),
        path.directions,
      )
    ) {
      candidates.push(index);
    }
  });

  if (candidates.length !== 1) {
    throw new UnsupportedTopology(
      'Graph topology is ambiguous because a named path has no unique pattern mapping.',
    );
  }
  return candidates[0];
}

function arraysEqual<T>(left: readonly T[], right: readonly T[]): boolean {
  return left.length === right.length && left.every((value, index) => value === right[index]);
}

function canMapDirectly(pattern: QueryPatternShape, returnAliases: Map<string, string[]>): boolean {
  return (
    pattern.nodes.every(
      (node) => node.variable !== undefined && returnAliases.get(node.variable)?.length === 1,
    ) &&
    pattern.relationships.every(
      (relationship) =>
        relationship.variable !== undefined &&
        returnAliases.get(relationship.variable)?.length === 1,
    )
  );
}

function addPath(
  pending: PendingGraph,
  pattern: QueryPatternShape,
  value: readonly unknown[],
  row: GraphRow,
  captionProjections: Map<string, PropertyReturnShape[]>,
): void {
  const nodeIdentities = pattern.nodes.map((node, index) => {
    const identity = requireIdentity(value[index * 2]);
    addNode(
      pending,
      identity,
      node.labels,
      readCaptionValues(row, node.variable, captionProjections),
    );
    return identity;
  });

  pattern.relationships.forEach((relationship, index) => {
    const identity = requireIdentity(value[index * 2 + 1]);
    const [source, target] = endpoints(
      nodeIdentities[index],
      nodeIdentities[index + 1],
      relationship.direction,
    );
    addEdge(pending, identity, source, target, singleType(relationship.types));
  });
}

function addDirectPattern(
  pending: PendingGraph,
  pattern: QueryPatternShape,
  row: GraphRow,
  returnAliases: Map<string, string[]>,
  captionProjections: Map<string, PropertyReturnShape[]>,
): void {
  const nodeIdentities = pattern.nodes.map((node) => {
    const variable = node.variable as string;
    const identity = requireIdentity(
      readColumn(row, requireSingleReturnAlias(returnAliases, variable, `node ${variable}`)),
    );
    addNode(
      pending,
      identity,
      node.labels,
      readCaptionValues(row, node.variable, captionProjections),
    );
    return identity;
  });

  pattern.relationships.forEach((relationship, index) => {
    const variable = relationship.variable as string;
    const identity = requireIdentity(
      readColumn(
        row,
        requireSingleReturnAlias(returnAliases, variable, `relationship ${variable}`),
      ),
    );
    const [source, target] = endpoints(
      nodeIdentities[index],
      nodeIdentities[index + 1],
      relationship.direction,
    );
    addEdge(pending, identity, source, target, singleType(relationship.types));
  });
}

function readColumn(row: GraphRow, column: string): unknown {
  if (!Object.prototype.hasOwnProperty.call(row, column)) {
    throw new UnsupportedTopology(
      `Graph topology is unavailable because column ${column} is missing.`,
    );
  }

  const cell = row[column];
  if (isNormalizedCell(cell)) {
    if (cell.kind === 'missing') {
      throw new UnsupportedTopology(
        `Graph topology is unavailable because column ${column} is missing.`,
      );
    }
    return cell.value;
  }
  return cell;
}

function isNormalizedCell(value: unknown): value is NormalizedCell {
  if (!isRecord(value)) return false;
  return value.kind === 'missing' || (value.kind === 'value' && 'value' in value);
}

function requireIdentity(value: unknown): OpaqueIdentity {
  if (typeof value === 'string') {
    return { key: `string:${JSON.stringify(value)}`, id: value };
  }
  if (isCanonicalBytesObject(value)) {
    return { key: `bytes:${JSON.stringify(value.$bytes)}`, id: `bytes:${value.$bytes}` };
  }
  throw new UnsupportedTopology(
    'Graph identity must be an opaque string or exact {$bytes: string} value.',
  );
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function readCaptionValues(
  row: GraphRow,
  variable: string | undefined,
  captionProjections: Map<string, PropertyReturnShape[]>,
): readonly [string, string][] {
  if (variable === undefined) return [];
  const values: [string, string][] = [];
  for (const projection of captionProjections.get(variable) ?? []) {
    if (!Object.prototype.hasOwnProperty.call(row, projection.alias)) continue;
    const cell = row[projection.alias];
    const value = isNormalizedCell(cell) ? (cell.kind === 'value' ? cell.value : undefined) : cell;
    const caption = scalarCaption(value);
    if (caption !== undefined) values.push([projection.property, caption]);
  }
  return values;
}

function scalarCaption(value: unknown): string | undefined {
  return typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean'
    ? String(value)
    : undefined;
}

function addNode(
  pending: PendingGraph,
  identity: OpaqueIdentity,
  labels: readonly string[],
  captionValues: readonly (readonly [string, string])[],
): void {
  registerIdentity(pending, identity, 'node');
  const existing = pending.nodes.get(identity.key);
  if (!existing) {
    pending.nodes.set(identity.key, {
      identity,
      labels: [...new Set(labels)],
      captionValues: new Map(captionValues),
    });
    return;
  }
  for (const label of labels) {
    if (!existing.labels.includes(label)) existing.labels.push(label);
  }
  for (const [property, value] of captionValues) {
    if (!existing.captionValues.has(property)) existing.captionValues.set(property, value);
  }
}

function addEdge(
  pending: PendingGraph,
  identity: OpaqueIdentity,
  source: OpaqueIdentity,
  target: OpaqueIdentity,
  type: string | undefined,
): void {
  registerIdentity(pending, identity, 'edge');
  const existing = pending.edges.get(identity.key);
  if (!existing) {
    pending.edges.set(identity.key, { identity, source, target, type });
    return;
  }
  if (
    existing.source.key !== source.key ||
    existing.target.key !== target.key ||
    existing.type !== type
  ) {
    throw new UnsupportedTopology(
      'Graph topology is ambiguous because one relationship identity has conflicting mappings.',
    );
  }
}

function registerIdentity(
  pending: PendingGraph,
  identity: OpaqueIdentity,
  role: 'node' | 'edge',
): void {
  const displayKey = pending.displayIds.get(identity.id);
  if (displayKey !== undefined && displayKey !== identity.key) {
    throw new UnsupportedTopology(
      'Graph identity encoding is ambiguous for the returned identifier values.',
    );
  }
  pending.displayIds.set(identity.id, identity.key);

  const existingRole = pending.roles.get(identity.key);
  if (existingRole !== undefined && existingRole !== role) {
    throw new UnsupportedTopology(
      'Graph topology is ambiguous because one identity is both a node and a relationship.',
    );
  }
  pending.roles.set(identity.key, role);
}

function endpoints(
  left: OpaqueIdentity,
  right: OpaqueIdentity,
  direction: RelationshipDirection,
): readonly [OpaqueIdentity, OpaqueIdentity] {
  return direction === 'incoming' ? [right, left] : [left, right];
}

function singleType(types: readonly string[]): string | undefined {
  return types.length === 1 ? types[0] : undefined;
}

function applyLimits(pending: PendingGraph, limits: GraphLimits): GraphExtraction {
  const nodes: GraphNode[] = [];
  const includedNodes = new Set<string>();

  for (const node of pending.nodes.values()) {
    if (nodes.length === limits.maxNodes) break;
    includedNodes.add(node.identity.key);
    nodes.push({
      id: node.identity.id,
      labels: [...node.labels],
      caption: chooseCaption(node),
    });
  }

  const edges: GraphEdge[] = [];
  for (const edge of pending.edges.values()) {
    if (edges.length === limits.maxEdges) break;
    if (!includedNodes.has(edge.source.key) || !includedNodes.has(edge.target.key)) continue;
    edges.push({
      id: edge.identity.id,
      source: edge.source.id,
      target: edge.target.id,
      ...(edge.type === undefined ? {} : { type: edge.type }),
      inferred: true,
    });
  }

  return {
    available: true,
    nodes,
    edges,
    totalNodes: pending.nodes.size,
    totalEdges: pending.edges.size,
    truncated: nodes.length < pending.nodes.size || edges.length < pending.edges.size,
  };
}

function chooseCaption(node: PendingNode): string {
  for (const property of ['name', 'title', 'label']) {
    const value = node.captionValues.get(property);
    if (value !== undefined) return value;
  }
  const firstOther = node.captionValues.values().next().value;
  if (firstOther !== undefined) return firstOther;
  return node.labels[0] ?? node.identity.id;
}
