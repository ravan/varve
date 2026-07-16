import type { QueryShape } from './gql';

export interface ObservedSchemaEntry {
  readonly count: number;
  readonly firstSeen: number;
  readonly lastSeen: number;
  readonly starterGql: string;
}

export interface ObservedSchema {
  readonly labels: Readonly<Record<string, ObservedSchemaEntry>>;
  readonly relationshipTypes: Readonly<Record<string, ObservedSchemaEntry>>;
}

type SchemaKind = 'label' | 'relationship';

export function extractObservedSchema(shape: QueryShape, timestamp: number): ObservedSchema {
  requireTimestamp(timestamp);
  if (shape.ambiguous) return { labels: {}, relationshipTypes: {} };

  const labels = new Map<string, number>();
  const relationshipTypes = new Map<string, number>();

  for (const pattern of shape.patterns) {
    for (const node of pattern.nodes) {
      for (const label of node.labels) increment(labels, label);
    }
    for (const relationship of pattern.relationships) {
      for (const type of relationship.types) increment(relationshipTypes, type);
    }
  }

  return {
    labels: observedRecord(labels, timestamp, 'label'),
    relationshipTypes: observedRecord(relationshipTypes, timestamp, 'relationship'),
  };
}

export function mergeObservedSchema(
  previous: ObservedSchema | undefined,
  observation: ObservedSchema,
): ObservedSchema {
  return {
    labels: mergeRecords(previous?.labels, observation.labels, 'label'),
    relationshipTypes: mergeRecords(
      previous?.relationshipTypes,
      observation.relationshipTypes,
      'relationship',
    ),
  };
}

export function buildLabelStarterGql(label: string): string {
  return `MATCH (n:${escapeGqlIdentifier(label)}) RETURN n`;
}

export function buildRelationshipStarterGql(type: string): string {
  return `MATCH (a)-[r:${escapeGqlIdentifier(type)}]->(b) RETURN a, r, b`;
}

const SAFE_IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]*$/;

// Varve's lexer has no backtick-quoting, so a plain identifier must stay
// plain or the query is a parse error. Backticks remain only as a marker for
// names Varve could never parse anyway.
export function escapeGqlIdentifier(identifier: string): string {
  if (SAFE_IDENTIFIER.test(identifier)) return identifier;
  return `\`${identifier.replaceAll('`', '``')}\``;
}

function increment(counts: Map<string, number>, name: string): void {
  counts.set(name, (counts.get(name) ?? 0) + 1);
}

function observedRecord(
  counts: Map<string, number>,
  timestamp: number,
  kind: SchemaKind,
): Record<string, ObservedSchemaEntry> {
  return Object.fromEntries(
    [...counts.entries()]
      .sort(([left], [right]) => compareText(left, right))
      .map(([name, count]) => [
        name,
        {
          count,
          firstSeen: timestamp,
          lastSeen: timestamp,
          starterGql: starterGql(kind, name),
        },
      ]),
  );
}

function mergeRecords(
  previous: Readonly<Record<string, ObservedSchemaEntry>> | undefined,
  observation: Readonly<Record<string, ObservedSchemaEntry>>,
  kind: SchemaKind,
): Record<string, ObservedSchemaEntry> {
  const names = new Set([...Object.keys(previous ?? {}), ...Object.keys(observation)]);

  return Object.fromEntries(
    [...names].sort(compareText).map((name) => {
      const earlier = previous?.[name];
      const current = observation[name];
      if (!earlier) return [name, { ...current, starterGql: starterGql(kind, name) }];
      if (!current) return [name, { ...earlier, starterGql: starterGql(kind, name) }];

      return [
        name,
        {
          count: earlier.count + current.count,
          firstSeen: Math.min(earlier.firstSeen, current.firstSeen),
          lastSeen: Math.max(earlier.lastSeen, current.lastSeen),
          starterGql: starterGql(kind, name),
        },
      ];
    }),
  );
}

function starterGql(kind: SchemaKind, name: string): string {
  return kind === 'label' ? buildLabelStarterGql(name) : buildRelationshipStarterGql(name);
}

function requireTimestamp(timestamp: number): void {
  if (!Number.isFinite(timestamp)) {
    throw new TypeError('Observed schema timestamp must be finite');
  }
}

function compareText(left: string, right: string): number {
  return left < right ? -1 : left > right ? 1 : 0;
}
