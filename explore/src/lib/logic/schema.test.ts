import { describe, expect, it } from 'vitest';

import { extractQueryShape } from './gql';
import {
  buildLabelStarterGql,
  buildRelationshipStarterGql,
  extractObservedSchema,
  mergeObservedSchema,
} from './schema';

describe('observed schema', () => {
  it('aggregates query-derived labels and types with usage metadata', () => {
    const first = extractObservedSchema(
      extractQueryShape('MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b'),
      10,
    );
    const merged = mergeObservedSchema(undefined, first);

    expect(merged.labels.Person).toMatchObject({ count: 2, firstSeen: 10, lastSeen: 10 });
    expect(merged.relationshipTypes.KNOWS.count).toBe(1);
  });

  it('merges counts and time bounds without mutating either observation', () => {
    const first = extractObservedSchema(
      extractQueryShape('MATCH (a:Person:Employee)-[:KNOWS]->(b:Person) RETURN a'),
      20,
    );
    const second = extractObservedSchema(
      extractQueryShape('MATCH (a:Employee)-[:WORKS_WITH]->(b:Team) RETURN a'),
      5,
    );

    const merged = mergeObservedSchema(first, second);

    expect(merged).toMatchObject({
      labels: {
        Employee: { count: 2, firstSeen: 5, lastSeen: 20 },
        Person: { count: 2, firstSeen: 20, lastSeen: 20 },
        Team: { count: 1, firstSeen: 5, lastSeen: 5 },
      },
      relationshipTypes: {
        KNOWS: { count: 1, firstSeen: 20, lastSeen: 20 },
        WORKS_WITH: { count: 1, firstSeen: 5, lastSeen: 5 },
      },
    });
    expect(Object.keys(merged.labels)).toEqual(['Employee', 'Person', 'Team']);
    expect(first.labels.Employee.count).toBe(1);
    expect(second.labels.Employee.count).toBe(1);
  });

  it('does not treat an ambiguous query shape as observed metadata', () => {
    expect(
      extractObservedSchema({ ambiguous: true, patterns: [], paths: [], returns: [] }, 10),
    ).toEqual({ labels: {}, relationshipTypes: {} });
  });

  it('builds safe starter GQL with backtick-escaped identifiers', () => {
    expect(buildLabelStarterGql('Person`Admin')).toBe('MATCH (n:`Person``Admin`) RETURN n');
    expect(buildRelationshipStarterGql('KNOWS`SECRET')).toBe(
      'MATCH (a)-[r:`KNOWS``SECRET`]->(b) RETURN a, r, b',
    );
  });
});
