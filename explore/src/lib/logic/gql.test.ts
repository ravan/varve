import { describe, expect, it } from 'vitest';

import { classifyGql, extractQueryShape } from './gql';

describe('classifyGql', () => {
  it.each([
    'INSERT (:P {_id: 1})',
    'MATCH (p:P) SET p.name = "Ada"',
    'MATCH (p:P) DELETE p',
    'DROP GRAPH people',
    'CREATE GRAPH people',
  ])('classifies %s as write', (gql) => {
    expect(classifyGql(gql)).toBe('write');
  });

  it('ignores mutation words inside strings and quoted identifiers', () => {
    expect(classifyGql("MATCH (p:Person {name: 'DELETE'}) RETURN p")).toBe('read');
    expect(classifyGql('MATCH (`SET`:Person) RETURN `SET`')).toBe('read');
  });

  it('ignores mutation words inside line and block comments', () => {
    expect(classifyGql('/* DELETE p */ MATCH (p:Person) RETURN p')).toBe('read');
    expect(classifyGql('// DROP GRAPH people\\nMATCH (p:Person) RETURN p')).toBe('read');
    expect(classifyGql('-- INSERT (:P)\\nMATCH (p:Person) RETURN p')).toBe('read');
  });

  it('only treats statement-level keywords as mutations', () => {
    expect(classifyGql('RETURN { DELETE: 1 }')).toBe('read');
  });
});

describe('extractQueryShape', () => {
  it('extracts an unambiguous named path shape', () => {
    expect(
      extractQueryShape('MATCH p = (a:Person)-[r:KNOWS]->(b:Person) RETURN p AS path'),
    ).toEqual({
      ambiguous: false,
      patterns: [
        {
          nodes: [
            { variable: 'a', labels: ['Person'] },
            { variable: 'b', labels: ['Person'] },
          ],
          relationships: [{ variable: 'r', types: ['KNOWS'], direction: 'outgoing' }],
        },
      ],
      paths: [
        {
          alias: 'p',
          nodes: ['a', 'b'],
          relationships: ['r'],
          directions: ['outgoing'],
        },
      ],
      returns: [{ kind: 'entity', source: 'p', alias: 'path' }],
    });
  });

  it('recognizes reverse relationships and direct return variables', () => {
    expect(
      extractQueryShape('MATCH (a:Person)<-[r:KNOWS]-(b:Person) RETURN a, r, b'),
    ).toMatchObject({
      ambiguous: false,
      patterns: [
        {
          relationships: [{ variable: 'r', types: ['KNOWS'], direction: 'incoming' }],
        },
      ],
      returns: [
        { kind: 'entity', source: 'a', alias: 'a' },
        { kind: 'entity', source: 'r', alias: 'r' },
        { kind: 'entity', source: 'b', alias: 'b' },
      ],
    });
  });

  it('recognizes multiple labels, relationship types, and return aliases', () => {
    expect(
      extractQueryShape(
        'MATCH (a:Person:Employee)-[:KNOWS|WORKS_WITH]-(b:Person) RETURN a AS person',
      ),
    ).toMatchObject({
      patterns: [
        {
          nodes: [
            { variable: 'a', labels: ['Person', 'Employee'] },
            { variable: 'b', labels: ['Person'] },
          ],
          relationships: [
            {
              variable: undefined,
              types: ['KNOWS', 'WORKS_WITH'],
              direction: 'undirected',
            },
          ],
        },
      ],
      returns: [{ kind: 'entity', source: 'a', alias: 'person' }],
    });
  });

  it('keeps strings and comments opaque while extracting shape', () => {
    expect(
      extractQueryShape("/* MATCH (fake:Wrong) */ MATCH (a:Person {name: '(:Wrong)'}) RETURN a"),
    ).toMatchObject({
      patterns: [{ nodes: [{ variable: 'a', labels: ['Person'] }] }],
      returns: [{ kind: 'entity', source: 'a', alias: 'a' }],
    });
  });

  it.each([
    ['[:KNOWS]', { variable: undefined, types: ['KNOWS'] }],
    ['[r]', { variable: 'r', types: [] }],
    ['[r:KNOWS]', { variable: 'r', types: ['KNOWS'] }],
  ])('accepts the supported relationship body %s', (body, relationship) => {
    expect(extractQueryShape(`MATCH (a)-${body}->(b) RETURN a`)).toMatchObject({
      ambiguous: false,
      patterns: [{ relationships: [relationship] }],
    });
  });

  it.each([
    'MATCH (a:Person RETURN a',
    'MATCH shortestPath((a)-[:KNOWS]->(b)) RETURN a',
    "MATCH (a:Person {name: 'Ada}) RETURN a",
    'MATCH (a)-foo-(b) RETURN a',
    'MATCH (a unexpected)-[:KNOWS]->(b) RETURN a',
    'MATCH (a:Person unexpected)-[:KNOWS]->(b) RETURN a',
    'MATCH (a)-[r unexpected:KNOWS]->(b) RETURN a',
    'MATCH (a)-[r|KNOWS]->(b) RETURN a',
    'MATCH (a)-[r junk]->(b) RETURN a',
  ])('returns an empty ambiguous shape for unsupported input %s', (gql) => {
    expect(extractQueryShape(gql)).toEqual({
      ambiguous: true,
      patterns: [],
      paths: [],
      returns: [],
    });
  });

  it('discriminates entity and aliased property projections', () => {
    expect(
      extractQueryShape(
        'MATCH path = (a:Person)-[:KNOWS]->(b:Person) RETURN path, a.name AS from_name, b.title AS to_title',
      ).returns,
    ).toEqual([
      { kind: 'entity', source: 'path', alias: 'path' },
      { kind: 'property', entity: 'a', property: 'name', alias: 'from_name' },
      { kind: 'property', entity: 'b', property: 'title', alias: 'to_title' },
    ]);
  });

  it('retains multiple property projections in return order', () => {
    expect(
      extractQueryShape('MATCH (a:Person) RETURN a.title, a.name AS display_name, a.age').returns,
    ).toEqual([
      { kind: 'property', entity: 'a', property: 'title', alias: 'a.title' },
      { kind: 'property', entity: 'a', property: 'name', alias: 'display_name' },
      { kind: 'property', entity: 'a', property: 'age', alias: 'a.age' },
    ]);
  });

  it('rejects unsupported return expressions conservatively', () => {
    expect(extractQueryShape('MATCH (a:Person) RETURN a, coalesce(a.name, "unknown")')).toEqual({
      ambiguous: true,
      patterns: [],
      paths: [],
      returns: [],
    });
  });

  it('parses a property-only return without inventing an entity projection', () => {
    expect(extractQueryShape('MATCH (a:Person) RETURN a.name AS name')).toMatchObject({
      ambiguous: false,
      returns: [{ kind: 'property', entity: 'a', property: 'name', alias: 'name' }],
    });
  });
});
