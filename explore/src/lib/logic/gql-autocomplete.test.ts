import { describe, expect, it } from 'vitest';

import type { ObservedSchema } from './schema';
import { analyzeCompletion, completionOptions } from './gql-autocomplete';

function at(text: string): { text: string; pos: number } {
  const pos = text.indexOf('¦');
  return { text: text.replace('¦', ''), pos };
}

function kindAt(marked: string): string | null {
  const { text, pos } = at(marked);
  return analyzeCompletion(text, pos)?.kind ?? null;
}

const schema: ObservedSchema = {
  labels: {
    Person: { count: 2, firstSeen: 1, lastSeen: 2, starterGql: '' },
    'Weird Label': { count: 1, firstSeen: 1, lastSeen: 1, starterGql: '' },
  },
  relationshipTypes: {
    KNOWS: { count: 1, firstSeen: 1, lastSeen: 1, starterGql: '' },
  },
};

describe('analyzeCompletion', () => {
  it('completes labels after a colon inside a node parenthesis', () => {
    expect(kindAt('MATCH (n:¦')).toBe('label');
    expect(kindAt('MATCH (n:Per¦')).toBe('label');
    expect(kindAt('MATCH (:¦)')).toBe('label');
  });

  it('completes relationship types inside brackets, including alternatives', () => {
    expect(kindAt('MATCH (a)-[r:¦]->(b)')).toBe('relationshipType');
    expect(kindAt('MATCH (a)-[:KNOWS|LIK¦]->(b)')).toBe('relationshipType');
  });

  it('completes keywords while typing a bare word', () => {
    expect(kindAt('MATCH (n) RET¦')).toBe('keyword');
    expect(kindAt('MAT¦')).toBe('keyword');
  });

  it('reports the start of the partial word', () => {
    const { text, pos } = at('MATCH (n:Per¦)');
    expect(analyzeCompletion(text, pos)).toEqual({ kind: 'label', from: pos - 3 });
  });

  it('stays quiet inside strings, property maps, comments, and property access', () => {
    expect(kindAt("MATCH (n) WHERE n.name = 'Per¦'")).toBeNull();
    expect(kindAt('MATCH (n {name:¦})')).toBeNull();
    expect(kindAt('// MAT¦')).toBeNull();
    expect(kindAt('MATCH (n) RETURN n.na¦')).toBeNull();
    expect(kindAt('MATCH (n)¦')).toBeNull();
  });
});

describe('completionOptions', () => {
  it('offers observed labels and escapes unsafe identifiers', () => {
    const options = completionOptions('label', schema);
    expect(options.map(({ label }) => label)).toEqual(['Person', 'Weird Label']);
    expect(options[0].apply).toBeUndefined();
    expect(options[1].apply).toBe('`Weird Label`');
  });

  it('offers observed relationship types and keywords', () => {
    expect(completionOptions('relationshipType', schema).map(({ label }) => label)).toEqual([
      'KNOWS',
    ]);
    expect(completionOptions('keyword', schema).map(({ label }) => label)).toContain('RETURN');
  });
});
