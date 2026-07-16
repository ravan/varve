import { describe, expect, it } from 'vitest';

import { hasGqlErrors, validateGql, type GqlDiagnostic } from './gql-validate';

function errors(gql: string): GqlDiagnostic[] {
  return validateGql(gql).filter(({ severity }) => severity === 'error');
}

function warnings(gql: string): GqlDiagnostic[] {
  return validateGql(gql).filter(({ severity }) => severity === 'warning');
}

describe('validateGql', () => {
  it('accepts well-formed reads, writes, and temporal queries', () => {
    expect(validateGql('MATCH (p:Person) RETURN p LIMIT 100')).toEqual([]);
    expect(validateGql("INSERT (:Person {_id: 1, name: 'Ada'})")).toEqual([]);
    expect(
      validateGql(
        "FOR VALID_TIME AS OF TIMESTAMP '2026-01-01T00:00:00Z'\nMATCH (a:S)-[r:CALLS]->(b:S) RETURN a, r, b",
      ),
    ).toEqual([]);
  });

  it('rejects empty and non-statement input with positions', () => {
    expect(errors('   ')[0].message).toBe('Enter a GQL statement.');
    const notGql = errors('hello world');
    expect(notGql[0].message).toContain('found "hello"');
    expect(notGql[0]).toMatchObject({ from: 0, to: 5 });
    expect(errors('// only a comment')[0].message).toBe('Enter a GQL statement.');
  });

  it('flags unbalanced brackets at the offending character', () => {
    expect(errors('MATCH (p:Person RETURN p')[0]).toMatchObject({
      message: 'Unclosed (: expected ).',
      from: 6,
    });
    expect(errors('MATCH (p:Person)) RETURN p')[0].message).toContain('Unexpected )');
    expect(errors('MATCH (p:Person] RETURN p').map(({ message }) => message)).toContain(
      'Expected ) before this ].',
    );
  });

  it('flags unterminated strings and comments', () => {
    expect(errors("MATCH (p:Person) WHERE p.name = 'Ada RETURN p")[0].message).toContain(
      'Unterminated string',
    );
    expect(errors('MATCH (p:Person) /* hmm RETURN p')[0].message).toContain(
      'Unterminated block comment',
    );
  });

  it('rejects backtick identifiers, which Varve cannot parse', () => {
    const found = errors('MATCH (p:`Person`) RETURN p');
    expect(found[0].message).toContain('backtick');
    expect(found[0]).toMatchObject({ from: 9, to: 17 });
  });

  it('requires a relationship type on edge patterns', () => {
    expect(errors('MATCH (a:P)-[r]->(b:P) RETURN a, r, b')[0].message).toContain(
      'relationship type',
    );
    expect(errors('MATCH (a:P)-[]->(b:P) RETURN a, b')[0].message).toContain('relationship type');
    expect(validateGql('MATCH (a:P)-[r:KNOWS]->(b:P) RETURN a, r, b')).toEqual([]);
  });

  it('does not mistake list literals for relationship brackets', () => {
    expect(validateGql('MATCH (p:Person) WHERE p.age IN [1, 2, 3] RETURN p')).toEqual([]);
  });

  it('warns on unlabeled read patterns without blocking', () => {
    const found = validateGql('MATCH (n) RETURN n LIMIT 100');
    expect(found).toHaveLength(1);
    expect(found[0].severity).toBe('warning');
    expect(found[0].message).toContain('(n) has no label');
    expect(hasGqlErrors(found)).toBe(false);
  });

  it('does not warn when the variable is labeled in another pattern', () => {
    expect(
      warnings(
        'MATCH (a:Person)-[r:KNOWS]->(b:Person), (a)-[s:LIKES]->(c:Food) RETURN a, r, b, s, c',
      ),
    ).toEqual([]);
  });

  it('does not warn on writes', () => {
    expect(warnings("INSERT ({name: 'Ada'})")).toEqual([]);
  });

  it('sorts multiple findings by position', () => {
    const found = validateGql('MATCH (a:P)-[x]->(b:`B`) RETURN a');
    expect(found.length).toBeGreaterThanOrEqual(2);
    const positions = found.map(({ from }) => from);
    expect(positions).toEqual(positions.toSorted((l, r) => l - r));
  });
});

describe('hasGqlErrors', () => {
  it('distinguishes errors from warnings', () => {
    expect(hasGqlErrors([{ severity: 'warning', message: '', from: 0, to: 0 }])).toBe(false);
    expect(hasGqlErrors([{ severity: 'error', message: '', from: 0, to: 0 }])).toBe(true);
  });
});
