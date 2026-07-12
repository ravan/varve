import { describe, expect, it } from 'vitest';

import { extractQueryShape } from './gql';
import { extractGraph } from './graph';

describe('extractGraph', () => {
  it('turns an alternating named path into deduplicated topology', () => {
    const shape = extractQueryShape('MATCH p = (a:Person)-[:KNOWS]->(b:Person) RETURN p');
    const graph = extractGraph(
      shape,
      [{ p: ['node-a', 'edge-ab', 'node-b'] }, { p: ['node-a', 'edge-ab', 'node-b'] }],
      1000,
    );

    expect(graph.available).toBe(true);
    expect(graph.nodes).toEqual([
      { id: 'node-a', labels: ['Person'] },
      { id: 'node-b', labels: ['Person'] },
    ]);
    expect(graph.edges).toEqual([
      expect.objectContaining({
        id: 'edge-ab',
        source: 'node-a',
        target: 'node-b',
        type: 'KNOWS',
        inferred: true,
      }),
    ]);
    expect(graph.truncated).toBe(false);
  });

  it('uses pattern direction for reverse named paths', () => {
    const shape = extractQueryShape('MATCH p = (a:Person)<-[:KNOWS]-(b:Person) RETURN p AS route');

    expect(extractGraph(shape, [{ route: ['node-a', 'edge-ba', 'node-b'] }], 1000)).toMatchObject({
      available: true,
      edges: [{ source: 'node-b', target: 'node-a', type: 'KNOWS' }],
    });
  });

  it('maps direct returned variables and ignores unrelated scalar attachments', () => {
    const shape = extractQueryShape(
      'MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a AS from, r, b AS to',
    );
    const graph = extractGraph(
      shape,
      [
        {
          from: { kind: 'value', value: 'node-a' },
          r: { kind: 'value', value: 'edge-ab' },
          to: { kind: 'value', value: 'node-b' },
          score: { kind: 'value', value: 0.8 },
        },
      ],
      1000,
    );

    expect(graph).toMatchObject({
      available: true,
      nodes: [{ id: 'node-a' }, { id: 'node-b' }],
      edges: [{ id: 'edge-ab', source: 'node-a', target: 'node-b' }],
    });
  });

  it('supports exact Varve byte identifiers without inferring from display properties', () => {
    const shape = extractQueryShape('MATCH p = (a)-[r]->(b) RETURN p');
    const graph = extractGraph(
      shape,
      [{ p: [{ $bytes: 'YQ==' }, { $bytes: 'cg==' }, { $bytes: 'Yg==' }] }],
      1000,
    );

    expect(graph).toMatchObject({
      available: true,
      nodes: [{ id: 'bytes:YQ==' }, { id: 'bytes:Yg==' }],
      edges: [{ id: 'bytes:cg==', source: 'bytes:YQ==', target: 'bytes:Yg==' }],
    });
    expect(extractGraph(shape, [{ p: [{ id: 'a' }, 'edge', { id: 'b' }] }], 1000)).toMatchObject({
      available: false,
      reason: expect.stringContaining('identity'),
    });
  });

  it('refuses malformed path values and ambiguous query shapes', () => {
    const shape = extractQueryShape('MATCH p = (a)-[r]->(b) RETURN p');

    expect(extractGraph(shape, [{ p: ['node-a', 'edge-ab'] }], 1000)).toMatchObject({
      available: false,
      reason: expect.stringContaining('alternating'),
    });
    expect(
      extractGraph({ ambiguous: true, patterns: [], paths: [], returns: [] }, [], 1000),
    ).toMatchObject({ available: false, reason: expect.stringContaining('ambiguous') });
  });

  it('refuses to invent topology from scalar table rows', () => {
    expect(
      extractGraph(
        { ambiguous: false, patterns: [], paths: [], returns: [] },
        [{ name: 'Ada' }],
        1000,
      ),
    ).toMatchObject({ available: false, reason: expect.stringContaining('topology') });
  });

  it('caps graph elements at 1,000 without changing source rows', () => {
    const shape = extractQueryShape('MATCH p = (a)-[r]->(b) RETURN p');
    const rows = Array.from({ length: 501 }, (_, index) => ({
      p: ['root', `edge-${index}`, `node-${index}`],
    }));

    const graph = extractGraph(shape, rows, 1000);

    expect(graph.available).toBe(true);
    expect(graph.nodes.length + graph.edges.length).toBe(1000);
    expect(graph.truncated).toBe(true);
    expect(rows[0]).toEqual({ p: ['root', 'edge-0', 'node-0'] });
    expect(rows).toHaveLength(501);
  });
});
