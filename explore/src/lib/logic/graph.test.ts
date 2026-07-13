import { describe, expect, it } from 'vitest';

import { extractQueryShape } from './gql';
import { extractGraph } from './graph';

const LIMITS = { maxNodes: 2_000, maxEdges: 4_000 } as const;

describe('extractGraph', () => {
  it('turns an alternating named path into deduplicated topology', () => {
    const shape = extractQueryShape('MATCH p = (a:Person)-[:KNOWS]->(b:Person) RETURN p');
    const graph = extractGraph(
      shape,
      [{ p: ['node-a', 'edge-ab', 'node-b'] }, { p: ['node-a', 'edge-ab', 'node-b'] }],
      LIMITS,
    );

    expect(graph.available).toBe(true);
    expect(graph.nodes).toEqual([
      { id: 'node-a', labels: ['Person'], caption: 'Person' },
      { id: 'node-b', labels: ['Person'], caption: 'Person' },
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
    expect(graph).toMatchObject({ totalNodes: 2, totalEdges: 1 });
  });

  it('uses pattern direction for reverse named paths', () => {
    const shape = extractQueryShape('MATCH p = (a:Person)<-[:KNOWS]-(b:Person) RETURN p AS route');

    expect(extractGraph(shape, [{ route: ['node-a', 'edge-ba', 'node-b'] }], LIMITS)).toMatchObject(
      {
        available: true,
        edges: [{ source: 'node-b', target: 'node-a', type: 'KNOWS' }],
      },
    );
  });

  it('keeps deterministic endpoints for an undirected relationship', () => {
    const shape = extractQueryShape('MATCH p = (a)-[:CONNECTED]-(b) RETURN p');

    expect(extractGraph(shape, [{ p: ['node-a', 'edge-ab', 'node-b'] }], LIMITS)).toMatchObject({
      available: true,
      edges: [{ source: 'node-a', target: 'node-b', type: 'CONNECTED' }],
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
      LIMITS,
    );

    expect(graph).toMatchObject({
      available: true,
      nodes: [{ id: 'node-a' }, { id: 'node-b' }],
      edges: [{ id: 'edge-ab', source: 'node-a', target: 'node-b' }],
    });
  });

  it('uses complete direct variables when a named path is not returned', () => {
    const shape = extractQueryShape('MATCH p = (a)-[r:KNOWS]->(b) RETURN a, r, b');

    expect(extractGraph(shape, [{ a: 'node-a', r: 'edge-ab', b: 'node-b' }], LIMITS)).toMatchObject(
      {
        available: true,
        nodes: [{ id: 'node-a' }, { id: 'node-b' }],
        edges: [{ id: 'edge-ab', source: 'node-a', target: 'node-b', type: 'KNOWS' }],
      },
    );
  });

  it('rejects a non-returned named path when direct variables are incomplete', () => {
    const shape = extractQueryShape('MATCH p = (a)-[r]->(b) RETURN a, r');

    expect(extractGraph(shape, [{ a: 'node-a', r: 'edge-ab' }], LIMITS)).toMatchObject({
      available: false,
      reason: expect.stringContaining('topology'),
    });
  });

  it('supports exact Varve byte identifiers without inferring from display properties', () => {
    const shape = extractQueryShape('MATCH p = (a)-[r]->(b) RETURN p');
    const graph = extractGraph(
      shape,
      [{ p: [{ $bytes: 'YQ==' }, { $bytes: 'cg==' }, { $bytes: 'Yg==' }] }],
      LIMITS,
    );

    expect(graph).toMatchObject({
      available: true,
      nodes: [{ id: 'bytes:YQ==' }, { id: 'bytes:Yg==' }],
      edges: [{ id: 'bytes:cg==', source: 'bytes:YQ==', target: 'bytes:Yg==' }],
    });
    expect(extractGraph(shape, [{ p: [{ id: 'a' }, 'edge', { id: 'b' }] }], LIMITS)).toMatchObject({
      available: false,
      reason: expect.stringContaining('identity'),
    });
  });

  it.each(['not-base64', 'AB==', 'AAB='])(
    'rejects the malformed or noncanonical byte identity %s',
    ($bytes) => {
      const shape = extractQueryShape('MATCH p = (a)-[r]->(b) RETURN p');

      expect(extractGraph(shape, [{ p: [{ $bytes }, 'edge', 'node-b'] }], LIMITS)).toMatchObject({
        available: false,
        reason: expect.stringContaining('identity'),
      });
    },
  );

  it('refuses malformed path values and ambiguous query shapes', () => {
    const shape = extractQueryShape('MATCH p = (a)-[r]->(b) RETURN p');

    expect(extractGraph(shape, [{ p: ['node-a', 'edge-ab'] }], LIMITS)).toMatchObject({
      available: false,
      reason: expect.stringContaining('alternating'),
    });
    expect(
      extractGraph({ ambiguous: true, patterns: [], paths: [], returns: [] }, [], LIMITS),
    ).toMatchObject({ available: false, reason: expect.stringContaining('ambiguous') });
  });

  it('refuses to invent topology from scalar table rows', () => {
    expect(
      extractGraph(
        { ambiguous: false, patterns: [], paths: [], returns: [] },
        [{ name: 'Ada' }],
        LIMITS,
      ),
    ).toMatchObject({ available: false, reason: expect.stringContaining('topology') });
  });

  it('applies independent node and edge limits, reports totals, and drops dangling edges', () => {
    const shape = extractQueryShape('MATCH p = (a)-[r]->(b) RETURN p');
    const rows = Array.from({ length: 3 }, (_, index) => ({
      p: ['root', `edge-${index}`, `node-${index}`],
    }));

    const nodeLimited = extractGraph(shape, rows, { maxNodes: 2, maxEdges: 10 });
    const edgeLimited = extractGraph(shape, rows, { maxNodes: 10, maxEdges: 1 });

    expect(nodeLimited).toMatchObject({
      available: true,
      totalNodes: 4,
      totalEdges: 3,
      truncated: true,
      nodes: [{ id: 'root' }, { id: 'node-0' }],
      edges: [{ id: 'edge-0', source: 'root', target: 'node-0' }],
    });
    expect(edgeLimited).toMatchObject({
      available: true,
      totalNodes: 4,
      totalEdges: 3,
      truncated: true,
      edges: [{ id: 'edge-0' }],
    });
    expect(rows[0]).toEqual({ p: ['root', 'edge-0', 'node-0'] });
    expect(rows).toHaveLength(3);
  });

  it('chooses deterministic scalar captions and falls back to label then identity', () => {
    const shape = extractQueryShape(
      'MATCH p = (a:Person)-[:KNOWS]->(b:Person) RETURN p, a.title AS title, a.name AS name, a.age AS age, b.name AS peer_name',
    );
    const graph = extractGraph(
      shape,
      [
        {
          p: ['node-a', 'edge-ab', 'node-b'],
          title: 'Engineer',
          name: 'Ada',
          age: 36,
          peer_name: null,
        },
        {
          p: ['node-a', 'edge-ab', 'node-b'],
          title: 'Architect',
          name: 'Grace',
          age: 37,
          peer_name: { nested: true },
        },
      ],
      LIMITS,
    );

    expect(graph.nodes).toEqual([
      { id: 'node-a', labels: ['Person'], caption: 'Ada' },
      { id: 'node-b', labels: ['Person'], caption: 'Person' },
    ]);

    const unlabeled = extractGraph(
      extractQueryShape('MATCH p = (a)-[r]->(b) RETURN p, a.payload AS payload'),
      [{ p: ['node-a', 'edge-ab', 'node-b'], payload: { $bytes: 'YQ==' } }],
      LIMITS,
    );
    expect(unlabeled.nodes).toEqual([
      { id: 'node-a', labels: [], caption: 'node-a' },
      { id: 'node-b', labels: [], caption: 'node-b' },
    ]);
  });

  it('uses the first returned scalar value for caption conflicts', () => {
    const shape = extractQueryShape('MATCH p = (a:Person)-[r]->(b) RETURN p, a.label AS label');
    const graph = extractGraph(
      shape,
      [
        { p: ['node-a', 'edge-ab', 'node-b'], label: false },
        { p: ['node-a', 'edge-ab', 'node-b'], label: true },
      ],
      LIMITS,
    );

    expect(graph.nodes[0]).toMatchObject({ caption: 'false' });
  });

  it('rejects duplicate property aliases instead of sharing one caption column', () => {
    const shape = extractQueryShape(
      'MATCH p = (a:Person)-[r]->(b:Person) RETURN p, a.name AS caption, b.name AS caption',
    );

    expect(
      extractGraph(shape, [{ p: ['node-a', 'edge-ab', 'node-b'], caption: 'ambiguous' }], LIMITS),
    ).toMatchObject({ available: false, reason: expect.stringContaining('alias is repeated') });
  });
});
