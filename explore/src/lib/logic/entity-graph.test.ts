import { describe, expect, it } from 'vitest';

import { extractEntityGraph } from './entity-graph';
import type { NormalizedRow } from './results';

const LIMITS = { maxNodes: 2_000, maxEdges: 4_000 };

function row(values: Record<string, unknown>): NormalizedRow {
  return Object.fromEntries(
    Object.entries(values).map(([column, value]) => [column, { kind: 'value' as const, value }]),
  );
}

// Column shape taken from a live varved response for RETURN a, r, b.
const callsRow = row({
  'a._id': 10,
  'a._iid': 'iid-api',
  'a._labels': ['Service'],
  'a.name': 'api',
  'b._id': 11,
  'b._iid': 'iid-db',
  'b._labels': ['Service'],
  'b.name': 'db',
  'r._id': 'varve:gen:7:2',
  'r._iid': 'iid-calls',
  'r._labels': ['CALLS'],
  'r._src_iid': 'iid-api',
  'r._dst_iid': 'iid-db',
});

describe('extractEntityGraph', () => {
  it('assembles nodes and typed relationships from expanded entity columns', () => {
    const extraction = extractEntityGraph([callsRow], LIMITS);

    expect(extraction.available).toBe(true);
    expect(extraction.nodes).toEqual([
      { id: 'iid-api', labels: ['Service'], caption: 'api' },
      { id: 'iid-db', labels: ['Service'], caption: 'db' },
    ]);
    expect(extraction.edges).toEqual([
      { id: 'iid-calls', source: 'iid-api', target: 'iid-db', type: 'CALLS', inferred: true },
    ]);
  });

  it('handles node-only label scans and dedupes across rows', () => {
    const ada = row({ 'n._iid': 'iid-ada', 'n._labels': ['Person'], 'n.name': 'Ada' });
    const extraction = extractEntityGraph([ada, ada], LIMITS);

    expect(extraction.nodes).toEqual([{ id: 'iid-ada', labels: ['Person'], caption: 'Ada' }]);
    expect(extraction.edges).toEqual([]);
  });

  it('drops relationships whose endpoints were not returned', () => {
    const dangling = row({
      'r._iid': 'iid-rel',
      'r._labels': ['KNOWS'],
      'r._src_iid': 'iid-ada',
      'r._dst_iid': 'iid-ghost',
    });
    const ada = row({ 'n._iid': 'iid-ada', 'n._labels': ['Person'], 'n.name': 'Ada' });

    const extraction = extractEntityGraph([dangling, ada], LIMITS);
    expect(extraction.nodes.map(({ id }) => id)).toEqual(['iid-ada']);
    expect(extraction.edges).toEqual([]);
  });

  it('is unavailable when rows carry no entity columns', () => {
    const extraction = extractEntityGraph([row({ name: 'Ada', count: 3 })], LIMITS);
    expect(extraction.available).toBe(false);
    expect(extraction.reason).toContain('RETURN a, r, b');
  });

  it('respects node and edge limits and reports truncation', () => {
    const rows = Array.from({ length: 5 }, (_, index) =>
      row({ [`n._iid`]: `iid-${index}`, 'n._labels': ['Thing'] }),
    );
    const extraction = extractEntityGraph(rows, { maxNodes: 3, maxEdges: 0 });

    expect(extraction.nodes).toHaveLength(3);
    expect(extraction.totalNodes).toBe(5);
    expect(extraction.truncated).toBe(true);
  });

  it('falls back through caption preferences and skips system fields', () => {
    const extraction = extractEntityGraph(
      [
        row({ 'n._iid': 'iid-1', 'n._labels': ['Doc'], 'n.title': 'Spec', 'n._id': 7 }),
        row({ 'n._iid': 'iid-2', 'n._labels': ['Doc'] }),
        row({ 'n._iid': 'iid-3' }),
      ],
      LIMITS,
    );

    expect(extraction.nodes.map(({ caption }) => caption)).toEqual(['Spec', 'Doc', 'iid-3']);
  });
});
