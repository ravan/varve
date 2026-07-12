import { describe, expect, it } from 'vitest';

import {
  copyableJson,
  formatCell,
  normalizeQueryResponse,
  normalizeTxReceipt,
  pageRows,
  sortRows,
  type NormalizedRow,
} from './results';

describe('normalizeQueryResponse', () => {
  it('keeps first-seen column order and distinguishes missing from null', () => {
    const result = normalizeQueryResponse({ rows: [{ a: 1 }, { b: null, a: 2 }] });

    expect(result.columns).toEqual(['a', 'b']);
    expect(result.rows[0].b).toEqual({ kind: 'missing' });
    expect(result.rows[1].b).toEqual({ kind: 'value', value: null });
  });

  it('keeps the raw response untouched and creates immutable normalized rows', () => {
    const raw = { rows: [{ value: { nested: true } }] };
    const result = normalizeQueryResponse(raw);

    expect(result.raw).toBe(raw);
    expect(result.rows[0]).not.toBe(raw.rows[0]);
    expect(Object.isFrozen(result.columns)).toBe(true);
    expect(Object.isFrozen(result.rows)).toBe(true);
    expect(Object.isFrozen(result.rows[0])).toBe(true);
    expect(Object.isFrozen(result.rows[0].value)).toBe(true);
    expect(raw).toEqual({ rows: [{ value: { nested: true } }] });
  });

  it('rejects malformed query envelopes', () => {
    expect(() => normalizeQueryResponse({ rows: 'not-an-array' })).toThrow('rows');
    expect(() => normalizeQueryResponse({ rows: [null] })).toThrow('row 1');
    expect(() => normalizeQueryResponse({ rows: [[]] })).toThrow('row 1');
  });
});

describe('normalizeTxReceipt', () => {
  it('defaults every known receipt side effect and preserves raw input', () => {
    const raw = {
      tx_id: 12,
      system_time: '2026-07-12T00:00:00Z',
      system_time_us: 12,
      basis: 12,
      side_effects: { nodes_created: 2 },
    };

    expect(normalizeTxReceipt(raw)).toEqual({
      tx_id: 12,
      system_time: '2026-07-12T00:00:00Z',
      system_time_us: 12,
      basis: 12,
      side_effects: {
        nodes_created: 2,
        nodes_deleted: 0,
        relationships_created: 0,
        relationships_deleted: 0,
        properties_set: 0,
        properties_removed: 0,
        labels_added: 0,
        labels_removed: 0,
      },
      raw,
    });
  });

  it('rejects malformed receipt counters', () => {
    expect(() =>
      normalizeTxReceipt({
        tx_id: 1,
        system_time: 'now',
        system_time_us: 1,
        basis: 1,
        side_effects: { nodes_created: -1 },
      }),
    ).toThrow('nodes_created');
  });
});

describe('formatCell', () => {
  it('formats every table value as safe plain text', () => {
    expect(formatCell({ kind: 'missing' })).toBe('Missing');
    expect(formatCell({ kind: 'value', value: null })).toBe('null');
    expect(formatCell({ kind: 'value', value: false })).toBe('false');
    expect(formatCell({ kind: 'value', value: 12.5 })).toBe('12.5');
    expect(formatCell({ kind: 'value', value: '<script>alert(1)</script>' })).toBe(
      '<script>alert(1)</script>',
    );
    expect(formatCell({ kind: 'value', value: { quote: '"', a: [1, true] } })).toBe(
      '{"a":[1,true],"quote":"\\\""}',
    );
  });

  it('presents exact base64 objects with decoded size and a bounded preview', () => {
    expect(formatCell({ kind: 'value', value: { $bytes: 'YQ==' } })).toBe('bytes · 1 B · YQ==');
    expect(
      formatCell({ kind: 'value', value: { $bytes: 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA' } }),
    ).toBe('bytes · 24 B · AAAAAAAAAAAAAAAAAAAAAAAA…');
  });

  it('does not present non-canonical base64 objects as bytes', () => {
    expect(formatCell({ kind: 'value', value: { $bytes: 'AB==' } })).toBe('{"$bytes":"AB=="}');
    expect(formatCell({ kind: 'value', value: { $bytes: 'AAB=' } })).toBe('{"$bytes":"AAB="}');
  });
});

describe('sortRows', () => {
  const row = (id: string, ...values: [unknown?]): NormalizedRow =>
    Object.freeze({
      id: Object.freeze({ kind: 'value', value: id }),
      value:
        values.length > 0
          ? Object.freeze({ kind: 'value' as const, value: values[0] })
          : Object.freeze({ kind: 'missing' }),
    });

  it('sorts stably across missing, null, boolean, number, string, and structured values', () => {
    const rows = [
      row('object-b', { z: 1 }),
      row('number-b', 2),
      row('missing'),
      row('string', '10'),
      row('false', false),
      row('null', null),
      row('number-a', 2),
      row('true', true),
      row('object-a', { a: 1 }),
    ];

    const sorted = sortRows(rows, 'value', 'asc');

    expect(sorted.map((item) => formatCell(item.id))).toEqual([
      'missing',
      'null',
      'false',
      'true',
      'number-b',
      'number-a',
      'string',
      'object-a',
      'object-b',
    ]);
    expect(rows[0] && formatCell(rows[0].id)).toBe('object-b');
  });

  it('reverses value ordering while retaining original order for equal values', () => {
    const rows = [row('first', 4), row('second', 4), row('lower', 1)];

    expect(sortRows(rows, 'value', 'desc').map((item) => formatCell(item.id))).toEqual([
      'first',
      'second',
      'lower',
    ]);
  });
});

describe('pageRows', () => {
  it('returns one-based 50-row pages without mutating the input', () => {
    const rows = Array.from({ length: 101 }, (_, index) => index);

    expect(pageRows(rows, 1)).toEqual(rows.slice(0, 50));
    expect(pageRows(rows, 2)).toEqual(rows.slice(50, 100));
    expect(pageRows(rows, 3)).toEqual([100]);
    expect(pageRows(rows, 4)).toEqual([]);
    expect(rows).toHaveLength(101);
  });

  it('rejects invalid page numbers and page sizes', () => {
    expect(() => pageRows([], 0)).toThrow('page');
    expect(() => pageRows([], 1, 0)).toThrow('page size');
  });
});

describe('copyableJson', () => {
  it('produces recursively deterministic, formatted JSON without changing arrays', () => {
    const first = { z: 1, a: { d: 4, b: 2 }, list: [{ z: 2, a: 1 }] };
    const second = { list: [{ a: 1, z: 2 }], a: { b: 2, d: 4 }, z: 1 };

    expect(copyableJson(first)).toBe(copyableJson(second));
    expect(copyableJson(first)).toBe(
      [
        '{',
        '  "a": {',
        '    "b": 2,',
        '    "d": 4',
        '  },',
        '  "list": [',
        '    {',
        '      "a": 1,',
        '      "z": 2',
        '    }',
        '  ],',
        '  "z": 1',
        '}',
      ].join('\n'),
    );
  });
});
