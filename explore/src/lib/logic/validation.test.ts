import { describe, expect, it } from 'vitest';

import { parseBasis, parsePositiveInteger, validateParameters } from './validation';

describe('validateParameters', () => {
  it('accepts Varve scalar parameters', () => {
    expect(
      validateParameters(
        '{"name":"Ada","active":true,"missing":null,"debt":-12,"score":3.5,"blob":{"$bytes":"YQ=="}}',
      ),
    ).toEqual({
      ok: true,
      value: {
        name: 'Ada',
        active: true,
        missing: null,
        debt: -12,
        score: 3.5,
        blob: { $bytes: 'YQ==' },
      },
    });
  });

  it('rejects arrays before sending them to Varve', () => {
    expect(validateParameters('{"bad":[1]}')).toMatchObject({
      ok: false,
      error: expect.stringContaining('bad'),
    });
  });

  it('rejects nested objects other than an exact $bytes value', () => {
    expect(validateParameters('{"bad":{"nested":true}}')).toMatchObject({ ok: false });
    expect(validateParameters('{"bad":{"$bytes":"YQ==","extra":true}}')).toMatchObject({
      ok: false,
    });
  });

  it('rejects invalid base64 byte values', () => {
    expect(validateParameters('{"blob":{"$bytes":"not base64"}}')).toMatchObject({
      ok: false,
      error: expect.stringContaining('blob'),
    });
  });

  it.each(['AB==', 'AAB=', 'A===', 'AA=A', 'AAA'])(
    'rejects non-canonical standard base64 %s',
    (bytes) => {
      expect(validateParameters(JSON.stringify({ blob: { $bytes: bytes } }))).toMatchObject({
        ok: false,
        error: expect.stringContaining('blob'),
      });
    },
  );

  it.each(['', 'AA==', '/w==', 'AAA=', '//8=', '////'])(
    'accepts canonical standard base64 boundary %s',
    (bytes) => {
      expect(validateParameters(JSON.stringify({ blob: { $bytes: bytes } }))).toEqual({
        ok: true,
        value: { blob: { $bytes: bytes } },
      });
    },
  );

  it('rejects invalid JSON and non-object roots', () => {
    expect(validateParameters('{')).toMatchObject({ ok: false });
    expect(validateParameters('null')).toMatchObject({ ok: false });
    expect(validateParameters('[1]')).toMatchObject({ ok: false });
  });

  it('rejects unsafe integers and non-finite JSON spellings', () => {
    expect(validateParameters('{"unsafe":9007199254740992}')).toMatchObject({ ok: false });
    expect(validateParameters('{"number":NaN}')).toMatchObject({ ok: false });
    expect(validateParameters('{"number":Infinity}')).toMatchObject({ ok: false });
  });

  it('accepts Number.MAX_SAFE_INTEGER', () => {
    expect(validateParameters('{"maximum":9007199254740991}')).toEqual({
      ok: true,
      value: { maximum: Number.MAX_SAFE_INTEGER },
    });
  });
});

describe('parseBasis', () => {
  it('accepts transaction ids and packed positions', () => {
    expect(parseBasis('42')).toEqual({ ok: true, value: 42 });
    expect(parseBasis('at:99')).toEqual({ ok: true, value: 'at:99' });
  });

  it('accepts the inclusive numeric boundaries', () => {
    expect(parseBasis('9007199254740991')).toEqual({
      ok: true,
      value: Number.MAX_SAFE_INTEGER,
    });
    expect(parseBasis('at:18446744073709551615')).toEqual({
      ok: true,
      value: 'at:18446744073709551615',
    });
  });

  it('accepts an empty optional basis', () => {
    expect(parseBasis('  ')).toEqual({ ok: true, value: undefined });
  });

  it.each(['-1', '1.5', '9007199254740992', 'at:-1', 'at:1.5', 'at:18446744073709551616'])(
    'rejects invalid basis %s',
    (basis) => {
      expect(parseBasis(basis)).toMatchObject({ ok: false });
    },
  );
});

describe('parsePositiveInteger', () => {
  it('accepts a trimmed positive integer timeout', () => {
    expect(parsePositiveInteger(' 60000 ', 'Basis timeout')).toEqual({
      ok: true,
      value: 60_000,
    });
  });

  it('accepts Number.MAX_SAFE_INTEGER', () => {
    expect(parsePositiveInteger('9007199254740991', 'Basis timeout')).toEqual({
      ok: true,
      value: Number.MAX_SAFE_INTEGER,
    });
  });

  it.each(['0', '-1', '1.5', '9007199254740992'])('rejects invalid timeout %s', (timeout) => {
    expect(parsePositiveInteger(timeout, 'Basis timeout')).toEqual({
      ok: false,
      error: 'Basis timeout must be a positive integer',
    });
  });
});
