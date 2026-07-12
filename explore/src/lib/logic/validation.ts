import type { Basis, JsonScalar, QueryParameters } from '../types';

export type ValidationResult<T> = { ok: true; value: T } | { ok: false; error: string };

const BASE64_PATTERN = /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/;
const MAX_PACKED_POSITION = 18_446_744_073_709_551_615n;

function isBytes(value: object): value is { $bytes: string } {
  const record = value as Record<string, unknown>;
  const keys = Object.keys(record);

  return (
    keys.length === 1 &&
    keys[0] === '$bytes' &&
    typeof record.$bytes === 'string' &&
    BASE64_PATTERN.test(record.$bytes)
  );
}

function isJsonScalar(value: unknown): value is JsonScalar {
  if (value === null || typeof value === 'boolean' || typeof value === 'string') {
    return true;
  }

  if (typeof value === 'number') {
    return Number.isFinite(value) && (!Number.isInteger(value) || Number.isSafeInteger(value));
  }

  return typeof value === 'object' && !Array.isArray(value) && isBytes(value);
}

export function validateParameters(input: string): ValidationResult<QueryParameters> {
  let parsed: unknown;

  try {
    parsed = JSON.parse(input);
  } catch {
    return { ok: false, error: 'Parameters must be valid JSON' };
  }

  if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) {
    return { ok: false, error: 'Parameters must be a JSON object' };
  }

  for (const [field, value] of Object.entries(parsed)) {
    if (!isJsonScalar(value)) {
      return { ok: false, error: `Parameter "${field}" must be a Varve scalar` };
    }
  }

  return { ok: true, value: parsed as QueryParameters };
}

export function parseBasis(input: string): ValidationResult<Basis | undefined> {
  const value = input.trim();

  if (value === '') {
    return { ok: true, value: undefined };
  }

  if (/^\d+$/.test(value)) {
    const transactionId = Number(value);

    if (Number.isSafeInteger(transactionId)) {
      return { ok: true, value: transactionId };
    }
  }

  const packedPosition = /^at:(\d+)$/.exec(value);
  if (packedPosition && BigInt(packedPosition[1]) <= MAX_PACKED_POSITION) {
    return { ok: true, value: value as Basis };
  }

  return {
    ok: false,
    error: 'Basis must be a non-negative transaction id or at:<packed-u64>',
  };
}

export function parsePositiveInteger(input: string, fieldName = 'Value'): ValidationResult<number> {
  const value = input.trim();
  const parsed = Number(value);

  if (/^\d+$/.test(value) && Number.isSafeInteger(parsed) && parsed > 0) {
    return { ok: true, value: parsed };
  }

  return { ok: false, error: `${fieldName} must be a positive integer` };
}
