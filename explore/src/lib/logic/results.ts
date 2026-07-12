import type { QueryResponse, TxReceipt } from '../types';

export interface MissingCell {
  readonly kind: 'missing';
}

export interface ValueCell {
  readonly kind: 'value';
  readonly value: unknown;
}

export type NormalizedCell = MissingCell | ValueCell;
export type NormalizedRow = Readonly<Record<string, NormalizedCell>>;

export interface NormalizedQueryResponse {
  readonly columns: readonly string[];
  readonly rows: readonly NormalizedRow[];
  readonly raw: unknown;
}

export interface NormalizedTxReceipt extends TxReceipt {
  readonly side_effects: Readonly<Record<string, number>>;
  readonly raw: unknown;
}

export type SortDirection = 'asc' | 'desc';

const SIDE_EFFECT_KEYS = [
  'nodes_created',
  'nodes_deleted',
  'relationships_created',
  'relationships_deleted',
  'properties_set',
  'properties_removed',
  'labels_added',
  'labels_removed',
] as const;

const BASE64_PATTERN = /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/;
const BASE64_ALPHABET = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/';
const MISSING_CELL: MissingCell = Object.freeze({ kind: 'missing' });

export function normalizeQueryResponse(response: QueryResponse | unknown): NormalizedQueryResponse {
  if (!isRecord(response) || !Array.isArray(response.rows)) {
    throw new TypeError('Query response rows must be an array');
  }

  const sourceRows = response.rows;
  const columns: string[] = [];
  const seenColumns = new Set<string>();

  sourceRows.forEach((row, index) => {
    if (!isRecord(row)) {
      throw new TypeError(`Query response row ${index + 1} must be an object`);
    }
    for (const column of Object.keys(row)) {
      if (!seenColumns.has(column)) {
        seenColumns.add(column);
        columns.push(column);
      }
    }
  });

  const normalizedRows = sourceRows.map((row) =>
    Object.freeze(
      Object.fromEntries(
        columns.map((column) => [
          column,
          Object.prototype.hasOwnProperty.call(row, column)
            ? Object.freeze({
                kind: 'value' as const,
                value: cloneAndFreezeJson(row[column], new Set()),
              })
            : MISSING_CELL,
        ]),
      ),
    ),
  );

  return Object.freeze({
    columns: Object.freeze(columns),
    rows: Object.freeze(normalizedRows),
    raw: response,
  });
}

export function normalizeTxReceipt(response: TxReceipt | unknown): NormalizedTxReceipt {
  if (!isRecord(response)) {
    throw new TypeError('Transaction receipt must be an object');
  }

  const txId = requireNonNegativeInteger(response.tx_id, 'tx_id');
  const systemTime = requireString(response.system_time, 'system_time');
  const systemTimeUs = requireInteger(response.system_time_us, 'system_time_us');
  const basis = requireNonNegativeInteger(response.basis, 'basis');
  const sourceEffects = response.side_effects;
  if (sourceEffects !== undefined && !isRecord(sourceEffects)) {
    throw new TypeError('Transaction receipt side_effects must be an object');
  }

  const effects: Record<string, number> = {};
  for (const key of SIDE_EFFECT_KEYS) {
    effects[key] = sourceEffects?.[key] === undefined ? 0 : requireEffect(sourceEffects[key], key);
  }
  if (sourceEffects) {
    for (const [key, value] of Object.entries(sourceEffects)) {
      if (!Object.prototype.hasOwnProperty.call(effects, key)) {
        effects[key] = requireEffect(value, key);
      }
    }
  }

  return Object.freeze({
    tx_id: txId,
    system_time: systemTime,
    system_time_us: systemTimeUs,
    basis,
    side_effects: Object.freeze(effects),
    raw: response,
  });
}

export function formatCell(cell: NormalizedCell): string {
  if (cell.kind === 'missing') {
    return 'Missing';
  }

  const value = cell.value;
  if (value === null) {
    return 'null';
  }
  if (isCanonicalBytesObject(value)) {
    const preview = value.$bytes.length > 24 ? `${value.$bytes.slice(0, 24)}…` : value.$bytes;
    return `bytes · ${decodedBase64Size(value.$bytes)} B · ${preview}`;
  }
  if (typeof value === 'object') {
    return stringifyCanonical(value, 0);
  }
  return String(value);
}

export function sortRows<T extends NormalizedRow>(
  rows: readonly T[],
  column: string,
  direction: SortDirection = 'asc',
): T[] {
  const multiplier = direction === 'asc' ? 1 : -1;
  return rows
    .map((row, index) => ({ row, index }))
    .sort((left, right) => {
      const comparison = compareCells(
        left.row[column] ?? MISSING_CELL,
        right.row[column] ?? MISSING_CELL,
      );
      return comparison === 0 ? left.index - right.index : comparison * multiplier;
    })
    .map(({ row }) => row);
}

export function pageRows<T>(rows: readonly T[], page: number, pageSize = 50): T[] {
  if (!Number.isSafeInteger(page) || page < 1) {
    throw new RangeError('page must be a positive integer');
  }
  if (!Number.isSafeInteger(pageSize) || pageSize < 1) {
    throw new RangeError('page size must be a positive integer');
  }
  const start = (page - 1) * pageSize;
  return rows.slice(start, start + pageSize);
}

export function copyableJson(value: unknown): string {
  return stringifyCanonical(value, 2);
}

function compareCells(left: NormalizedCell, right: NormalizedCell): number {
  const leftRank = cellRank(left);
  const rightRank = cellRank(right);
  if (leftRank !== rightRank) {
    return leftRank - rightRank;
  }
  if (left.kind === 'missing' || right.kind === 'missing') {
    return 0;
  }

  const leftValue = left.value;
  const rightValue = right.value;
  if (typeof leftValue === 'boolean' && typeof rightValue === 'boolean') {
    return Number(leftValue) - Number(rightValue);
  }
  if (typeof leftValue === 'number' && typeof rightValue === 'number') {
    return leftValue < rightValue ? -1 : leftValue > rightValue ? 1 : 0;
  }
  if (typeof leftValue === 'string' && typeof rightValue === 'string') {
    return compareText(leftValue, rightValue);
  }
  if (leftValue === null || rightValue === null) {
    return 0;
  }
  return compareText(stringifyCanonical(leftValue, 0), stringifyCanonical(rightValue, 0));
}

function cellRank(cell: NormalizedCell): number {
  if (cell.kind === 'missing') return 0;
  if (cell.value === null) return 1;
  if (typeof cell.value === 'boolean') return 2;
  if (typeof cell.value === 'number') return 3;
  if (typeof cell.value === 'string') return 4;
  return 5;
}

function compareText(left: string, right: string): number {
  return left < right ? -1 : left > right ? 1 : 0;
}

function stringifyCanonical(value: unknown, indentation: number): string {
  const result = JSON.stringify(canonicalize(value, new Set()), null, indentation);
  return result ?? 'null';
}

function canonicalize(value: unknown, ancestors: Set<object>): unknown {
  if (value === null || typeof value !== 'object') {
    return value;
  }
  if (ancestors.has(value)) {
    throw new TypeError('Cannot format circular JSON');
  }

  ancestors.add(value);
  let result: unknown;
  if (Array.isArray(value)) {
    result = value.map((item) => canonicalize(item, ancestors));
  } else {
    result = Object.fromEntries(
      Object.keys(value)
        .sort(compareText)
        .filter((key) => {
          const item = (value as Record<string, unknown>)[key];
          return item !== undefined && typeof item !== 'function' && typeof item !== 'symbol';
        })
        .map((key) => [key, canonicalize((value as Record<string, unknown>)[key], ancestors)]),
    );
  }
  ancestors.delete(value);
  return result;
}

function cloneAndFreezeJson(value: unknown, ancestors: Set<object>): unknown {
  if (value === null || typeof value !== 'object') {
    return value;
  }
  if (ancestors.has(value)) {
    throw new TypeError('Cannot normalize circular JSON');
  }

  ancestors.add(value);
  const clone = Array.isArray(value)
    ? value.map((item) => cloneAndFreezeJson(item, ancestors))
    : Object.fromEntries(
        Object.keys(value).map((key) => [
          key,
          cloneAndFreezeJson((value as Record<string, unknown>)[key], ancestors),
        ]),
      );
  ancestors.delete(value);
  return Object.freeze(clone);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

export function isCanonicalBytesObject(value: unknown): value is { $bytes: string } {
  return (
    isRecord(value) &&
    Object.keys(value).length === 1 &&
    typeof value.$bytes === 'string' &&
    isCanonicalBase64(value.$bytes)
  );
}

function isCanonicalBase64(value: string): boolean {
  if (!BASE64_PATTERN.test(value)) return false;
  if (value.endsWith('==')) {
    return (BASE64_ALPHABET.indexOf(value[value.length - 3]) & 0b1111) === 0;
  }
  if (value.endsWith('=')) {
    return (BASE64_ALPHABET.indexOf(value[value.length - 2]) & 0b11) === 0;
  }
  return true;
}

function decodedBase64Size(value: string): number {
  if (value === '') return 0;
  const padding = value.endsWith('==') ? 2 : value.endsWith('=') ? 1 : 0;
  return (value.length / 4) * 3 - padding;
}

function requireString(value: unknown, field: string): string {
  if (typeof value !== 'string') {
    throw new TypeError(`Transaction receipt ${field} must be a string`);
  }
  return value;
}

function requireInteger(value: unknown, field: string): number {
  if (!Number.isSafeInteger(value)) {
    throw new TypeError(`Transaction receipt ${field} must be a safe integer`);
  }
  return value as number;
}

function requireNonNegativeInteger(value: unknown, field: string): number {
  const integer = requireInteger(value, field);
  if (integer < 0) {
    throw new TypeError(`Transaction receipt ${field} must be non-negative`);
  }
  return integer;
}

function requireEffect(value: unknown, field: string): number {
  const effect = requireNonNegativeInteger(value, `side_effects.${field}`);
  return effect;
}
