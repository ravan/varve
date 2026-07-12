const DEFAULT_TARGET = 'http://127.0.0.1:8080';
const DEFAULT_DISPLAY_NAME = 'Local Varve';
const DEFAULT_TIMEOUT_MS = 10_000;
const MAX_TIMEOUT_MS = 120_000;
const DEFAULT_MAX_REQUEST_BYTES = 1024 * 1024;
const MAX_MAX_REQUEST_BYTES = 16 * 1024 * 1024;

type Environment = Record<string, string | undefined>;

export interface ServerConfig {
  target: URL;
  displayName: string;
  allowedWriterOrigins: Set<string>;
  timeoutMs: number;
  maxRequestBytes: number;
  production: boolean;
}

function parseHttpUrl(value: string, variable: string): URL {
  let url: URL;
  try {
    url = new URL(value);
  } catch {
    throw new Error(`${variable} must be an absolute HTTP(S) URL`);
  }

  if (
    (url.protocol !== 'http:' && url.protocol !== 'https:') ||
    url.username !== '' ||
    url.password !== '' ||
    url.hash !== ''
  ) {
    throw new Error(`${variable} must be a credential-free HTTP(S) URL without a fragment`);
  }

  return url;
}

function parseWriterOrigin(value: string): string {
  const url = parseHttpUrl(value, 'VARVE_ALLOWED_WRITER_ORIGINS');
  if (url.pathname !== '/' || url.search !== '') {
    throw new Error('VARVE_ALLOWED_WRITER_ORIGINS entries must be origins');
  }
  return url.origin;
}

function parseBoundedInteger(
  value: string | undefined,
  variable: string,
  fallback: number,
  maximum: number,
): number {
  if (value === undefined) return fallback;

  const normalized = value.trim();
  if (!/^\d+$/.test(normalized)) {
    throw new Error(`${variable} must be an integer`);
  }

  const parsed = Number(normalized);
  if (!Number.isSafeInteger(parsed) || parsed < 1 || parsed > maximum) {
    throw new Error(`${variable} must be between 1 and ${maximum}`);
  }
  return parsed;
}

export function loadServerConfig(environment: Environment): ServerConfig {
  const production = environment.NODE_ENV === 'production';
  const targetValue = environment.VARVE_URL?.trim();
  if (production && !targetValue) {
    throw new Error('VARVE_URL is required in production');
  }

  const target = parseHttpUrl(targetValue || DEFAULT_TARGET, 'VARVE_URL');
  const allowedWriterOrigins = new Set([target.origin]);
  const configuredOrigins = environment.VARVE_ALLOWED_WRITER_ORIGINS;
  if (configuredOrigins !== undefined && configuredOrigins.trim() !== '') {
    for (const entry of configuredOrigins.split(',')) {
      const origin = entry.trim();
      if (origin === '') {
        throw new Error('VARVE_ALLOWED_WRITER_ORIGINS contains an empty entry');
      }
      allowedWriterOrigins.add(parseWriterOrigin(origin));
    }
  }

  return {
    target,
    displayName: environment.VARVE_DISPLAY_NAME?.trim() || DEFAULT_DISPLAY_NAME,
    allowedWriterOrigins,
    timeoutMs: parseBoundedInteger(
      environment.VARVE_TIMEOUT_MS,
      'VARVE_TIMEOUT_MS',
      DEFAULT_TIMEOUT_MS,
      MAX_TIMEOUT_MS,
    ),
    maxRequestBytes: parseBoundedInteger(
      environment.VARVE_MAX_REQUEST_BYTES,
      'VARVE_MAX_REQUEST_BYTES',
      DEFAULT_MAX_REQUEST_BYTES,
      MAX_MAX_REQUEST_BYTES,
    ),
    production,
  };
}
