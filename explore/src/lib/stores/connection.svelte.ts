import type { ExplorerErrorCode } from '$lib/types';
import { createSessionOperationCoordinator } from '$lib/logic/session-operations';

export interface FetchLike {
  (input: string, init?: RequestInit): Promise<Response>;
}

export interface ConnectionConfig {
  readonly displayName: string;
  readonly target: string;
  readonly authenticated: boolean;
}

export type ConnectionSession =
  | 'unknown'
  | 'checking'
  | 'connecting'
  | 'authenticated'
  | 'degraded'
  | 'unauthenticated'
  | 'error';

export interface ConnectionFailure {
  readonly code: ExplorerErrorCode;
  readonly status?: number;
}

interface RequestFailure {
  readonly code: ExplorerErrorCode;
  readonly status?: number;
}

export function createConnectionStore(fetcher: FetchLike) {
  let config = $state<ConnectionConfig | null>(null);
  let health = $state<unknown>(null);
  let status = $state<unknown>(null);
  let session = $state<ConnectionSession>('unknown');
  let error = $state<ConnectionFailure | null>(null);
  let degraded = $state(false);
  let refreshSequence = 0;
  const sessionOperations = createSessionOperationCoordinator();

  async function refresh(): Promise<void> {
    const sequence = ++refreshSequence;
    if (session === 'unknown') session = 'checking';
    error = null;

    try {
      const [configResult, healthResult] = await Promise.allSettled([
        requestJson(fetcher, '/api/config'),
        requestHealth(fetcher),
      ]);
      if (sequence !== refreshSequence) return;
      if (healthResult.status === 'fulfilled') {
        health = healthResult.value;
        degraded = isDegradedHealth(healthResult.value);
      } else {
        health = null;
        degraded = false;
        error = normalizeFailure(healthResult.reason);
      }
      if (configResult.status === 'rejected') throw configResult.reason;

      const nextConfig = configResult.value;
      if (!isConnectionConfig(nextConfig)) throw requestFailure('malformed_response');

      config = nextConfig;
      if (!nextConfig.authenticated) {
        status = null;
        session = 'unauthenticated';
        return;
      }

      const nextStatus = await requestJson(fetcher, '/api/varve/status');
      if (sequence !== refreshSequence) return;
      status = nextStatus;
      degraded = degraded || isDegradedStatus(nextStatus);
      session = error !== null ? 'error' : degraded ? 'degraded' : 'authenticated';
    } catch (cause) {
      if (sequence !== refreshSequence) return;
      const failure = normalizeFailure(cause);
      error = failure;
      if (failure.code === 'unauthorized') {
        status = null;
        session = 'unauthenticated';
      } else {
        session = 'error';
      }
    }
  }

  async function connect(token: string): Promise<void> {
    const sequence = ++refreshSequence;
    session = 'connecting';
    error = null;

    await sessionOperations.run(async () => {
      try {
        const response = await fetcher('/api/session/connect', {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ token }),
        });
        if (!response.ok) throw await responseFailure(response);
        if (sequence !== refreshSequence) return;
        await refresh();
      } catch (cause) {
        if (sequence !== refreshSequence) return;
        const failure = normalizeFailure(cause);
        error = failure;
        status = null;
        session = failure.code === 'unauthorized' ? 'unauthenticated' : 'error';
      }
    });
  }

  async function disconnect(): Promise<void> {
    const sequence = ++refreshSequence;
    error = null;

    await sessionOperations.run(async () => {
      try {
        const response = await fetcher('/api/session', { method: 'DELETE' });
        if (!response.ok) throw await responseFailure(response);
        if (sequence !== refreshSequence) return;
        config = config === null ? null : { ...config, authenticated: false };
        status = null;
        session = 'unauthenticated';
      } catch (cause) {
        if (sequence !== refreshSequence) return;
        error = normalizeFailure(cause);
        session = 'error';
      }
    });
  }

  return {
    get config() {
      return config;
    },
    get health() {
      return health;
    },
    get status() {
      return status;
    },
    get session() {
      return session;
    },
    get error() {
      return error;
    },
    get degraded() {
      return degraded;
    },
    connect,
    disconnect,
    refresh,
  };
}

export type ConnectionStore = ReturnType<typeof createConnectionStore>;

async function requestJson(fetcher: FetchLike, input: string): Promise<unknown> {
  let response: Response;
  try {
    response = await fetcher(input, { headers: { accept: 'application/json' } });
  } catch {
    throw requestFailure('network');
  }
  if (!response.ok) throw await responseFailure(response);
  try {
    return await response.json();
  } catch {
    throw requestFailure('malformed_response', response.status);
  }
}

async function requestHealth(fetcher: FetchLike): Promise<unknown> {
  let response: Response;
  try {
    response = await fetcher('/api/varve/health', { headers: { accept: 'application/json' } });
  } catch {
    throw requestFailure('network');
  }
  let value: unknown;
  try {
    value = await response.json();
  } catch {
    throw requestFailure('malformed_response', response.status);
  }
  if ((response.ok && isHealthyHealth(value)) || isDegradedHealth(value)) return value;
  if (isRecord(value) && isExplorerErrorCode(value.code)) {
    throw requestFailure(value.code, response.status);
  }
  throw requestFailure('malformed_response', response.status);
}

async function responseFailure(response: Response): Promise<RequestFailure> {
  try {
    const value: unknown = await response.json();
    if (isRecord(value) && isExplorerErrorCode(value.code)) {
      return requestFailure(value.code, response.status);
    }
  } catch {
    // The response code still provides a safe fallback category.
  }
  return requestFailure(response.status === 401 ? 'unauthorized' : 'network', response.status);
}

function requestFailure(code: ExplorerErrorCode, status?: number): RequestFailure {
  return status === undefined ? { code } : { code, status };
}

function normalizeFailure(value: unknown): ConnectionFailure {
  if (isRecord(value) && isExplorerErrorCode(value.code)) {
    return typeof value.status === 'number'
      ? { code: value.code, status: value.status }
      : { code: value.code };
  }
  return { code: 'network' };
}

function isConnectionConfig(value: unknown): value is ConnectionConfig {
  return (
    isRecord(value) &&
    typeof value.displayName === 'string' &&
    typeof value.target === 'string' &&
    typeof value.authenticated === 'boolean'
  );
}

function isDegradedHealth(value: unknown): boolean {
  return isRecord(value) && value.status === 'degraded';
}

function isHealthyHealth(value: unknown): boolean {
  return isRecord(value) && value.status === 'ok';
}

function isDegradedStatus(value: unknown): boolean {
  if (!isRecord(value)) return true;
  if (value.follower_error !== null && value.follower_error !== '') return true;
  if (!isRecord(value.probe)) return true;
  return value.probe.verdict !== 'supported' && value.probe.verdict !== 'unsupported';
}

function isExplorerErrorCode(value: unknown): value is ExplorerErrorCode {
  return (
    value === 'unauthorized' ||
    value === 'invalid_request' ||
    value === 'not_acceptable' ||
    value === 'basis_timeout' ||
    value === 'backpressure' ||
    value === 'misdirected_request' ||
    value === 'writer_unavailable' ||
    value === 'writer_fenced' ||
    value === 'follower_failed' ||
    value === 'internal' ||
    value === 'network' ||
    value === 'timeout' ||
    value === 'cancelled' ||
    value === 'malformed_response'
  );
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}
