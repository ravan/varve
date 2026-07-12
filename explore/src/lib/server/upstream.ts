import type { ExplorerError, ExplorerErrorCode } from '$lib/types';
import type { ServerConfig } from './config';

const JSON_CONTENT_TYPE = 'application/json';
const ALLOWED_PATH_METHODS = new Map<string, ForwardMethod>([
  ['/healthz', 'GET'],
  ['/v1/status', 'GET'],
  ['/v1/query', 'POST'],
  ['/v1/tx', 'POST'],
]);

type ForwardMethod = 'GET' | 'POST';
type Fetch = typeof globalThis.fetch;

export interface ErrorResponse {
  code: string;
  message: string;
  writer?: string | null;
}

export interface ForwardInput {
  config: ServerConfig;
  path: string;
  method: ForwardMethod;
  token?: string;
  request?: Request;
  fetch?: Fetch;
}

interface StableError {
  status: number;
  message: string;
}

const STABLE_UPSTREAM_ERRORS = new Map<ExplorerErrorCode, StableError>([
  ['unauthorized', { status: 401, message: 'Authentication required' }],
  ['invalid_request', { status: 400, message: 'Invalid request' }],
  ['not_acceptable', { status: 406, message: 'Requested response format is not supported' }],
  ['basis_timeout', { status: 408, message: 'Basis wait timed out' }],
  ['backpressure', { status: 429, message: 'Varve is busy; retry later' }],
  ['misdirected_request', { status: 421, message: 'Writer routing failed' }],
  ['writer_unavailable', { status: 503, message: 'Writer is unavailable' }],
  ['writer_fenced', { status: 503, message: 'Writer lease changed; retry' }],
  ['follower_failed', { status: 503, message: 'Query follower failed' }],
  ['internal', { status: 500, message: 'Varve request failed' }],
]);

function errorResponse(
  code: ExplorerErrorCode,
  message: string,
  status: number,
  retryAfterMs?: number,
): Response {
  const body: ExplorerError = { code, message, status };
  if (retryAfterMs !== undefined) body.retryAfterMs = retryAfterMs;

  const headers = new Headers({ 'content-type': JSON_CONTENT_TYPE });
  if (retryAfterMs !== undefined) headers.set('retry-after', String(retryAfterMs / 1000));
  return new Response(JSON.stringify(body), { status, headers });
}

function parseRetryAfter(value: string | null): number | undefined {
  if (value === null || !/^\d+$/.test(value)) return undefined;
  const seconds = Number(value);
  if (!Number.isSafeInteger(seconds) || seconds > 86_400) return undefined;
  return seconds * 1000;
}

function isErrorResponse(value: unknown): value is ErrorResponse {
  return (
    typeof value === 'object' &&
    value !== null &&
    'code' in value &&
    typeof value.code === 'string' &&
    'message' in value &&
    typeof value.message === 'string' &&
    (!('writer' in value) ||
      value.writer === undefined ||
      value.writer === null ||
      typeof value.writer === 'string')
  );
}

export function isSafeBearerToken(token: string): boolean {
  if (Buffer.byteLength(token, 'utf8') > 4096) return false;
  for (let index = 0; index < token.length; index += 1) {
    const code = token.charCodeAt(index);
    if (code < 33 || code > 126) return false;
  }
  return true;
}

export function normalizeUpstreamError(
  upstreamStatus: number,
  value: unknown,
  retryAfter: string | null = null,
): Response {
  if (upstreamStatus === 401) {
    return errorResponse('unauthorized', 'Authentication required', 401);
  }
  if (!isErrorResponse(value)) {
    return errorResponse('malformed_response', 'Varve returned an invalid response', 502);
  }

  const code = value.code as ExplorerErrorCode;
  const stable = STABLE_UPSTREAM_ERRORS.get(code);
  if (stable === undefined) {
    return errorResponse('malformed_response', 'Varve returned an invalid response', 502);
  }
  const retryAfterMs = code === 'backpressure' ? parseRetryAfter(retryAfter) : undefined;
  return errorResponse(code, stable.message, stable.status, retryAfterMs);
}

function isJsonContentType(value: string | null): boolean {
  if (value === null) return false;
  return value.split(';', 1)[0]?.trim().toLowerCase() === JSON_CONTENT_TYPE;
}

class BodyTooLargeError extends Error {}

async function readBoundedBody(
  source: Request | Response,
  maximumBytes: number,
): Promise<ArrayBuffer> {
  const declaredLength = source.headers.get('content-length');
  if (declaredLength !== null && /^\d+$/.test(declaredLength)) {
    const parsed = Number(declaredLength);
    if (!Number.isSafeInteger(parsed) || parsed > maximumBytes) throw new BodyTooLargeError();
  }

  if (source.body === null) return new ArrayBuffer(0);
  const reader = source.body.getReader();
  const chunks: Uint8Array[] = [];
  let length = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      if (length + value.byteLength > maximumBytes) {
        await reader.cancel();
        throw new BodyTooLargeError();
      }
      chunks.push(value);
      length += value.byteLength;
    }
  } finally {
    reader.releaseLock();
  }

  const body = new Uint8Array(length);
  let offset = 0;
  for (const chunk of chunks) {
    body.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return body.buffer;
}

function parseJson(bytes: ArrayBuffer): unknown {
  return JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(new Uint8Array(bytes)));
}

function writerUrl(value: unknown, allowedOrigins: Set<string>): URL | null {
  if (!isErrorResponse(value) || typeof value.writer !== 'string') return null;

  try {
    const advertised = new URL(value.writer);
    if (
      (advertised.protocol !== 'http:' && advertised.protocol !== 'https:') ||
      advertised.username !== '' ||
      advertised.password !== '' ||
      !allowedOrigins.has(advertised.origin)
    ) {
      return null;
    }
    return new URL('/v1/tx', advertised.origin);
  } catch {
    return null;
  }
}

function upstreamUrl(config: ServerConfig, path: string): URL | null {
  if (!ALLOWED_PATH_METHODS.has(path)) return null;
  return new URL(path, config.target.origin);
}

async function requestBody(input: ForwardInput): Promise<ArrayBuffer | Response | undefined> {
  if (input.method === 'GET') return undefined;
  if (
    input.request === undefined ||
    !isJsonContentType(input.request.headers.get('content-type'))
  ) {
    return errorResponse('invalid_request', 'JSON request required', 415);
  }

  let bytes: ArrayBuffer;
  try {
    bytes = await readBoundedBody(input.request, input.config.maxRequestBytes);
  } catch (error) {
    if (error instanceof BodyTooLargeError) {
      return errorResponse('invalid_request', 'Request body too large', 413);
    }
    return errorResponse('invalid_request', 'Invalid request body', 400);
  }

  try {
    parseJson(bytes);
  } catch {
    return errorResponse('invalid_request', 'Invalid JSON request', 400);
  }
  return bytes;
}

export async function forwardVarve(input: ForwardInput): Promise<Response> {
  const expectedMethod = ALLOWED_PATH_METHODS.get(input.path);
  const url = upstreamUrl(input.config, input.path);
  if (expectedMethod === undefined || expectedMethod !== input.method || url === null) {
    return errorResponse('invalid_request', 'Unsupported Varve request', 400);
  }

  const body = await requestBody(input);
  if (body instanceof Response) return body;

  if (input.token !== undefined && !isSafeBearerToken(input.token)) {
    return errorResponse('unauthorized', 'Authentication required', 401);
  }

  const fetchImplementation = input.fetch ?? globalThis.fetch;
  let headers: Headers;
  try {
    headers = new Headers({ accept: JSON_CONTENT_TYPE });
    if (input.token !== undefined) headers.set('authorization', `Bearer ${input.token}`);
    if (body !== undefined) headers.set('content-type', JSON_CONTENT_TYPE);
  } catch {
    return errorResponse('unauthorized', 'Authentication required', 401);
  }
  const signal = AbortSignal.timeout(input.config.timeoutMs);

  const send = async (target: URL): Promise<Response> => {
    return fetchImplementation(target, {
      method: input.method,
      headers,
      body,
      redirect: 'manual',
      signal,
    });
  };

  let response: Response;
  try {
    response = await send(url);
  } catch {
    if (signal.aborted) return errorResponse('timeout', 'Varve request timed out', 504);
    return errorResponse('network', 'Unable to reach Varve', 502);
  }

  for (let attempt = 0; ; attempt += 1) {
    if (response.status === 401) return normalizeUpstreamError(401, null);

    if (!isJsonContentType(response.headers.get('content-type'))) {
      return errorResponse('malformed_response', 'Varve returned an invalid response', 502);
    }

    let responseBytes: ArrayBuffer;
    try {
      responseBytes = await readBoundedBody(response, input.config.maxRequestBytes);
    } catch (error) {
      if (signal.aborted) return errorResponse('timeout', 'Varve request timed out', 504);
      if (error instanceof BodyTooLargeError) {
        return errorResponse('malformed_response', 'Varve returned an invalid response', 502);
      }
      return errorResponse('network', 'Unable to reach Varve', 502);
    }

    let parsed: unknown;
    try {
      parsed = parseJson(responseBytes);
    } catch {
      return errorResponse('malformed_response', 'Varve returned an invalid response', 502);
    }

    if (
      input.path === '/healthz' &&
      response.status === 503 &&
      typeof parsed === 'object' &&
      parsed !== null &&
      'status' in parsed &&
      parsed.status === 'degraded'
    ) {
      return new Response(JSON.stringify({ status: 'degraded', error: 'follower stopped' }), {
        status: 503,
        headers: { 'content-type': JSON_CONTENT_TYPE },
      });
    }

    if (
      response.status === 421 &&
      input.path === '/v1/tx' &&
      input.method === 'POST' &&
      attempt === 0
    ) {
      const retryUrl = writerUrl(parsed, input.config.allowedWriterOrigins);
      if (retryUrl !== null) {
        try {
          response = await send(retryUrl);
          continue;
        } catch {
          if (signal.aborted) return errorResponse('timeout', 'Varve request timed out', 504);
          return errorResponse('network', 'Unable to reach Varve', 502);
        }
      }
    }

    if (!response.ok) {
      return normalizeUpstreamError(response.status, parsed, response.headers.get('retry-after'));
    }

    return new Response(responseBytes, {
      status: response.status,
      headers: { 'content-type': response.headers.get('content-type') ?? JSON_CONTENT_TYPE },
    });
  }
}
