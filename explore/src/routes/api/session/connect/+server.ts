import { dev } from '$app/environment';
import { env } from '$env/dynamic/private';
import { loadServerConfig } from '$lib/server/config';
import { encodeSession, SESSION_COOKIE_NAME, sessionCookieOptions } from '$lib/server/session';
import { forwardVarve, isSafeBearerToken } from '$lib/server/upstream';
import type { ExplorerError } from '$lib/types';
import type { RequestHandler } from './$types';

const MAX_TOKEN_BYTES = 4096;
const MAX_COOKIE_VALUE_BYTES = 4096;
const MAX_CONNECT_BODY_BYTES = 32_768;

function jsonError(message: string, status: number): Response {
  const body: ExplorerError = { code: 'invalid_request', message, status };
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'content-type': 'application/json' },
  });
}

function isJsonContentType(value: string | null): boolean {
  return value?.split(';', 1)[0]?.trim().toLowerCase() === 'application/json';
}

async function readConnectBody(request: Request): Promise<Uint8Array | null> {
  const declared = request.headers.get('content-length');
  if (declared !== null && /^\d+$/.test(declared) && Number(declared) > MAX_CONNECT_BODY_BYTES) {
    return null;
  }
  if (request.body === null) return new Uint8Array();

  const reader = request.body.getReader();
  const chunks: Uint8Array[] = [];
  let length = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      if (length + value.byteLength > MAX_CONNECT_BODY_BYTES) {
        await reader.cancel();
        return null;
      }
      chunks.push(value);
      length += value.byteLength;
    }
  } finally {
    reader.releaseLock();
  }

  const bytes = new Uint8Array(length);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return bytes;
}

export const POST: RequestHandler = async ({ request, cookies }) => {
  if (!isJsonContentType(request.headers.get('content-type'))) {
    return jsonError('JSON request required', 415);
  }

  const bytes = await readConnectBody(request);
  if (bytes === null) return jsonError('Request body too large', 413);

  let value: unknown;
  try {
    value = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes));
  } catch {
    return jsonError('Invalid JSON request', 400);
  }
  if (
    typeof value !== 'object' ||
    value === null ||
    !('token' in value) ||
    typeof value.token !== 'string' ||
    value.token.length === 0
  ) {
    return jsonError('Token is required', 400);
  }
  if (Buffer.byteLength(value.token, 'utf8') > MAX_TOKEN_BYTES) {
    return jsonError('Token is too large', 413);
  }
  if (!isSafeBearerToken(value.token)) return jsonError('Token is invalid', 400);
  const encodedSession = encodeSession(value.token);
  if (Buffer.byteLength(encodedSession, 'utf8') > MAX_COOKIE_VALUE_BYTES) {
    return jsonError('Token cannot be stored in a session cookie', 400);
  }

  const config = loadServerConfig(env);
  const response = await forwardVarve({
    config,
    path: '/v1/status',
    method: 'GET',
    token: value.token,
  });
  if (!response.ok) return response;

  cookies.set(SESSION_COOKIE_NAME, encodedSession, sessionCookieOptions(dev));
  return response;
};
