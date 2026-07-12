import { env } from '$env/dynamic/private';
import { loadServerConfig } from '$lib/server/config';
import { decodeSession, SESSION_COOKIE_NAME } from '$lib/server/session';
import { forwardVarve, isSafeBearerToken, normalizeUpstreamError } from '$lib/server/upstream';
import type { RequestHandler } from './$types';

export const POST: RequestHandler = ({ cookies, request }) => {
  const config = loadServerConfig(env);
  const encoded = cookies.get(SESSION_COOKIE_NAME);
  const token = encoded === undefined ? null : decodeSession(encoded);
  if (token === null || !isSafeBearerToken(token)) {
    return normalizeUpstreamError(401, null);
  }

  return forwardVarve({ config, path: '/v1/tx', method: 'POST', token, request });
};
