import { env } from '$env/dynamic/private';
import { json } from '@sveltejs/kit';
import type { RequestHandler } from './$types';
import { loadServerConfig } from '$lib/server/config';
import { SESSION_COOKIE_NAME, decodeSession } from '$lib/server/session';

export const GET: RequestHandler = ({ cookies }) => {
  const config = loadServerConfig(env);
  const session = cookies.get(SESSION_COOKIE_NAME);

  return json({
    displayName: config.displayName,
    target: `${config.target.host}${config.target.protocol === 'https:' ? ' (HTTPS)' : ''}`,
    authenticated: session !== undefined && decodeSession(session) !== null,
  });
};
