import { env } from '$env/dynamic/private';
import { loadServerConfig } from '$lib/server/config';
import { forwardVarve } from '$lib/server/upstream';
import type { RequestHandler } from './$types';

export const GET: RequestHandler = () => {
  const config = loadServerConfig(env);
  return forwardVarve({ config, path: '/healthz', method: 'GET' });
};
