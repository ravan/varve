import { dev } from '$app/environment';
import type { RequestHandler } from './$types';
import { SESSION_COOKIE_NAME, sessionCookieOptions } from '$lib/server/session';

export const DELETE: RequestHandler = ({ cookies }) => {
  cookies.delete(SESSION_COOKIE_NAME, sessionCookieOptions(dev));
  return new Response(null, { status: 204 });
};
