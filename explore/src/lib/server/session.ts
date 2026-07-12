import type { Cookies } from '@sveltejs/kit';

export const SESSION_COOKIE_NAME = 'varve_explorer_session';

export type CookieSerializeOptions = Parameters<Cookies['set']>[2];

export function encodeSession(token: string): string {
  return Buffer.from(JSON.stringify({ token }), 'utf8').toString('base64url');
}

export function decodeSession(value: string): string | null {
  if (!/^[A-Za-z0-9_-]+$/.test(value)) return null;

  try {
    const bytes = Buffer.from(value, 'base64url');
    if (bytes.toString('base64url') !== value) return null;

    const decoded: unknown = JSON.parse(bytes.toString('utf8'));
    if (
      typeof decoded !== 'object' ||
      decoded === null ||
      !('token' in decoded) ||
      typeof decoded.token !== 'string'
    ) {
      return null;
    }
    return decoded.token;
  } catch {
    return null;
  }
}

export function sessionCookieOptions(dev: boolean): CookieSerializeOptions {
  return {
    httpOnly: true,
    sameSite: 'strict',
    secure: !dev,
    path: '/',
  };
}
