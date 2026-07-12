import { dev } from '$app/environment';
import { expect, it, vi } from 'vitest';
import type { Cookies } from '@sveltejs/kit';
import { SESSION_COOKIE_NAME, sessionCookieOptions } from '$lib/server/session';
import { DELETE } from './+server';

it('deletes the session with matching cookie options and returns 204', async () => {
  const deleteCookie = vi.fn();

  const response = await DELETE({
    cookies: { delete: deleteCookie } as unknown as Cookies,
  } as Parameters<typeof DELETE>[0]);

  expect(deleteCookie).toHaveBeenCalledOnce();
  expect(deleteCookie).toHaveBeenCalledWith(
    SESSION_COOKIE_NAME,
    sessionCookieOptions(dev),
  );
  expect(response.status).toBe(204);
  expect(await response.text()).toBe('');
});
