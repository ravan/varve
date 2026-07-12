import { expect, it, vi } from 'vitest';
import type { Cookies } from '@sveltejs/kit';
import { SESSION_COOKIE_NAME } from '$lib/server/session';
import { GET } from './+server';

vi.mock('$env/dynamic/private', () => ({
  env: {
    NODE_ENV: 'production',
    VARVE_URL: 'https://query.example.test/internal?tenant=secret',
    VARVE_DISPLAY_NAME: 'Review Varve',
  },
}));

it('redacts target internals and reports malformed cookies as unauthenticated', async () => {
  const get = vi.fn(() => 'not canonical');

  const response = await GET({ cookies: { get } as unknown as Cookies } as Parameters<
    typeof GET
  >[0]);

  expect(response.status).toBe(200);
  await expect(response.json()).resolves.toEqual({
    displayName: 'Review Varve',
    target: 'query.example.test (HTTPS)',
    authenticated: false,
  });
  expect(get).toHaveBeenCalledWith(SESSION_COOKIE_NAME);
});
