import { expect, it, vi } from 'vitest';
import { handle } from './hooks.server';

it('assigns a request ID and logs only safe request metadata', async () => {
  const info = vi.spyOn(console, 'info').mockImplementation(() => {});
  const request = new Request('https://explorer.example.test/api/varve/query?token=secret', {
    method: 'POST',
    headers: {
      authorization: 'Bearer secret',
      cookie: 'session=secret',
    },
    body: 'upstream details secret',
  });
  const locals = {} as App.Locals;
  const resolve = vi.fn().mockResolvedValue(new Response('{}', { status: 202 }));

  const response = await handle({
    event: { request, url: new URL(request.url), locals },
    resolve,
  } as unknown as Parameters<typeof handle>[0]);

  expect(locals.requestId).toMatch(/^[0-9a-f-]{36}$/);
  expect(response.headers.get('x-request-id')).toBe(locals.requestId);
  expect(info).toHaveBeenCalledOnce();
  expect(info.mock.calls[0]?.[0]).toBe('request');
  expect(info.mock.calls[0]?.[1]).toEqual({
    requestId: locals.requestId,
    method: 'POST',
    route: '/api/varve/query',
    status: 202,
    durationMs: expect.any(Number),
  });
  expect(JSON.stringify(info.mock.calls)).not.toContain('secret');
  info.mockRestore();
});
