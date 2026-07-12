import type { Cookies } from '@sveltejs/kit';
import { beforeEach, expect, it, vi } from 'vitest';
import { encodeSession, SESSION_COOKIE_NAME, sessionCookieOptions } from '$lib/server/session';

const forwardVarve = vi.hoisted(() => vi.fn());

vi.mock('$env/dynamic/private', () => ({
  env: {
    NODE_ENV: 'production',
    VARVE_URL: 'https://query.example.test/internal?tenant=secret',
    VARVE_ALLOWED_WRITER_ORIGINS: 'https://writer.example.test',
  },
}));

vi.mock('$app/environment', () => ({ dev: false }));

vi.mock('$lib/server/upstream', async (importOriginal) => {
  const actual = await importOriginal<typeof import('$lib/server/upstream')>();
  return { ...actual, forwardVarve };
});

import { POST as connect } from '../session/connect/+server';
import { GET as health } from './health/+server';
import { POST as query } from './query/+server';
import { GET as status } from './status/+server';
import { POST as tx } from './tx/+server';

function cookies(value?: string) {
  return {
    get: vi.fn((name: string) => (name === SESSION_COOKIE_NAME ? value : undefined)),
    set: vi.fn(),
  } as unknown as Cookies;
}

function event(request: Request, cookieJar = cookies()) {
  return { request, cookies: cookieJar };
}

type TestHandler = (event: unknown) => Response | Promise<Response>;

beforeEach(() => {
  forwardVarve.mockReset();
});

it('connects only after an authenticated status check succeeds', async () => {
  const upstream = new Response(JSON.stringify({ roles: ['writer'], applied_tx_id: 4 }), {
    status: 200,
    headers: { 'content-type': 'application/json' },
  });
  forwardVarve.mockResolvedValue(upstream);
  const cookieJar = cookies();
  const request = new Request('https://explorer.example.test/api/session/connect', {
    method: 'POST',
    headers: { 'content-type': 'application/json; charset=utf-8' },
    body: JSON.stringify({ token: 'valid-token' }),
  });

  const response = await connect(event(request, cookieJar) as Parameters<typeof connect>[0]);

  expect(response).toBe(upstream);
  expect(forwardVarve).toHaveBeenCalledOnce();
  expect(forwardVarve).toHaveBeenCalledWith(
    expect.objectContaining({ path: '/v1/status', method: 'GET', token: 'valid-token' }),
  );
  expect(cookieJar.set).toHaveBeenCalledWith(
    SESSION_COOKIE_NAME,
    encodeSession('valid-token'),
    sessionCookieOptions(false),
  );
});

it('stores a session at the largest representable name-value boundary below 4096 bytes', async () => {
  const token = 'a'.repeat(3042);
  const encoded = encodeSession(token);
  expect(Buffer.byteLength(token, 'utf8')).toBeLessThanOrEqual(4096);
  expect(Buffer.byteLength(`${SESSION_COOKIE_NAME}=${encoded}`, 'utf8')).toBe(4095);
  const upstream = new Response(JSON.stringify({ roles: ['writer'] }), {
    status: 200,
    headers: { 'content-type': 'application/json' },
  });
  forwardVarve.mockResolvedValue(upstream);
  const cookieJar = cookies();
  const request = new Request('https://explorer.example.test/api/session/connect', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ token }),
  });

  const response = await connect(event(request, cookieJar) as Parameters<typeof connect>[0]);

  expect(response).toBe(upstream);
  expect(forwardVarve).toHaveBeenCalledWith(expect.objectContaining({ token }));
  expect(cookieJar.set).toHaveBeenCalledWith(
    SESSION_COOKIE_NAME,
    encoded,
    sessionCookieOptions(false),
  );
});

it('rejects the smallest session name-value pair above 4096 bytes before upstream', async () => {
  const token = 'a'.repeat(3043);
  const encoded = encodeSession(token);
  expect(Buffer.byteLength(token, 'utf8')).toBeLessThanOrEqual(4096);
  expect(Buffer.byteLength(`${SESSION_COOKIE_NAME}=${encoded}`, 'utf8')).toBe(4097);
  const cookieJar = cookies();
  const request = new Request('https://explorer.example.test/api/session/connect', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ token }),
  });

  const response = await connect(event(request, cookieJar) as Parameters<typeof connect>[0]);

  expect(response.status).toBe(400);
  await expect(response.json()).resolves.toMatchObject({
    code: 'invalid_request',
    message: 'Token cannot be stored in a session cookie',
  });
  expect(forwardVarve).not.toHaveBeenCalled();
  expect(cookieJar.set).not.toHaveBeenCalled();
});

it('connects with an empty token and stores its round-trippable session', async () => {
  const token = '';
  const encoded = encodeSession(token);
  const upstream = new Response(JSON.stringify({ roles: ['writer'] }), {
    status: 200,
    headers: { 'content-type': 'application/json' },
  });
  forwardVarve.mockResolvedValue(upstream);
  const cookieJar = cookies();
  const request = new Request('https://explorer.example.test/api/session/connect', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ token }),
  });

  const response = await connect(event(request, cookieJar) as Parameters<typeof connect>[0]);

  expect(response).toBe(upstream);
  expect(forwardVarve).toHaveBeenCalledWith(expect.objectContaining({ token: '' }));
  expect(cookieJar.set).toHaveBeenCalledWith(
    SESSION_COOKIE_NAME,
    encoded,
    sessionCookieOptions(false),
  );
});

it('does not store a token when the status check fails', async () => {
  forwardVarve.mockResolvedValue(
    new Response(JSON.stringify({ code: 'unauthorized', message: 'Authentication required' }), {
      status: 401,
      headers: { 'content-type': 'application/json' },
    }),
  );
  const cookieJar = cookies();
  const request = new Request('https://explorer.example.test/api/session/connect', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ token: 'wrong-token' }),
  });

  const response = await connect(event(request, cookieJar) as Parameters<typeof connect>[0]);

  expect(response.status).toBe(401);
  expect(cookieJar.set).not.toHaveBeenCalled();
});

it.each([
  ['text/plain', JSON.stringify({ token: 'valid-token' }), 415],
  ['application/problem+json', JSON.stringify({ token: 'valid-token' }), 415],
  ['application/json', JSON.stringify({ token: 'bad\nheader' }), 400],
  ['application/json', JSON.stringify({ token: 42 }), 400],
  ['application/json', JSON.stringify({ token: 'é'.repeat(2049) }), 413],
])('rejects an invalid connect request', async (contentType, body, expectedStatus) => {
  const cookieJar = cookies();
  const request = new Request('https://explorer.example.test/api/session/connect', {
    method: 'POST',
    headers: { 'content-type': contentType },
    body,
  });

  const response = await connect(event(request, cookieJar) as Parameters<typeof connect>[0]);

  expect(response.status).toBe(expectedStatus);
  expect(forwardVarve).not.toHaveBeenCalled();
  expect(cookieJar.set).not.toHaveBeenCalled();
});

it('forwards health without a session token', async () => {
  const upstream = new Response(JSON.stringify({ status: 'ok' }), {
    headers: { 'content-type': 'application/json' },
  });
  forwardVarve.mockResolvedValue(upstream);

  const response = await health({} as Parameters<typeof health>[0]);

  expect(response).toBe(upstream);
  expect(forwardVarve).toHaveBeenCalledWith(
    expect.objectContaining({ path: '/healthz', method: 'GET' }),
  );
  expect(forwardVarve.mock.calls[0]?.[0]).not.toHaveProperty('token');
});

it.each([
  ['status', status, 'GET'],
  ['query', query, 'POST'],
  ['tx', tx, 'POST'],
] as const)(
  'returns a stable 401 for %s without a valid session',
  async (_name, handler, method) => {
    const init: RequestInit = { method };
    if (method === 'POST') {
      init.headers = { 'content-type': 'application/json' };
      init.body = '{}';
    }
    const request = new Request(`https://explorer.example.test/api/varve/${_name}`, init);

    const response = await (handler as TestHandler)(event(request, cookies('not-canonical')));

    expect(response.status).toBe(401);
    await expect(response.json()).resolves.toMatchObject({
      code: 'unauthorized',
      message: 'Authentication required',
    });
    expect(forwardVarve).not.toHaveBeenCalled();
  },
);

it('rejects a session token that cannot be represented in a bearer header', async () => {
  const request = new Request('https://explorer.example.test/api/varve/status');

  const response = await status(
    event(request, cookies(encodeSession('bad\nheader'))) as Parameters<typeof status>[0],
  );

  expect(response.status).toBe(401);
  expect(forwardVarve).not.toHaveBeenCalled();
});

it.each([
  ['status', status, 'GET', '/v1/status'],
  ['query', query, 'POST', '/v1/query'],
  ['tx', tx, 'POST', '/v1/tx'],
] as const)(
  'forwards authenticated %s requests through the bounded primitive',
  async (name, handler, method, path) => {
    const upstream = new Response(JSON.stringify({ ok: true }), {
      headers: { 'content-type': 'application/json' },
    });
    forwardVarve.mockResolvedValue(upstream);
    const init: RequestInit = { method };
    if (method === 'POST') {
      init.headers = { 'content-type': 'application/json' };
      init.body = '{}';
    }
    const request = new Request(`https://explorer.example.test/api/varve/${name}`, init);

    const response = await (handler as TestHandler)(
      event(request, cookies(encodeSession('session-token'))),
    );

    expect(response).toBe(upstream);
    expect(forwardVarve).toHaveBeenCalledWith(
      expect.objectContaining({ path, method, token: 'session-token' }),
    );
    if (method === 'POST') {
      expect(forwardVarve.mock.calls[0]?.[0].request).toBe(request);
    }
  },
);
