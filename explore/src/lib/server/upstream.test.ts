import { describe, expect, it, vi } from 'vitest';
import type { ServerConfig } from './config';
import { forwardVarve, isSafeBearerToken, normalizeUpstreamError } from './upstream';

const TOKEN = 'super-secret-token';

function makeConfig(overrides: Partial<ServerConfig> = {}): ServerConfig {
  return {
    target: new URL('https://query.example.test/internal?tenant=secret'),
    displayName: 'Test Varve',
    allowedWriterOrigins: new Set(['https://query.example.test', 'https://writer.example.test']),
    timeoutMs: 100,
    maxRequestBytes: 1024,
    production: true,
    ...overrides,
  };
}

function jsonRequest(body: unknown, contentType = 'application/json'): Request {
  return new Request('https://explorer.example.test/api', {
    method: 'POST',
    headers: {
      'content-type': contentType,
      cookie: 'session=browser-secret',
      connection: 'keep-alive',
      'x-forwarded-for': '192.0.2.1',
    },
    body: JSON.stringify(body),
  });
}

function makeInput(overrides: Partial<Parameters<typeof forwardVarve>[0]> = {}) {
  return {
    config: makeConfig(),
    path: '/v1/status',
    method: 'GET' as const,
    token: TOKEN,
    fetch: vi.fn().mockResolvedValue(
      new Response(JSON.stringify({ roles: ['query'] }), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      }),
    ),
    ...overrides,
  };
}

async function expectError(response: Response, expected: object) {
  expect(response.headers.get('content-type')).toContain('application/json');
  await expect(response.json()).resolves.toMatchObject(expected);
}

describe('forwardVarve', () => {
  it.each(['toString', 'constructor'])(
    'rejects inherited stable-error table key %s',
    async (code) => {
      const response = normalizeUpstreamError(500, { code, message: 'untrusted detail' });

      expect(response.status).toBe(502);
      await expectError(response, {
        code: 'malformed_response',
        message: 'Varve returned an invalid response',
      });
    },
  );

  it('accepts only empty or visible ASCII bearer token values', () => {
    expect(isSafeBearerToken('')).toBe(true);
    expect(isSafeBearerToken('AZaz09!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~')).toBe(true);
    expect(isSafeBearerToken('contains space')).toBe(false);
    expect(isSafeBearerToken('emoji-😀')).toBe(false);
    expect(isSafeBearerToken('control\ncharacter')).toBe(false);
  });

  it.each(['emoji-😀', 'control\ncharacter'])(
    'normalizes an unsafe bearer token without calling fetch',
    async (token) => {
      const fetch = vi.fn();

      const response = await forwardVarve(makeInput({ fetch, token }));

      expect(response.status).toBe(401);
      expect(fetch).not.toHaveBeenCalled();
      await expectError(response, {
        code: 'unauthorized',
        message: 'Authentication required',
      });
    },
  );

  it('normalizes authorization header construction failures', async () => {
    const originalSet = Headers.prototype.set;
    const set = vi.spyOn(Headers.prototype, 'set').mockImplementation(function (
      this: Headers,
      name: string,
      value: string,
    ) {
      if (name.toLowerCase() === 'authorization') throw new TypeError('header rejected');
      return originalSet.call(this, name, value);
    });

    try {
      const fetch = vi.fn();
      const response = await forwardVarve(makeInput({ fetch }));

      expect(response.status).toBe(401);
      expect(fetch).not.toHaveBeenCalled();
      await expectError(response, { code: 'unauthorized' });
    } finally {
      set.mockRestore();
    }
  });

  it('joins only the configured origin and attaches the bearer token', async () => {
    const fetch = vi.fn().mockResolvedValue(
      new Response(JSON.stringify({ roles: ['query'] }), {
        headers: { 'content-type': 'application/json; charset=utf-8' },
      }),
    );

    const response = await forwardVarve(makeInput({ fetch }));

    expect(response.status).toBe(200);
    expect(fetch).toHaveBeenCalledOnce();
    const [url, init] = fetch.mock.calls[0] as unknown as [URL, RequestInit];
    expect(url.href).toBe('https://query.example.test/v1/status');
    expect(new Headers(init.headers).get('authorization')).toBe(`Bearer ${TOKEN}`);
    expect(new Headers(init.headers).get('accept')).toBe('application/json');
  });

  it('accepts parameterized application/json requests and forwards no browser headers', async () => {
    const fetch = vi.fn().mockResolvedValue(
      new Response(JSON.stringify({ rows: [] }), {
        headers: { 'content-type': 'application/json' },
      }),
    );

    const response = await forwardVarve(
      makeInput({
        fetch,
        path: '/v1/query',
        method: 'POST',
        request: jsonRequest({ gql: 'MATCH (n) RETURN n' }, 'application/json; charset=utf-8'),
      }),
    );

    expect(response.status).toBe(200);
    const [, init] = fetch.mock.calls[0] as unknown as [URL, RequestInit];
    const headers = new Headers(init.headers);
    expect(headers.get('content-type')).toBe('application/json');
    expect(headers.get('cookie')).toBeNull();
    expect(headers.get('connection')).toBeNull();
    expect(headers.get('x-forwarded-for')).toBeNull();
    expect(new TextDecoder().decode(new Uint8Array(init.body as ArrayBuffer))).toBe(
      JSON.stringify({ gql: 'MATCH (n) RETURN n' }),
    );
  });

  it.each(['text/plain', 'application/problem+json'])(
    'rejects non-JSON request media type %s without calling upstream',
    async (contentType) => {
      const fetch = vi.fn();

      const response = await forwardVarve(
        makeInput({
          fetch,
          path: '/v1/query',
          method: 'POST',
          request: jsonRequest({ gql: 'RETURN 1' }, contentType),
        }),
      );

      expect(response.status).toBe(415);
      expect(fetch).not.toHaveBeenCalled();
      await expectError(response, { code: 'invalid_request', message: 'JSON request required' });
    },
  );

  it.each(['text/plain', 'application/problem+json'])(
    'rejects non-JSON upstream media type %s without exposing its body',
    async (contentType) => {
      const fetch = vi.fn().mockResolvedValue(
        new Response(`upstream leaked ${TOKEN}`, {
          status: 500,
          headers: { 'content-type': contentType },
        }),
      );

      const response = await forwardVarve(makeInput({ fetch }));

      expect(response.status).toBe(502);
      const body = await response.text();
      expect(body).not.toContain(TOKEN);
      expect(JSON.parse(body)).toMatchObject({ code: 'malformed_response' });
    },
  );

  it('maps a timed-out request to a stable gateway timeout', async () => {
    const fetch = vi.fn((_url: string | URL | Request, init?: RequestInit) => {
      return new Promise<Response>((_resolve, reject) => {
        init?.signal?.addEventListener('abort', () => reject(init.signal?.reason), { once: true });
      });
    });

    const response = await forwardVarve(makeInput({ fetch, config: makeConfig({ timeoutMs: 1 }) }));

    expect(response.status).toBe(504);
    await expectError(response, { code: 'timeout', message: 'Varve request timed out' });
  });

  it('rejects a response whose declared content length exceeds the configured limit', async () => {
    const fetch = vi.fn().mockResolvedValue(
      new Response('{}', {
        headers: { 'content-type': 'application/json', 'content-length': '1025' },
      }),
    );

    const response = await forwardVarve(makeInput({ fetch }));

    expect(response.status).toBe(502);
    await expectError(response, { code: 'malformed_response' });
  });

  it('counts streamed response bytes when content length is absent', async () => {
    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(new TextEncoder().encode('{"value":"'));
        controller.enqueue(new Uint8Array(2048));
        controller.close();
      },
    });
    const fetch = vi
      .fn()
      .mockResolvedValue(new Response(stream, { headers: { 'content-type': 'application/json' } }));

    const response = await forwardVarve(makeInput({ fetch }));

    expect(response.status).toBe(502);
    await expectError(response, { code: 'malformed_response' });
  });

  it('bounds request bodies before contacting upstream', async () => {
    const request = new Request('https://explorer.example.test/api', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(new TextEncoder().encode('{"gql":"'));
          controller.enqueue(new Uint8Array(2048));
          controller.close();
        },
      }),
      duplex: 'half',
    } as RequestInit);
    const fetch = vi.fn();

    const response = await forwardVarve(
      makeInput({ fetch, path: '/v1/query', method: 'POST', request }),
    );

    expect(response.status).toBe(413);
    expect(fetch).not.toHaveBeenCalled();
    await expectError(response, { code: 'invalid_request', message: 'Request body too large' });
  });

  it('maps upstream 401 without forwarding authentication headers or details', async () => {
    const fetch = vi.fn().mockResolvedValue(
      new Response(`authentication backend leaked ${TOKEN}`, {
        status: 401,
        headers: {
          'content-type': 'text/plain',
          'www-authenticate': 'Bearer realm="internal-secret"',
          server: 'secret-server/1.0',
          'set-cookie': 'upstream-secret=1',
        },
      }),
    );

    const response = await forwardVarve(makeInput({ fetch }));

    expect(response.status).toBe(401);
    expect(response.headers.get('www-authenticate')).toBeNull();
    expect(response.headers.get('server')).toBeNull();
    expect(response.headers.get('set-cookie')).toBeNull();
    const body = await response.text();
    expect(body).not.toContain(TOKEN);
    expect(JSON.parse(body)).toMatchObject({
      code: 'unauthorized',
      message: 'Authentication required',
    });
  });

  it('normalizes Retry-After into milliseconds and forwards only that safe response header', async () => {
    const fetch = vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify({ code: 'backpressure', message: 'queue details', writer: null }),
        {
          status: 429,
          headers: {
            'content-type': 'application/json',
            'retry-after': '2',
            connection: 'close',
            server: 'hidden',
          },
        },
      ),
    );

    const response = await forwardVarve(makeInput({ fetch }));

    expect(response.status).toBe(429);
    expect(response.headers.get('retry-after')).toBe('2');
    expect(response.headers.get('connection')).toBeNull();
    expect(response.headers.get('server')).toBeNull();
    await expectError(response, { code: 'backpressure', retryAfterMs: 2000 });
  });

  it('preserves the public degraded health envelope and status', async () => {
    const fetch = vi.fn().mockResolvedValue(
      new Response(JSON.stringify({ status: 'degraded', error: 'follower stopped' }), {
        status: 503,
        headers: { 'content-type': 'application/json' },
      }),
    );

    const response = await forwardVarve(
      makeInput({ fetch, path: '/healthz', method: 'GET', token: undefined }),
    );

    expect(response.status).toBe(503);
    await expect(response.json()).resolves.toEqual({
      status: 'degraded',
      error: 'follower stopped',
    });
  });

  it('returns successful JSON bytes while stripping unsafe upstream headers', async () => {
    const fetch = vi.fn().mockResolvedValue(
      new Response(JSON.stringify({ rows: [{ value: 1 }] }), {
        headers: {
          'content-type': 'application/json; charset=utf-8',
          'set-cookie': 'upstream-secret=1',
          server: 'secret-server/1.0',
          connection: 'keep-alive',
        },
      }),
    );

    const response = await forwardVarve(makeInput({ fetch }));

    expect(response.status).toBe(200);
    expect(response.headers.get('content-type')).toBe('application/json; charset=utf-8');
    expect(response.headers.get('set-cookie')).toBeNull();
    expect(response.headers.get('server')).toBeNull();
    expect(response.headers.get('connection')).toBeNull();
    await expect(response.json()).resolves.toEqual({ rows: [{ value: 1 }] });
  });

  it('retries a write once against an allowed advertised writer', async () => {
    const fetch = vi
      .fn()
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            code: 'misdirected_request',
            message: 'request must be sent to writer',
            writer: 'https://writer.example.test/evil/path?token=secret#fragment',
          }),
          { status: 421, headers: { 'content-type': 'application/json' } },
        ),
      )
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            tx_id: 1,
            basis: 1,
            system_time: '2026-07-12T00:00:00Z',
            system_time_us: 1,
            side_effects: {},
          }),
          { status: 200, headers: { 'content-type': 'application/json' } },
        ),
      );

    const response = await forwardVarve(
      makeInput({
        fetch,
        path: '/v1/tx',
        method: 'POST',
        request: jsonRequest({ gql: 'INSERT (:N)' }),
      }),
    );

    expect(response.status).toBe(200);
    expect(fetch).toHaveBeenCalledTimes(2);
    expect((fetch.mock.calls[1]![0] as URL).href).toBe('https://writer.example.test/v1/tx');
  });

  it.each([
    ['https://attacker.example.test', 'unlisted origin'],
    ['https://user:password@writer.example.test', 'credentials'],
  ])('does not retry an advertised writer with %s (%s)', async (writer) => {
    const fetch = vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify({
          code: 'misdirected_request',
          message: 'request must be sent to writer',
          writer,
        }),
        { status: 421, headers: { 'content-type': 'application/json' } },
      ),
    );

    const response = await forwardVarve(
      makeInput({
        fetch,
        path: '/v1/tx',
        method: 'POST',
        request: jsonRequest({ gql: 'INSERT (:N)' }),
      }),
    );

    expect(response.status).toBe(421);
    expect(fetch).toHaveBeenCalledOnce();
    expect(await response.text()).not.toContain(writer);
  });

  it('never retries reads or a second 421 response', async () => {
    const misdirected = () =>
      new Response(
        JSON.stringify({
          code: 'misdirected_request',
          message: 'request must be sent to writer',
          writer: 'https://writer.example.test',
        }),
        { status: 421, headers: { 'content-type': 'application/json' } },
      );
    const readFetch = vi.fn().mockResolvedValue(misdirected());
    const writeFetch = vi
      .fn()
      .mockResolvedValueOnce(misdirected())
      .mockResolvedValueOnce(misdirected());

    const readResponse = await forwardVarve(makeInput({ fetch: readFetch }));
    const writeResponse = await forwardVarve(
      makeInput({
        fetch: writeFetch,
        path: '/v1/tx',
        method: 'POST',
        request: jsonRequest({ gql: 'INSERT (:N)' }),
      }),
    );

    expect(readResponse.status).toBe(421);
    expect(readFetch).toHaveBeenCalledOnce();
    expect(writeResponse.status).toBe(421);
    expect(writeFetch).toHaveBeenCalledTimes(2);
  });
});
