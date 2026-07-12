import { expect, it } from 'vitest';
import { createConnectionStore, type FetchLike } from './connection.svelte';

async function refreshWithStatus(status: unknown) {
  const responses: Record<string, unknown> = {
    '/api/config': {
      displayName: 'Local Varve',
      target: '127.0.0.1:8080',
      authenticated: true,
    },
    '/api/varve/health': { status: 'ok' },
    '/api/varve/status': status,
  };
  const fetcher: FetchLike = async (input) =>
    new Response(JSON.stringify(responses[input]), {
      status: input in responses ? 200 : 404,
      headers: { 'content-type': 'application/json' },
    });
  const connection = createConnectionStore(fetcher);

  await connection.refresh();

  return connection;
}

it.each(['supported', 'unsupported'])(
  'keeps a status with the known %s capability verdict healthy',
  async (verdict) => {
    const connection = await refreshWithStatus({
      roles: ['writer', 'query', 'compactor'],
      follower_error: null,
      probe: { verdict, reason: 'capability report' },
    });

    expect(connection.session).toBe('authenticated');
    expect(connection.degraded).toBe(false);
  },
);

it('degrades an inconsistent capability verdict', async () => {
  const connection = await refreshWithStatus({
    follower_error: null,
    probe: { verdict: 'inconsistent' },
  });

  expect(connection.session).toBe('degraded');
  expect(connection.degraded).toBe(true);
});

it('degrades an unknown string capability verdict', async () => {
  const connection = await refreshWithStatus({
    follower_error: null,
    probe: { verdict: 'maybe' },
  });

  expect(connection.session).toBe('degraded');
  expect(connection.degraded).toBe(true);
});

it.each([null, 7, false])('degrades the non-string capability verdict %j', async (verdict) => {
  const connection = await refreshWithStatus({
    follower_error: null,
    probe: { verdict },
  });

  expect(connection.session).toBe('degraded');
  expect(connection.degraded).toBe(true);
});

it.each([null, [], 'unsupported', {}])('degrades the malformed probe %j', async (probe) => {
  const connection = await refreshWithStatus({ follower_error: null, probe });

  expect(connection.session).toBe('degraded');
  expect(connection.degraded).toBe(true);
});

it('degrades a follower error even when the capability verdict is supported', async () => {
  const connection = await refreshWithStatus({
    follower_error: 'replication failed',
    probe: { verdict: 'supported' },
  });

  expect(connection.session).toBe('degraded');
  expect(connection.degraded).toBe(true);
});

it.each([undefined, 7, {}, []])(
  'degrades the malformed follower error %j',
  async (followerError) => {
    const connection = await refreshWithStatus({
      follower_error: followerError,
      probe: { verdict: 'supported' },
    });

    expect(connection.session).toBe('degraded');
    expect(connection.degraded).toBe(true);
  },
);

it.each(['malformed', null, []])('degrades the malformed status %j', async (status) => {
  const connection = await refreshWithStatus(status);

  expect(connection.session).toBe('degraded');
  expect(connection.degraded).toBe(true);
});
