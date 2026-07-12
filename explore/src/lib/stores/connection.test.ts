import { expect, it } from 'vitest';
import { createConnectionStore, type FetchLike } from './connection.svelte';

it('keeps designated-writer targets healthy when conditional writes are unsupported', async () => {
  const responses: Record<string, unknown> = {
    '/api/config': {
      displayName: 'Local Varve',
      target: '127.0.0.1:8080',
      authenticated: true,
    },
    '/api/varve/health': { status: 'ok' },
    '/api/varve/status': {
      roles: ['writer', 'query', 'compactor'],
      follower_error: null,
      probe: { verdict: 'unsupported', reason: 'conditional writes unavailable' },
    },
  };
  const fetcher: FetchLike = async (input) =>
    new Response(JSON.stringify(responses[input]), {
      status: input in responses ? 200 : 404,
      headers: { 'content-type': 'application/json' },
    });
  const connection = createConnectionStore(fetcher);

  await connection.refresh();

  expect(connection.session).toBe('authenticated');
  expect(connection.degraded).toBe(false);
});
