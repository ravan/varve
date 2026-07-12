import type { Handle } from '@sveltejs/kit';

export const handle: Handle = async ({ event, resolve }) => {
  const startedAt = performance.now();
  const requestId = crypto.randomUUID();
  event.locals.requestId = requestId;
  let status = 500;

  try {
    const response = await resolve(event);
    status = response.status;
    response.headers.set('x-request-id', requestId);
    return response;
  } finally {
    console.info('request', {
      requestId,
      method: event.request.method,
      route: event.url.pathname,
      status,
      durationMs: Math.round(performance.now() - startedAt),
    });
  }
};
