import { expect, it } from 'vitest';
import { createSessionOperationCoordinator } from './session-operations';

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

it('runs cookie-changing session operations in invocation order', async () => {
  const coordinator = createSessionOperationCoordinator();
  const connectGate = deferred<void>();
  const order: string[] = [];

  const connect = coordinator.run(async () => {
    order.push('connect:start');
    await connectGate.promise;
    order.push('connect:end');
  });
  const disconnect = coordinator.run(async () => {
    order.push('disconnect:start');
    order.push('disconnect:end');
  });

  await Promise.resolve();
  expect(order).toEqual(['connect:start']);

  connectGate.resolve();
  await Promise.all([connect, disconnect]);
  expect(order).toEqual(['connect:start', 'connect:end', 'disconnect:start', 'disconnect:end']);
});

it('continues the session operation queue after a failure', async () => {
  const coordinator = createSessionOperationCoordinator();
  const order: string[] = [];

  const failed = coordinator.run(async () => {
    order.push('connect');
    throw new Error('rejected');
  });
  const disconnect = coordinator.run(async () => {
    order.push('disconnect');
  });

  await expect(failed).rejects.toThrow('rejected');
  await disconnect;
  expect(order).toEqual(['connect', 'disconnect']);
});
