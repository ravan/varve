import { describe, expect, it } from 'vitest';

import { degradedExplanation } from './connection-status';

const HEALTHY_HEALTH = { status: 'ok' };

function statusWith(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    roles: ['query'],
    applied_tx_id: 133,
    log_head_position: 133,
    follower_error: null,
    probe: { verdict: 'supported', reason: null },
    ...overrides,
  };
}

describe('degradedExplanation', () => {
  it('stays silent on a healthy node', () => {
    expect(degradedExplanation(HEALTHY_HEALTH, statusWith())).toBeNull();
  });

  it('stays silent when the backend honestly has no conditional-write API', () => {
    const status = statusWith({
      probe: { verdict: 'unsupported', reason: 'backend exposes no conditional-write API' },
    });
    expect(degradedExplanation(HEALTHY_HEALTH, status)).toBeNull();
  });

  // The blast-radius gallery on Garage v1.0.1, verbatim.
  it('explains an inconsistent probe with the node reason, and scopes the blast', () => {
    const status = statusWith({
      probe: {
        verdict: 'inconsistent',
        reason: 'create-if-absent over an existing object succeeded (precondition ignored)',
      },
    });

    const explanation = degradedExplanation(HEALTHY_HEALTH, status);

    expect(explanation?.summary).toBe(
      "The object store failed varve's conditional-write probe (verdict: inconsistent).",
    );
    expect(explanation?.detail).toBe(
      'create-if-absent over an existing object succeeded (precondition ignored). ' +
        'Conditional writes guard writer fencing, so reads and time travel are unaffected.',
    );
  });

  it('does not promise unaffected reads when the follower has stopped', () => {
    const status = statusWith({ follower_error: 'log truncated beneath cursor' });

    const explanation = degradedExplanation(HEALTHY_HEALTH, status);

    expect(explanation?.summary).toContain('stopped applying the log');
    expect(explanation?.detail).toBe('log truncated beneath cursor');
  });

  it('prefers the stalled follower over a probe verdict', () => {
    const status = statusWith({
      follower_error: 'log truncated beneath cursor',
      probe: { verdict: 'inconsistent', reason: 'precondition ignored' },
    });
    expect(degradedExplanation(HEALTHY_HEALTH, status)?.detail).toBe(
      'log truncated beneath cursor',
    );
  });

  it('falls back to health when the status document is clean', () => {
    const health = { status: 'degraded', error: 'follower stopped' };
    expect(degradedExplanation(health, statusWith())).toEqual({
      summary: 'The node reported degraded health.',
      detail: 'follower stopped',
    });
  });

  it.each([null, 'nonsense', 42, []])('reports an unreadable status document: %j', (status) => {
    expect(degradedExplanation(HEALTHY_HEALTH, status)?.summary).toBe(
      'The node did not return a readable status document.',
    );
  });

  it('reports a missing probe rather than inventing a verdict', () => {
    const explanation = degradedExplanation(HEALTHY_HEALTH, statusWith({ probe: null }));
    expect(explanation?.summary).toBe('The node did not report an object-store probe result.');
    expect(explanation?.detail).toBeNull();
  });

  it('handles an absent follower_error field, which the store already treats as degraded', () => {
    const status = statusWith();
    delete status.follower_error;
    expect(degradedExplanation(HEALTHY_HEALTH, status)?.summary).toBe(
      'The node did not report whether its follower is applying the log.',
    );
  });

  it('omits a blank probe reason instead of trailing a stray period', () => {
    const status = statusWith({ probe: { verdict: 'inconsistent', reason: '' } });
    expect(degradedExplanation(HEALTHY_HEALTH, status)?.detail).toBe(
      'Conditional writes guard writer fencing, so reads and time travel are unaffected.',
    );
  });
});
