/**
 * Why the connection badge reads "Degraded".
 *
 * `createConnectionStore` decides *that* a session is degraded — see
 * `isDegradedHealth` / `isDegradedStatus` in `stores/connection.svelte.ts`.
 * This module answers *why*, from the same two payloads and in the same order,
 * so the badge and its tooltip can never disagree about the cause.
 *
 * The reason strings are the node's own: a backend that fails the probe for a
 * new reason explains itself here without a change to Explorer.
 */

export interface DegradedExplanation {
  /** One sentence naming the subsystem at fault. */
  readonly summary: string;
  /** The node's verbatim reason, and what it does and does not affect. */
  readonly detail: string | null;
}

/**
 * A conditional-write verdict is the object store's, not the data's. Varve
 * uses compare-and-swap only to fence writers and commit manifests, so a
 * failed probe never means the rows you are reading are wrong.
 */
const WRITE_ONLY =
  'Conditional writes guard writer fencing, so reads and time travel are unaffected.';

export function degradedExplanation(health: unknown, status: unknown): DegradedExplanation | null {
  return statusExplanation(status) ?? healthExplanation(health);
}

function statusExplanation(status: unknown): DegradedExplanation | null {
  if (!isRecord(status)) {
    return {
      summary: 'The node did not return a readable status document.',
      detail: null,
    };
  }

  // Mirrors `isDegradedStatus`, which treats any non-null, non-empty value —
  // including a missing field — as a stalled follower.
  const followerError = status.follower_error;
  if (followerError !== null && followerError !== '') {
    if (typeof followerError === 'string') {
      return {
        summary: 'This follower stopped applying the log, so reads can be stale.',
        detail: followerError,
      };
    }
    return {
      summary: 'The node did not report whether its follower is applying the log.',
      detail: null,
    };
  }

  const probe = status.probe;
  if (!isRecord(probe)) {
    return {
      summary: 'The node did not report an object-store probe result.',
      detail: null,
    };
  }

  const verdict = probe.verdict;
  if (verdict === 'supported' || verdict === 'unsupported') return null;

  const named =
    typeof verdict === 'string' && verdict !== '' ? `verdict: ${verdict}` : 'unrecognised verdict';
  const reason = typeof probe.reason === 'string' && probe.reason !== '' ? probe.reason : null;
  return {
    summary: `The object store failed varve's conditional-write probe (${named}).`,
    detail: reason === null ? WRITE_ONLY : `${reason}. ${WRITE_ONLY}`,
  };
}

function healthExplanation(health: unknown): DegradedExplanation | null {
  if (!isRecord(health) || health.status !== 'degraded') return null;
  const error = typeof health.error === 'string' && health.error !== '' ? health.error : null;
  return { summary: 'The node reported degraded health.', detail: error };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}
