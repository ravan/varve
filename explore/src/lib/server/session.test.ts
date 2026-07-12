import { expect, it } from 'vitest';
import {
  SESSION_COOKIE_NAME,
  decodeSession,
  encodeSession,
  sessionCookieOptions,
} from './session';

it('round-trips empty and punctuation-bearing bearer tokens', () => {
  for (const token of ['', 'abc.def/+_=-']) {
    expect(decodeSession(encodeSession(token))).toBe(token);
  }
});

it('returns null for malformed or incorrectly shaped values', () => {
  for (const value of ['', 'not-base64', Buffer.from('{}').toString('base64url')]) {
    expect(() => decodeSession(value)).not.toThrow();
    expect(decodeSession(value)).toBeNull();
  }
});

it('rejects non-canonical base64url before decoding JSON', () => {
  const valid = encodeSession('secret');

  for (const value of [
    `${valid}=`,
    `${valid}===`,
    ` ${valid}`,
    `${valid}\n`,
    `${valid}!`,
    `${valid}!junk`,
  ]) {
    expect(decodeSession(value)).toBeNull();
  }
});

it('uses a session-only strict HttpOnly cookie in production', () => {
  const options = sessionCookieOptions(false);

  expect(SESSION_COOKIE_NAME).toBe('varve_explorer_session');
  expect(options).toMatchObject({
    httpOnly: true,
    sameSite: 'strict',
    secure: true,
    path: '/',
  });
  expect(options).not.toHaveProperty('maxAge');
  expect(options).not.toHaveProperty('expires');
  expect(options).not.toHaveProperty('domain');
});

it('allows an insecure cookie only in development', () => {
  expect(sessionCookieOptions(true).secure).toBe(false);
});
