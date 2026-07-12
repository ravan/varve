import { describe, expect, it } from 'vitest';
import { loadServerConfig } from './config';

describe('loadServerConfig', () => {
  it('requires an absolute http(s) target in production', () => {
    expect(() => loadServerConfig({ NODE_ENV: 'production' })).toThrow('VARVE_URL');
    expect(() =>
      loadServerConfig({ NODE_ENV: 'production', VARVE_URL: 'file:///tmp/db' }),
    ).toThrow('VARVE_URL');
    expect(() =>
      loadServerConfig({ NODE_ENV: 'production', VARVE_URL: '/relative' }),
    ).toThrow('VARVE_URL');
  });

  it('normalizes writer origins and supplies bounded defaults', () => {
    const config = loadServerConfig({
      NODE_ENV: 'production',
      VARVE_URL: 'https://query.example.test/',
      VARVE_ALLOWED_WRITER_ORIGINS: 'https://writer.example.test',
    });

    expect(config.allowedWriterOrigins).toEqual(
      new Set(['https://query.example.test', 'https://writer.example.test']),
    );
    expect(config.timeoutMs).toBeGreaterThan(0);
    expect(config.maxRequestBytes).toBeGreaterThan(0);
    expect(config.production).toBe(true);
  });

  it('rejects unsafe target URL components', () => {
    for (const target of [
      'https://user:secret@query.example.test',
      'https://query.example.test/#internal',
    ]) {
      expect(() =>
        loadServerConfig({ NODE_ENV: 'production', VARVE_URL: target }),
      ).toThrow('VARVE_URL');
    }
  });

  it('rejects malformed writer origins', () => {
    for (const origin of [
      'file:///tmp/writer',
      'https://user:secret@writer.example.test',
      'https://writer.example.test/path',
      'not a URL',
    ]) {
      expect(() =>
        loadServerConfig({
          NODE_ENV: 'production',
          VARVE_URL: 'https://query.example.test',
          VARVE_ALLOWED_WRITER_ORIGINS: origin,
        }),
      ).toThrow('VARVE_ALLOWED_WRITER_ORIGINS');
    }
  });

  it('rejects malformed or out-of-range numeric settings', () => {
    for (const [name, value] of [
      ['VARVE_TIMEOUT_MS', '0'],
      ['VARVE_TIMEOUT_MS', '120001'],
      ['VARVE_TIMEOUT_MS', '1.5'],
      ['VARVE_MAX_REQUEST_BYTES', '0'],
      ['VARVE_MAX_REQUEST_BYTES', '16777217'],
      ['VARVE_MAX_REQUEST_BYTES', 'many'],
    ] as const) {
      expect(() =>
        loadServerConfig({
          NODE_ENV: 'development',
          [name]: value,
        }),
      ).toThrow(name);
    }
  });

  it('uses development-safe defaults and accepts bounded numeric settings', () => {
    const config = loadServerConfig({
      NODE_ENV: 'development',
      VARVE_DISPLAY_NAME: 'Development Varve',
      VARVE_TIMEOUT_MS: '2500',
      VARVE_MAX_REQUEST_BYTES: '2048',
    });

    expect(config.target.href).toBe('http://127.0.0.1:8080/');
    expect(config.displayName).toBe('Development Varve');
    expect(config.timeoutMs).toBe(2500);
    expect(config.maxRequestBytes).toBe(2048);
    expect(config.production).toBe(false);
  });
});
