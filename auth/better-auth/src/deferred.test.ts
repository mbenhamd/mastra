import { betterAuth } from 'better-auth';
import { memoryAdapter } from 'better-auth/adapters/memory';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { MastraAuthBetterAuth } from './index';

/**
 * Deferred instance mode: the provider builds its own `betterAuth()` in
 * `init()` on the host database and owns schema migrations (once-per-process
 * latch, retry on failure). Migrations are mocked here — real migration
 * behavior is exercised against a real database in the host's smoke tests.
 */

const { runMigrations } = vi.hoisted(() => ({ runMigrations: vi.fn(async () => {}) }));

vi.mock('better-auth/db/migration', () => ({
  getMigrations: vi.fn(async () => ({ runMigrations })),
}));

const SECRET = 'test-secret-that-is-at-least-32-chars';

/** Passthrough database handle: the provider forwards `database` to betterAuth as-is. */
const memoryDb = () => ({
  database: memoryAdapter({
    user: [],
    session: [],
    account: [],
    verification: [],
    organization: [],
    member: [],
    invitation: [],
  }),
});

type ProviderInternals = { auth: { options: { trustedOrigins?: string[]; basePath?: string } } };

beforeEach(() => {
  runMigrations.mockReset();
  runMigrations.mockResolvedValue(undefined);
});

describe('deferred instance mode', () => {
  it('requires auth or secret at construction', () => {
    expect(() => new MastraAuthBetterAuth({} as never)).toThrow(/Better Auth instance is required/);
  });

  it('accessing the instance before init() throws', () => {
    const provider = new MastraAuthBetterAuth({ secret: SECRET });
    expect(() => (provider as unknown as ProviderInternals).auth).toThrow(/not initialized/);
  });

  it('init() fails fast without a database', async () => {
    const provider = new MastraAuthBetterAuth({ secret: SECRET });
    await expect(provider.init({ publicUrl: 'https://factory.acme.com' })).rejects.toThrow(/database/);
  });

  it('init() builds a provider-owned instance under /auth/api and trusts host origins', async () => {
    // SameSite=None only lets the browser send the cookie — better-auth still
    // rejects requests from origins outside trustedOrigins, so cross-origin
    // SPA deploys must have their origins forwarded.
    const provider = new MastraAuthBetterAuth({ secret: SECRET });
    await provider.init({
      database: memoryDb(),
      publicUrl: 'https://api.acme.com',
      allowedOrigins: ['https://app.acme.com'],
    });

    const auth = (provider as unknown as ProviderInternals).auth;
    expect(auth).toBeDefined();
    expect(auth.options.basePath).toBe('/auth/api');
    expect(auth.options.trustedOrigins).toEqual(['https://app.acme.com']);
  });

  it('handleAuthRequest 503s while migrations fail, then retries and recovers', async () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
    runMigrations.mockRejectedValueOnce(new Error('database locked'));
    const provider = new MastraAuthBetterAuth({ secret: SECRET });
    await provider.init({ database: memoryDb(), publicUrl: 'http://localhost:3000' });

    const first = await provider.handleAuthRequest(new Request('http://localhost:3000/auth/api/get-session'));
    expect(first.status).toBe(503);
    expect(await first.json()).toEqual({ error: 'auth_unavailable' });

    const second = await provider.handleAuthRequest(new Request('http://localhost:3000/auth/api/get-session'));
    expect(second.status).not.toBe(503);
    expect(runMigrations).toHaveBeenCalledTimes(2);

    // The once-per-process latch holds after success: no more migration runs.
    await provider.handleAuthRequest(new Request('http://localhost:3000/auth/api/get-session'));
    expect(runMigrations).toHaveBeenCalledTimes(2);
    warn.mockRestore();
  });

  it('credential entry points await the deferred-migration latch idempotently', async () => {
    const provider = new MastraAuthBetterAuth({ secret: SECRET });
    await provider.init({ database: memoryDb(), publicUrl: 'http://localhost:3000' });

    // Sign-in is the FIRST operation on this provider — it must run the lazy
    // migration itself instead of relying on some earlier migrated call. (The
    // in-memory adapter accepts queries either way, so the latch call count is
    // the observable contract here; the real-database behavior is pinned in
    // deferred-credentials.test.ts.)
    await provider
      .signIn('nobody@acme.test', 'password-1234', new Request('http://localhost:3000/auth/credentials/sign-in'))
      .catch(() => {
        // invalid credentials — irrelevant here
      });
    expect(runMigrations).toHaveBeenCalledTimes(1);

    // The once-per-process latch holds across the other credential entry point.
    await provider.signUp(
      'new-user@acme.test',
      'password-1234',
      'New User',
      new Request('http://localhost:3000/auth/credentials/sign-up'),
    );
    expect(runMigrations).toHaveBeenCalledTimes(1);
  });

  it('sign-up fails closed while migrations fail, then retries and recovers', async () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
    runMigrations.mockRejectedValueOnce(new Error('database locked'));
    const provider = new MastraAuthBetterAuth({ secret: SECRET });
    await provider.init({ database: memoryDb(), publicUrl: 'http://localhost:3000' });

    // First sign-up: migration fails → the credential op must fail closed
    // instead of proceeding against an unmigrated database.
    await expect(
      provider.signUp(
        'blocked-user@acme.test',
        'password-1234',
        'Blocked User',
        new Request('http://localhost:3000/auth/credentials/sign-up'),
      ),
    ).rejects.toThrow(/database locked/);

    // Retry succeeds once migrations recover.
    const result = await provider.signUp(
      'recovered-user@acme.test',
      'password-1234',
      'Recovered User',
      new Request('http://localhost:3000/auth/credentials/sign-up'),
    );
    expect(result.user.email).toBe('recovered-user@acme.test');
    expect(runMigrations).toHaveBeenCalledTimes(2);
    warn.mockRestore();
  });

  it('bring-your-own instances never trigger provider-owned migrations', async () => {
    const auth = betterAuth({
      baseURL: 'http://localhost:3000',
      secret: SECRET,
      basePath: '/auth/api',
      database: memoryDb().database,
      emailAndPassword: { enabled: true },
    });
    const provider = new MastraAuthBetterAuth({ auth });
    await provider.init({});

    await provider.handleAuthRequest(new Request('http://localhost:3000/auth/api/get-session'));
    expect(runMigrations).not.toHaveBeenCalled();
  });
});
