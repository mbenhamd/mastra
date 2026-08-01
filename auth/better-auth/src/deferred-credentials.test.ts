import { createRequire } from 'node:module';

import { describe, expect, it } from 'vitest';

import { MastraAuthBetterAuth } from './index';

/**
 * Deferred instance mode, credential entry points, REAL migrations.
 *
 * Unlike `deferred.test.ts` (which mocks `better-auth/db/migration`), this
 * file runs better-auth's actual schema migrations against a fresh in-memory
 * libsql database — the same `dialect: 'libsql'` handle shape hosts pass in.
 * That makes the deferred-migration gate observable end-to-end: without it,
 * the first public `/auth/credentials/sign-up` call hits a database with no
 * tables at all and fails.
 */

// `@libsql/client` is a transitive dependency of `@libsql/kysely-libsql`
// (a direct dependency of this package); resolve it through that package so
// the strict pnpm layout doesn't require declaring a test-only dependency.
const requireFromLibsqlDialect = createRequire(import.meta.resolve('@libsql/kysely-libsql'));
const { createClient } = requireFromLibsqlDialect('@libsql/client') as typeof import('@libsql/client');

const SECRET = 'test-secret-that-is-at-least-32-chars';

describe('deferred instance mode credential entry points (real migrations)', () => {
  it('first sign-up on a fresh database succeeds without any prior migrated call', async () => {
    const provider = new MastraAuthBetterAuth({ secret: SECRET });
    await provider.init({
      database: { dialect: 'libsql', client: createClient({ url: ':memory:' }) },
      publicUrl: 'http://localhost:3000',
    });

    // No handleAuthRequest / getCurrentUser / any other call has run: signUp
    // itself must await the lazy migration before touching the database.
    const result = await provider.signUp(
      'first-user@acme.test',
      'password-1234',
      'First User',
      new Request('http://localhost:3000/auth/credentials/sign-up'),
    );

    expect(result.user.email).toBe('first-user@acme.test');
    expect(result.user.id).toBeTruthy();
    expect(result.token).toBeTruthy();
  });

  it('first sign-in on a fresh database reaches credential validation instead of a missing-table failure', async () => {
    const client = createClient({ url: ':memory:' });
    const provider = new MastraAuthBetterAuth({ secret: SECRET });
    await provider.init({
      database: { dialect: 'libsql', client },
      publicUrl: 'http://localhost:3000',
    });

    // Fresh DB, sign-in as the very first credential op: the migration gate
    // must run so better-auth evaluates the credentials against real (empty)
    // tables and reports invalid credentials — then a sign-up + sign-in pair
    // on the same database round-trips.
    await expect(
      provider.signIn(
        'nobody@acme.test',
        'password-1234',
        new Request('http://localhost:3000/auth/credentials/sign-in'),
      ),
    ).rejects.toThrow(/Invalid email or password/);

    await provider.signUp(
      'second-user@acme.test',
      'password-1234',
      undefined,
      new Request('http://localhost:3000/auth/credentials/sign-up'),
    );
    const signedIn = await provider.signIn(
      'second-user@acme.test',
      'password-1234',
      new Request('http://localhost:3000/auth/credentials/sign-in'),
    );
    expect(signedIn.user.email).toBe('second-user@acme.test');
    expect(signedIn.token).toBeTruthy();
  });
});
