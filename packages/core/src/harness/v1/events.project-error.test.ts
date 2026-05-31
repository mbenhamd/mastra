/**
 * Harness v1 — `projectHarnessPublicError` redaction (§13.3 / §13.3f.1).
 *
 * The projector is the single boundary that turns an arbitrary thrown value
 * into the public `{ code, message }` carried by `channel_ingress_failed` /
 * `signal_failed` / `queue_failed` / durable receipts and `error` turn events.
 *
 * §13.3f.1 requires that:
 *  - every public code is fully `harness.*` namespaced;
 *  - `harness.internal` is reserved for true unhandled failures;
 *  - the raw driver/SQL/path `err.message` never crosses the wire.
 */

import { describe, expect, it } from 'vitest';

import {
  HarnessBusyError,
  HarnessStorageError,
  HarnessValidationError,
} from './errors';
import { projectHarnessPublicError } from './events';

describe('projectHarnessPublicError', () => {
  it('maps a raw TypeError to harness.internal with a redacted message (no raw text leak)', () => {
    const raw = new TypeError('connect ECONNREFUSED 10.0.0.5:5432 — password authentication failed for user "postgres"');
    const projected = projectHarnessPublicError(raw);
    expect(projected.code).toBe('harness.internal');
    expect(projected.message).toBe('An internal harness error occurred');
    // The raw driver text must not leak onto the public surface.
    expect(projected.message).not.toContain('ECONNREFUSED');
    expect(projected.message).not.toContain('postgres');
    expect(projected.message).not.toBe(raw.message);
  });

  it('maps a bare Error (non-namespaced name) to harness.internal, not err.name', () => {
    // Previously the projector leaked `err.name` ("Error") as the code and the
    // raw message as the public text.
    const raw = new Error('SELECT * FROM secret_table WHERE token = $1 failed: relation does not exist');
    const projected = projectHarnessPublicError(raw);
    expect(projected.code).toBe('harness.internal');
    expect(projected.message).toBe('An internal harness error occurred');
    expect(projected.message).not.toContain('secret_table');
  });

  it('does NOT treat a non-namespaced `.code` as a passthrough code', () => {
    // A driver error may carry e.g. `code: 'ECONNREFUSED'` or `code: '28P01'`.
    // That is not a harness.* code and must be redacted.
    const raw = Object.assign(new Error('auth failed'), { code: '28P01' });
    const projected = projectHarnessPublicError(raw);
    expect(projected.code).toBe('harness.internal');
    expect(projected.message).toBe('An internal harness error occurred');
  });

  it('maps a HarnessStorageError to harness.storage with its safe summary message', () => {
    const raw = new HarnessStorageError({
      operation: 'session_save',
      sessionId: 'sess-1',
      // The driver cause carries sensitive text but stays local-only.
      cause: new Error('duplicate key value violates unique constraint "sessions_pkey"'),
    });
    const projected = projectHarnessPublicError(raw);
    expect(projected.code).toBe('harness.storage');
    expect(projected.message).toBe(raw.message);
    // The raw driver cause text must not leak through the public projection.
    expect(projected.message).not.toContain('sessions_pkey');
    expect(projected.message).not.toContain('duplicate key');
  });

  it('carries a workspace subject for a workspace_cleanup storage failure (§4.5d)', () => {
    // §4.5d: HarnessStorageOperation includes `workspace_cleanup`, so the
    // matching HarnessStorageSubject must include `{ kind: 'workspace' }` to
    // name the failing per-resource workspace row.
    const err = new HarnessStorageError({
      operation: 'workspace_cleanup',
      resourceId: 'res-9',
      subject: { kind: 'workspace', id: 'ws-res-9' },
      cause: new Error('cleanup failed'),
    });
    expect(err.operation).toBe('workspace_cleanup');
    expect(err.subject).toEqual({ kind: 'workspace', id: 'ws-res-9' });
    const projected = projectHarnessPublicError(err);
    expect(projected.code).toBe('harness.storage');
  });

  it('passes through a namespaced HarnessError code with its constructed (safe) message', () => {
    const busy = new HarnessBusyError('sess-2', 'in_flight');
    expect(busy.code).toBe('harness.busy');
    const projected = projectHarnessPublicError(busy);
    expect(projected.code).toBe('harness.busy');
    expect(projected.message).toBe(busy.message);
  });

  it('redacts a namespaced HarnessValidationError that has no `.code` field down to harness.internal', () => {
    // HarnessValidationError is a Harness error but carries no `.code` field
    // (its public wire code is assigned at the route boundary, not on the
    // class). Without a `harness.*` `.code` it is treated as unhandled here.
    const err = new HarnessValidationError('field', 'reason');
    expect((err as { code?: unknown }).code).toBeUndefined();
    const projected = projectHarnessPublicError(err);
    expect(projected.code).toBe('harness.internal');
    expect(projected.message).toBe('An internal harness error occurred');
  });

  it('maps a non-Error throw (string) to harness.internal without echoing the value', () => {
    const projected = projectHarnessPublicError('DROP TABLE users; -- secret');
    expect(projected.code).toBe('harness.internal');
    expect(projected.message).toBe('An internal harness error occurred');
    expect(projected.message).not.toContain('DROP TABLE');
  });

  it('maps a non-Error throw (object) to harness.internal without echoing the value', () => {
    const projected = projectHarnessPublicError({ sql: 'SELECT secret', host: 'db.internal' });
    expect(projected.code).toBe('harness.internal');
    expect(projected.message).toBe('An internal harness error occurred');
    expect(projected.message).not.toContain('secret');
    expect(projected.message).not.toContain('db.internal');
  });

  it('maps null/undefined throws to harness.internal', () => {
    expect(projectHarnessPublicError(null)).toEqual({
      code: 'harness.internal',
      message: 'An internal harness error occurred',
    });
    expect(projectHarnessPublicError(undefined)).toEqual({
      code: 'harness.internal',
      message: 'An internal harness error occurred',
    });
  });
});
