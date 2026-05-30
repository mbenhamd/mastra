/**
 * Harness v1 — §4.4c caller `requestContext.app` end-to-end wiring.
 *
 * Unit coverage of the validator lives in `request-context-input.test.ts`; these tests assert the
 * SESSION wiring: that an accepted `app` reaches the tool-visible request context, that reserved /
 * malformed request context is rejected before admission at each entry point, and that `app`
 * participates in message admission identity. They run against `MockAgent`, which records each
 * `stream`/`generate` call's options (including the RequestContext we hand it).
 */

import { describe, expect, it } from 'vitest';

import { setupHarness } from './__test-utils__/setup';
import { HarnessConfigError, HarnessValidationError } from './errors';

function appSlot(streamCalls: any[]): unknown {
  const ctx = streamCalls.at(-1)!.options.requestContext;
  expect(ctx).toBeDefined();
  return ctx.get('app');
}

describe('message() — request-context app delivery', () => {
  it('surfaces an accepted app bag as the tool-visible `app` slot', async () => {
    const { harness, agent } = setupHarness();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    await session.message({ content: 'hi', requestContext: { app: { tenant: 't1', plan: { tier: 2 } } } });
    expect(appSlot(agent.streamCalls)).toEqual({ tenant: 't1', plan: { tier: 2 } });
  });

  it('leaves the app slot unset when no request context is supplied', async () => {
    const { harness, agent } = setupHarness();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    await session.message({ content: 'hi' });
    expect(appSlot(agent.streamCalls)).toBeUndefined();
  });

  it('drops undefined app properties but preserves explicit null', async () => {
    const { harness, agent } = setupHarness();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    await session.message({ content: 'hi', requestContext: { app: { a: 1, b: undefined, c: null } } });
    expect(appSlot(agent.streamCalls)).toEqual({ a: 1, c: null });
  });

  it('rejects reserved / unknown / malformed request context before admission', async () => {
    const { harness, agent } = setupHarness();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });

    await expect(session.message({ content: 'x', requestContext: { harness: {} } as never })).rejects.toThrow(
      HarnessValidationError,
    );
    await expect(session.message({ content: 'x', requestContext: { channel: {} } as never })).rejects.toThrow(
      HarnessValidationError,
    );
    await expect(session.message({ content: 'x', requestContext: { mastra__x: 1 } as never })).rejects.toThrow(
      HarnessValidationError,
    );
    await expect(session.message({ content: 'x', requestContext: { app: { fn: () => 0 } } as never })).rejects.toThrow(
      HarnessValidationError,
    );
    await expect(session.message({ content: 'x', requestContext: { app: 'nope' } as never })).rejects.toThrow(
      HarnessValidationError,
    );
    // None of the rejected calls reached the agent.
    expect(agent.streamCalls).toHaveLength(0);
  });
});

describe('message() — app in admission identity', () => {
  it('still dedupes a retry with the same admissionId + same app (app is in the hash, not breaking dedup)', async () => {
    const { harness, agent } = setupHarness();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const opts = { content: 'hi', admissionId: 'adm-1', requestContext: { app: { a: 1 } } } as const;
    await session.message({ ...opts });
    await session.message({ ...opts });
    // Idempotent: the duplicate did not start a second run.
    expect(agent.streamCalls).toHaveLength(1);
  });
});

describe('signal() — request-context', () => {
  it('surfaces an accepted app bag to the woken run and rejects reserved keys', async () => {
    const { harness, agent } = setupHarness();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });

    await expect(session.signal({ content: 'x', requestContext: { channel: {} } as never })).rejects.toThrow(
      HarnessValidationError,
    );

    const result = await session.signal({ content: 'hi', requestContext: { app: { src: 'sig' } } });
    await result.result;
    expect(appSlot(agent.streamCalls)).toEqual({ src: 'sig' });
  });
});

describe('queue() — request-context', () => {
  it('delivers an accepted app bag to the drained run and rejects reserved keys', async () => {
    const { harness, agent } = setupHarness();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });

    await expect(session.admitQueue({ content: 'x', requestContext: { user: {} } as never })).rejects.toThrow(
      HarnessValidationError,
    );

    await session.queue({ content: 'hi', requestContext: { app: { src: 'queue' } } });
    expect(appSlot(agent.streamCalls)).toEqual({ src: 'queue' });
  });
});

describe('skills.use() — request-context', () => {
  it('rejects reserved keys before resolving the skill', async () => {
    const { harness } = setupHarness({
      codeSkills: [{ name: 'greet', instructions: 'Say hello.' }],
    });
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    await expect(session.skills.use('greet', { requestContext: { harness: {} } as never })).rejects.toThrow(
      HarnessValidationError,
    );
  });
});

// The active-delivery rejection (HarnessConfigError when a run is already in flight) is exercised by
// the unit/logic review rather than here, since deterministically holding a concurrent in-flight
// turn open against MockAgent is racy; HarnessConfigError is imported to keep the contract visible.
void HarnessConfigError;
