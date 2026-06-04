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

describe('respondTo* resume — request-context restoration (§5.1 / C3)', () => {
  it('persists the suspended turn app bag on PendingResume, rebuilds it on resume, and a re-suspend re-persists it', async () => {
    const { harness, agent } = setupHarness();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });

    // Turn 1: suspend on a tool approval with a caller app bag.
    agent.enqueueRun({
      finishReason: 'suspended',
      runId: 'r1',
      suspendPayload: { toolCallId: 'tc1', toolName: 'do_thing', args: {} },
    });
    await session.message({ content: 'go', requestContext: { app: { tenant: 't1', plan: 'pro' } } });

    // The suspended turn's app bag is persisted on the pending resume (storage-internal)...
    const pending = session.getRecord().pendingResume;
    expect(pending?.toolCallId).toBe('tc1');
    expect(pending?.requestContext?.metadata).toEqual({ tenant: 't1', plan: 'pro' });
    // ...and STRIPPED from the public display projection (like runtimeDependencies).
    const displayPending = session.getDisplayState().pending as Record<string, unknown> | null;
    expect(displayPending).not.toBeNull();
    expect(displayPending).not.toHaveProperty('requestContext');

    // Resume → suspend AGAIN, proving the restored bag is re-stashed + re-persisted.
    agent.enqueueRun({
      finishReason: 'suspended',
      runId: 'r1',
      suspendPayload: { toolCallId: 'tc2', toolName: 'do_thing', args: {} },
    });
    await session.respondToToolApproval({ approved: true });

    // The RESUMED run received the SAME app bag the suspended turn carried.
    const resumeCtx = (agent.resumeCalls.at(-1)!.options as any).requestContext;
    expect(resumeCtx).toBeDefined();
    expect(resumeCtx.get('app')).toEqual({ tenant: 't1', plan: 'pro' });

    // The re-suspension re-persisted the bag for the NEXT resume too.
    const pending2 = session.getRecord().pendingResume;
    expect(pending2?.toolCallId).toBe('tc2');
    expect(pending2?.requestContext?.metadata).toEqual({ tenant: 't1', plan: 'pro' });

    // Final resume completes and still carries the bag.
    agent.enqueueRun({ finishReason: 'stop', text: 'done', runId: 'r1' });
    const result = await session.respondToToolApproval({ approved: true });
    expect((result as { text?: string }).text).toBe('done');
    const finalCtx = (agent.resumeCalls.at(-1)!.options as any).requestContext;
    expect(finalCtx.get('app')).toEqual({ tenant: 't1', plan: 'pro' });
  });

  it('resumes a legacy pending (no persisted requestContext) with no app bag — additive back-compat', async () => {
    const { harness, agent } = setupHarness();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });

    // Suspend WITHOUT a caller request context.
    agent.enqueueRun({
      finishReason: 'suspended',
      runId: 'r2',
      suspendPayload: { toolCallId: 'tc1', toolName: 'do_thing', args: {} },
    });
    await session.message({ content: 'go' });
    expect(session.getRecord().pendingResume?.requestContext).toBeUndefined();

    agent.enqueueRun({ finishReason: 'stop', text: 'ok', runId: 'r2' });
    await session.respondToToolApproval({ approved: true });
    const resumeCtx = (agent.resumeCalls.at(-1)!.options as any).requestContext;
    expect(resumeCtx).toBeDefined(); // the harness slot is always rebuilt...
    expect(resumeCtx.get('app')).toBeUndefined(); // ...but there is no app bag to restore
  });
});
