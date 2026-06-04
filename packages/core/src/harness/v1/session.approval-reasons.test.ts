/**
 * Harness v1 — §10.2 `tool_approval_required.approvalReasons`.
 *
 * End-to-end check that a reason captured from a conditional approval predicate (carried on the
 * durable suspend payload) surfaces on the harness `tool_approval_required` event and its
 * `PendingResume`, rather than the previously-hardcoded empty array. Runs against `MockAgent`,
 * which lets us drive a suspended turn with an explicit `suspendPayload`.
 */

import { describe, expect, it } from 'vitest';

import { setupHarness } from './__test-utils__/setup';
import type { HarnessEvent } from './events';

type ApprovalEvent = Extract<HarnessEvent, { type: 'tool_approval_required' }>;

async function approvalEventFor(suspendPayload: Record<string, unknown>): Promise<ApprovalEvent | undefined> {
  const { harness, agent } = setupHarness();
  agent.enqueueRun({ finishReason: 'suspended', runId: 'run-approval', suspendPayload });
  const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
  const events: HarnessEvent[] = [];
  const off = session.subscribe(e => {
    events.push(e);
  });
  await session.message({ content: 'go' });
  off();
  return events.find((e): e is ApprovalEvent => e.type === 'tool_approval_required');
}

describe('tool_approval_required — approvalReasons', () => {
  it('surfaces reasons captured from the approval predicate', async () => {
    const evt = await approvalEventFor({
      toolCallId: 'tc-1',
      toolName: 'shell',
      args: { cmd: 'rm -rf /' },
      approvalReasons: ['destructive command outside workspace'],
    });
    expect(evt).toBeDefined();
    expect(evt!.toolName).toBe('shell');
    expect(evt!.approvalReasons).toEqual(['destructive command outside workspace']);
  });

  it('emits an empty array when the predicate recorded no reason', async () => {
    const evt = await approvalEventFor({ toolCallId: 'tc-2', toolName: 'shell', args: { cmd: 'ls' } });
    expect(evt).toBeDefined();
    expect(evt!.approvalReasons).toEqual([]);
  });
});
