/**
 * PF-2277 discriminator: a duplicate delivery of an admission whose turn has
 * already COMPLETED must attach to the §10.2 receipt and return the stored
 * result — never start a second agent run (live-observed: a replay 4.5 min
 * after run_completed re-ran create_collection and re-suspended its approval).
 */

import { describe, expect, it } from 'vitest';

import { setupHarness } from './__test-utils__';

describe('duplicate admission delivery after completion (§10.2)', () => {
  it('replays the receipt instead of starting a new run', async () => {
    const { harness, agent } = setupHarness();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });

    const first = await session.message({ content: 'create it', admissionId: 'adm-1' });
    const runsAfterFirst = agent.streamCalls.filter(call => call.type === 'stream').length;
    expect(runsAfterFirst).toBe(1);

    const second = await session.message({ content: 'create it', admissionId: 'adm-1' });
    const runsAfterSecond = agent.streamCalls.filter(call => call.type === 'stream').length;

    expect(runsAfterSecond).toBe(1);
    expect(second.runId).toBe(first.runId);
  });

  it('a different admission id still starts a fresh run', async () => {
    const { harness, agent } = setupHarness();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });

    await session.message({ content: 'one', admissionId: 'adm-a' });
    await session.message({ content: 'two', admissionId: 'adm-b' });

    expect(agent.streamCalls.filter(call => call.type === 'stream').length).toBe(2);
  });
});
