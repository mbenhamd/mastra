/**
 * Harness v1 — OM source-write guard ownership while a turn is active (PF-4233).
 */

import { describe, expect, it } from 'vitest';

import { Agent } from '../../agent';
import { InMemoryHarness } from '../../storage/domains/harness/inmemory';
import { InMemoryDB } from '../../storage/domains/inmemory-db';

import { buildFakeOutput } from './__test-utils__/fake-output';
import { HarnessOverrideConflictError } from './errors';
import { Harness } from './harness';

interface FakeCall {
  type: 'stream';
  messages: unknown;
  options: any;
}

class FakeAgent extends Agent<any, any, any> {
  calls: FakeCall[] = [];
  fullOutput: any = {
    text: 'ok',
    usage: { inputTokens: 3, outputTokens: 5, totalTokens: 8 },
    finishReason: 'stop',
    object: undefined,
    steps: [],
    warnings: [],
    providerMetadata: undefined,
    request: {},
    reasoning: [],
    reasoningText: undefined,
    toolCalls: [],
    toolResults: [],
    sources: [],
    files: [],
    response: { id: 'r', timestamp: new Date(), modelId: 'fake', messages: [], uiMessages: [] },
    totalUsage: { inputTokens: 3, outputTokens: 5, totalTokens: 8 },
    error: undefined,
    tripwire: undefined,
    traceId: undefined,
    spanId: undefined,
    runId: 'fake-run',
    suspendPayload: undefined,
    messages: [],
    rememberedMessages: [],
  };
  /** When set, agent.stream() awaits this gate before returning. */
  gate?: Promise<void>;

  constructor(name = 'default') {
    super({ id: name, name, instructions: 'fake', model: 'openai/gpt-4o-mini' as any });
  }

  async stream(messages: any, options?: any): Promise<any> {
    this.calls.push({ type: 'stream', messages, options });
    if (this.gate) await this.gate;
    const out = buildFakeOutput({
      runId: options?.runId ?? this.fullOutput.runId,
      fullOutput: this.fullOutput,
    });
    this._internalRegisterStreamRun(out, (options ?? {}) as any);
    return out;
  }
}

function setup() {
  const agent = new FakeAgent();
  const storage = new InMemoryHarness({ db: new InMemoryDB() });
  const harness = new Harness({
    agents: { default: agent } as any,
    modes: [{ id: 'default', agentId: 'default' }],
    defaultModeId: 'default',
    sessions: { storage },
  });
  return { harness, agent };
}

describe('Session.message() source-write guard ownership', () => {
  it('rejects a replacement guard without ending the active turn that owns the original guard', async () => {
    const { harness, agent } = setup();
    let releaseGate!: () => void;
    agent.gate = new Promise<void>(resolve => (releaseGate = resolve));
    const session = await harness.session({ resourceId: 'r', threadId: { fresh: true } });
    const threadId = session.threadId;
    const guardA = { recordId: 'record-a', threadId, resourceId: 'r' };
    const guardB = { recordId: 'record-b', threadId, resourceId: 'r' };

    const active = session.message({ content: 'first', observationalMemorySourceWriteGuard: guardA });
    await Promise.resolve();
    expect(session.isRunning()).toBe(true);

    await expect(
      session.message({ content: 'steer', observationalMemorySourceWriteGuard: guardB }),
    ).rejects.toBeInstanceOf(HarnessOverrideConflictError);
    expect(session.isRunning()).toBe(true);

    releaseGate();
    await active;
    expect(agent.calls).toHaveLength(1);
    expect(session.isRunning()).toBe(false);
  });
});
