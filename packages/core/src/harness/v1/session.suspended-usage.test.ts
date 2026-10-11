/**
 * PF-5349 #5 — run usage across suspension (REAL agent loop).
 *
 * Live evidence (Writer campaign s3-r1/s3-r2, 2026-10-11): an `agent_end`
 * with `finishReason: 'suspended'` omitted the usage of the model step whose
 * tool call suspended the run (0 when it was the run's first step). That
 * step's `step-finish` is emitted only on resume.
 */

import { describe, expect, it } from 'vitest';
import { z } from 'zod';

import { Agent } from '../../agent';
import { convertArrayToReadableStream, MockLanguageModelV2 } from '../../agent/__tests__/mock-model';
import { Mastra } from '../../mastra';
import { InMemoryStore } from '../../storage';
import { createTool } from '../../tools';

import type { HarnessEvent } from './events';
import { Harness } from './harness';

const stepOneUsage = { inputTokens: 100, outputTokens: 10, totalTokens: 110 };
const stepTwoUsage = { inputTokens: 200, outputTokens: 20, totalTokens: 220 };

function toolCallStream(toolCallId: string, toolName: string, inputJson: string) {
  return convertArrayToReadableStream([
    { type: 'stream-start', warnings: [] },
    { type: 'response-metadata', id: 'id-tool', modelId: 'mock-model-id', timestamp: new Date(0) },
    { type: 'tool-call', toolCallId, toolName, input: inputJson, providerExecuted: false },
    { type: 'finish', finishReason: 'tool-calls', usage: stepOneUsage },
  ]);
}

function textStream(text: string) {
  return convertArrayToReadableStream([
    { type: 'stream-start', warnings: [] },
    { type: 'response-metadata', id: 'id-text', modelId: 'mock-model-id', timestamp: new Date(0) },
    { type: 'text-start', id: 'text-1' },
    { type: 'text-delta', id: 'text-1', delta: text },
    { type: 'text-end', id: 'text-1' },
    { type: 'finish', finishReason: 'stop', usage: stepTwoUsage },
  ]);
}

function newHarness(agent: Agent<any, any, any>) {
  return new Harness({
    agents: { default: agent } as any,
    storage: new InMemoryStore(),
    modes: [{ id: 'default', agentId: 'default' }],
    defaultModeId: 'default',
  });
}

async function waitFor(predicate: () => boolean, label: string, timeoutMs = 4000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${label}`);
    await new Promise(resolve => setImmediate(resolve));
  }
}

function agentEnds(events: HarnessEvent[]) {
  return events.filter(e => e.type === 'agent_end') as Array<{
    finishReason: string;
    usage: { promptTokens: number; completionTokens: number; totalTokens: number };
  }>;
}

describe('run usage across suspension (PF-5349 #5)', () => {
  it('counts the suspending step when the first model step suspends on approval', async () => {
    const applyEdit = createTool({
      id: 'applyEdit',
      description: 'apply an edit',
      inputSchema: z.object({ text: z.string() }),
      requireApproval: true,
      execute: async () => ({ applied: true }),
    });
    let calls = 0;
    const model = new MockLanguageModelV2({
      doStream: async () => {
        calls++;
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: calls === 1 ? toolCallStream('call-edit', 'applyEdit', '{"text":"x"}') : textStream('done'),
        };
      },
    });
    const agent = new Agent({ id: 'default', name: 'default', instructions: 'edit', model, tools: { applyEdit } });
    const harness = newHarness(agent);
    try {
      const session = await harness.session({ resourceId: 'u-usage', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(e => events.push(e));

      const suspended = (await session.message({ content: 'edit it' })) as { finishReason: string };
      expect(suspended.finishReason).toBe('suspended');

      const [suspendedEnd] = agentEnds(events);
      expect(suspendedEnd?.finishReason).toBe('suspended');
      // The provider billed step 1 before the run parked on approval.
      expect(suspendedEnd?.usage.promptTokens).toBe(100);
      expect(session.getTokenUsage().promptTokens).toBe(100);

      await session.respondToToolApproval({ approved: true });
      await waitFor(() => agentEnds(events).some(e => e.finishReason === 'complete'), 'agent_end:complete');

      // Both billed steps reach the session counter exactly once.
      expect(session.getTokenUsage().promptTokens).toBe(300);
      expect(session.getTokenUsage().completionTokens).toBe(30);
    } finally {
      await harness.shutdown();
    }
  });

  it('reports the suspending step on the suspended run output without exposing it on the chunk', async () => {
    const applyEdit = createTool({
      id: 'applyEdit',
      description: 'apply an edit',
      inputSchema: z.object({ text: z.string() }),
      requireApproval: true,
      execute: async () => ({ applied: true }),
    });
    const model = new MockLanguageModelV2({
      doStream: async () => ({
        rawCall: { rawPrompt: null, rawSettings: {} },
        warnings: [],
        stream: toolCallStream('call-edit', 'applyEdit', '{"text":"x"}'),
      }),
    });
    const agent = new Agent({ id: 'default', name: 'default', instructions: 'edit', model, tools: { applyEdit } });

    const stream = await agent.stream('edit it');
    const chunks: Array<{ type: string; payload?: Record<string, unknown> }> = [];
    for await (const chunk of stream.fullStream) chunks.push(chunk as any);

    const approval = chunks.find(chunk => chunk.type === 'tool-call-approval');
    expect(approval).toBeDefined();
    expect(approval?.payload).not.toHaveProperty('stepUsage');
    expect(await stream.finishReason).toBe('suspended');
    expect((await stream.totalUsage).inputTokens).toBe(100);
    expect((await stream.usage).outputTokens).toBe(10);
  });

  it('keeps reporting the suspending step after a cold resume that suspends again', async () => {
    // Two approval-gated calls in one billed step: the run suspends on the first,
    // resumes in a fresh Mastra (no in-process run scope), and suspends on the
    // second. Both suspended results report that step once; the final total
    // counts it once.
    const storage = new InMemoryStore();
    let calls = 0;
    const twoCalls = () =>
      convertArrayToReadableStream([
        { type: 'stream-start', warnings: [] },
        { type: 'response-metadata', id: 'id-tools', modelId: 'mock-model-id', timestamp: new Date(0) },
        {
          type: 'tool-call',
          toolCallId: 'call-1',
          toolName: 'applyEdit',
          input: '{"text":"a"}',
          providerExecuted: false,
        },
        {
          type: 'tool-call',
          toolCallId: 'call-2',
          toolName: 'applyEdit',
          input: '{"text":"b"}',
          providerExecuted: false,
        },
        { type: 'finish', finishReason: 'tool-calls', usage: stepOneUsage },
      ]);
    const freshAgent = () => {
      const applyEdit = createTool({
        id: 'applyEdit',
        description: 'apply an edit',
        inputSchema: z.object({ text: z.string() }),
        requireApproval: true,
        execute: async () => ({ applied: true }),
      });
      const model = new MockLanguageModelV2({
        doStream: async () => {
          calls++;
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: calls === 1 ? twoCalls() : textStream('done'),
          };
        },
      });
      const agent = new Agent({ id: 'editor', name: 'editor', instructions: 'edit', model, tools: { applyEdit } });
      new Mastra({ agents: { editor: agent }, logger: false, storage });
      return agent;
    };

    const first = await freshAgent().stream('edit both');
    for await (const _chunk of first.fullStream) {
      // drain
    }
    expect(await first.finishReason).toBe('suspended');
    expect((await first.totalUsage).inputTokens).toBe(100);

    // Cold: a new Mastra and agent over the same storage.
    const resumed = await freshAgent().approveToolCall({ runId: first.runId, toolCallId: 'call-1' });
    for await (const _chunk of resumed.fullStream) {
      // drain
    }
    expect(await resumed.finishReason).toBe('suspended');
    expect((await resumed.totalUsage).inputTokens).toBe(100);

    const finished = await freshAgent().approveToolCall({ runId: first.runId, toolCallId: 'call-2' });
    for await (const _chunk of finished.fullStream) {
      // drain
    }
    expect(await finished.finishReason).not.toBe('suspended');
    expect((await finished.totalUsage).inputTokens).toBe(300);
  });
});
