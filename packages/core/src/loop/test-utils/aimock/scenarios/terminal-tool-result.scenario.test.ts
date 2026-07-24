import { stepCountIs } from '@internal/ai-sdk-v5';
import { describe, expect, it, vi } from 'vitest';
import { z } from 'zod/v4';
import { convertMessages } from '../../../../agent';
import { createTerminalToolResultPartId } from '../../../../agent/message-list';
import { MockMemory } from '../../../../memory';
import { MessageHistory } from '../../../../processors/memory';
import { InMemoryStore } from '../../../../storage';
import type { ChunkType } from '../../../../stream/types';
import { createTool } from '../../../../tools';
import {
  createSharedAgent,
  describeForAllEngines,
  runApprovalScenario,
  runLoopScenario,
  useLoopScenarioAimock,
} from '../aimock-scenario';

describeForAllEngines('AIMock loop scenario: terminal tool result', engine => {
  const getMock = useLoopScenarioAimock();

  it('ends after one successful provider/tool turn and exposes the bounded terminal result', async () => {
    let executions = 0;
    let finishTerminal: unknown;
    const memory = new MockMemory();
    const threadId = `terminal-result-thread-${engine}`;
    const resourceId = `terminal-result-resource-${engine}`;
    const answerTool = createTool({
      id: 'answer_tool',
      description: 'Return the complete answer.',
      inputSchema: z.object({ query: z.string() }),
      outputSchema: z.object({ ok: z.boolean(), answer: z.string() }),
      terminalResult: {
        isSuccess: output => output.ok,
        outputSchema: z.object({ answer: z.string() }),
        project: output => ({ answer: output.answer }),
        maxBytes: 1024,
      },
      execute: async ({ query }) => {
        executions++;
        return { ok: true, answer: `ANSWER:${query}` };
      },
    });

    const { output, requests, chunks } = await runLoopScenario({
      llm: getMock(),
      prompt: 'Answer alpha.',
      tools: { answer_tool: answerTool },
      stopWhen: stepCountIs(5),
      engine,
      memory,
      threadId,
      resourceId,
      collectChunks: true,
      onFinish: event => {
        finishTerminal = event.terminalToolResult;
      },
      fixtures: llm => {
        llm.on(
          { endpoint: 'chat', hasToolResult: false },
          { toolCalls: [{ id: 'call-answer', name: 'answer_tool', arguments: { query: 'alpha' } }] },
        );
      },
    });

    expect(requests).toHaveLength(1);
    expect(executions).toBe(1);
    expect(await output.text).toBe('');
    expect(output.error).toBeUndefined();
    expect(output.terminalToolResult).toEqual({
      status: 'success',
      items: [
        {
          toolName: 'answer_tool',
          toolCallId: 'call-answer',
          status: 'success',
          value: { answer: 'ANSWER:alpha' },
        },
      ],
    });
    expect((await output.getFullOutput()).terminalToolResult).toEqual(output.terminalToolResult);
    expect(finishTerminal).toEqual(output.terminalToolResult);

    const types = chunks?.map(chunk => chunk.type) ?? [];
    expect(types.indexOf('tool-result')).toBeGreaterThan(-1);
    expect(types.indexOf('data-terminal-tool-result')).toBeGreaterThan(types.indexOf('tool-result'));
    expect(types.indexOf('step-finish')).toBeGreaterThan(types.indexOf('data-terminal-tool-result'));
    expect(types.indexOf('finish')).toBeGreaterThan(types.indexOf('step-finish'));

    const recalled = await memory.recall({ threadId, resourceId });
    const persistedTerminalParts = recalled.messages
      .flatMap(message => message.content?.parts ?? [])
      .filter(part => part.type === 'data-terminal-tool-result');
    expect(persistedTerminalParts).toEqual([
      expect.objectContaining({
        type: 'data-terminal-tool-result',
        data: output.terminalToolResult,
      }),
    ]);
  });

  it('keeps terminal delivery with an explicitly pass-through display-only payload transform', async () => {
    const answerTool = createTool({
      id: 'display_redacted_terminal_tool',
      description: 'Return the complete answer while display payloads are redacted.',
      inputSchema: z.object({ secret: z.string() }),
      terminalResult: {
        isSuccess: () => true,
        project: output => ({ answer: output.answer }),
        outputSchema: z.object({ answer: z.string() }),
      },
      execute: async () => ({ answer: 'Safe terminal answer.', raw: 'RAW_TOOL_OUTPUT' }),
    });

    const { output, requests, chunks } = await runLoopScenario({
      llm: getMock(),
      prompt: 'Return the safely projected answer.',
      tools: { display_redacted_terminal_tool: answerTool },
      engine,
      collectChunks: true,
      transform: {
        targets: ['display'],
        terminalToolResultPolicy: 'pass-through',
        transformToolPayload: context => ({ phase: context.phase, redacted: true }),
      },
      fixtures: llm => {
        llm.on(
          { endpoint: 'chat', hasToolResult: false },
          {
            toolCalls: [
              {
                id: 'call-display-redacted-terminal',
                name: 'display_redacted_terminal_tool',
                arguments: { secret: 'RAW_TOOL_INPUT' },
              },
            ],
          },
        );
      },
    });

    expect(requests).toHaveLength(1);
    expect(output.terminalToolResult?.items).toEqual([
      expect.objectContaining({ value: { answer: 'Safe terminal answer.' } }),
    ]);
    expect(chunks?.filter(chunk => chunk.type === 'data-terminal-tool-result')).toHaveLength(1);
    const toolChunks = chunks?.filter(chunk => chunk.type === 'tool-call' || chunk.type === 'tool-result') ?? [];
    expect(JSON.stringify(toolChunks.map(chunk => chunk.metadata))).not.toContain('RAW_TOOL_INPUT');
    expect(JSON.stringify(toolChunks.map(chunk => chunk.metadata))).not.toContain('RAW_TOOL_OUTPUT');
  });

  it('continues normally when the terminal-candidate step already emitted visible text', async () => {
    const answerTool = createTool({
      id: 'preamble_answer_tool',
      description: 'Return the complete answer.',
      inputSchema: z.object({}),
      terminalResult: {
        isSuccess: () => true,
        project: () => ({ answer: 'Specialist answer.' }),
        outputSchema: z.object({ answer: z.string() }),
      },
      execute: async () => ({ ok: true }),
    });

    const { output, requests, chunks } = await runLoopScenario({
      llm: getMock(),
      prompt: 'Answer without splitting delivery.',
      tools: { preamble_answer_tool: answerTool },
      stopWhen: stepCountIs(5),
      engine,
      collectChunks: true,
      fixtures: llm => {
        llm.on(
          { endpoint: 'chat', hasToolResult: false },
          {
            content: 'I will ask a specialist. ',
            toolCalls: [{ id: 'call-preamble', name: 'preamble_answer_tool', arguments: {} }],
          },
        );
        llm.on({ endpoint: 'chat', hasToolResult: true }, { content: 'Specialist answer.' });
      },
    });

    expect(requests).toHaveLength(2);
    expect(output.terminalToolResult).toBeUndefined();
    expect(chunks?.filter(chunk => chunk.type === 'data-terminal-tool-result')).toHaveLength(0);
    expect(await output.text).toContain('Specialist answer.');
  });

  it('delivers a terminal result when thread identifiers are supplied without configured memory', async () => {
    const answerTool = createTool({
      id: 'memoryless_terminal_tool',
      description: 'Return a complete answer without a memory backend.',
      inputSchema: z.object({}),
      terminalResult: {
        isSuccess: () => true,
        project: () => ({ answer: 'Memory is optional for delivery.' }),
        outputSchema: z.object({ answer: z.string() }),
      },
      execute: async () => ({ ok: true }),
    });

    const { output, requests, chunks } = await runLoopScenario({
      llm: getMock(),
      prompt: 'Return the answer without persistence.',
      tools: { memoryless_terminal_tool: answerTool },
      engine,
      threadId: `terminal-memoryless-thread-${engine}`,
      resourceId: `terminal-memoryless-resource-${engine}`,
      collectChunks: true,
      fixtures: llm => {
        llm.on(
          { endpoint: 'chat', hasToolResult: false },
          { toolCalls: [{ id: 'call-memoryless', name: 'memoryless_terminal_tool', arguments: {} }] },
        );
      },
    });

    expect(requests).toHaveLength(1);
    expect(output.terminalToolResult?.items).toEqual([
      expect.objectContaining({
        toolCallId: 'call-memoryless',
        value: { answer: 'Memory is optional for delivery.' },
      }),
    ]);
    expect(chunks?.filter(chunk => chunk.type === 'data-terminal-tool-result')).toHaveLength(1);
  });

  it('bounds a long Unicode run id and persists the same stable terminal identity', async () => {
    const storage = new InMemoryStore();
    const memory = new MockMemory({ storage, enableMessageHistory: false });
    const memoryStore = await storage.getStore('memory');
    const finalTurnHistory = new MessageHistory({
      storage: memoryStore!,
      persistence: { mode: 'final-turn' },
    });
    // Stays within the evented engine's 512-code-unit run-id contract while
    // exceeding MessageHistory's 1 KiB UTF-8 part-id limit in the old scheme.
    const runId = `run-${'界'.repeat(500)}`;
    const threadId = `terminal-long-run-id-${engine}`;
    const resourceId = `terminal-long-run-resource-${engine}`;
    const answerTool = createTool({
      id: 'long_run_id_answer',
      description: 'Return a bounded answer.',
      inputSchema: z.object({}),
      terminalResult: {
        isSuccess: () => true,
        project: () => ({ text: 'Stable identity answer.' }),
        outputSchema: z.object({ text: z.string() }),
      },
      execute: async () => ({ ok: true }),
    });

    const { output, chunks } = await runLoopScenario({
      llm: getMock(),
      engine,
      runId,
      prompt: 'Return the stable answer.',
      tools: { long_run_id_answer: answerTool },
      memory,
      threadId,
      resourceId,
      outputProcessors: [finalTurnHistory],
      collectChunks: true,
      fixtures: llm => {
        llm.on(
          { endpoint: 'chat', hasToolResult: false },
          { toolCalls: [{ id: 'call-long-run-id', name: 'long_run_id_answer', arguments: {} }] },
        );
      },
    });

    expect(output.error).toBeUndefined();
    const liveTerminalPart = chunks?.find(chunk => chunk.type === 'data-terminal-tool-result') as
      | { id: string }
      | undefined;
    const recalled = await memory.recall({ threadId, resourceId, perPage: false });
    const persistedTerminalParts = recalled.messages
      .flatMap(message => message.content.parts)
      .filter(part => part.type === 'data-terminal-tool-result');
    const expectedId = createTerminalToolResultPartId(runId, 1);

    expect(liveTerminalPart?.id).toBe(expectedId);
    expect(new TextEncoder().encode(liveTerminalPart?.id).byteLength).toBeLessThanOrEqual(1_024);
    expect(persistedTerminalParts).toHaveLength(1);
    expect(persistedTerminalParts[0]).toMatchObject({ id: expectedId, data: output.terminalToolResult });
  });

  it('round-trips one stable terminal part through final-turn MessageHistory and AI SDK UI conversion', async () => {
    const storage = new InMemoryStore();
    const memory = new MockMemory({ storage, enableMessageHistory: false });
    const memoryStore = await storage.getStore('memory');
    expect(memoryStore).toBeDefined();
    const finalTurnHistory = new MessageHistory({
      storage: memoryStore!,
      persistence: { mode: 'final-turn' },
    });
    const threadId = `terminal-final-turn-thread-${engine}`;
    const resourceId = `terminal-final-turn-resource-${engine}`;
    const answerTool = createTool({
      id: 'final_turn_answer_tool',
      description: 'Return the complete answer.',
      inputSchema: z.object({}),
      terminalResult: {
        isSuccess: () => true,
        project: () => ({ answer: 'Persisted specialist answer.' }),
        outputSchema: z.object({ answer: z.string() }),
      },
      execute: async () => ({ ok: true }),
    });

    const { output, requests } = await runLoopScenario({
      llm: getMock(),
      prompt: 'Return and persist the specialist answer.',
      tools: { final_turn_answer_tool: answerTool },
      engine,
      memory,
      threadId,
      resourceId,
      outputProcessors: [finalTurnHistory],
      fixtures: llm => {
        llm.on(
          { endpoint: 'chat', hasToolResult: false },
          { toolCalls: [{ id: 'call-final-turn', name: 'final_turn_answer_tool', arguments: {} }] },
        );
      },
    });

    expect(requests).toHaveLength(1);
    const recalled = await memory.recall({ threadId, resourceId, perPage: false });
    const persistedTerminalParts = recalled.messages
      .flatMap(message => message.content?.parts ?? [])
      .filter(part => part.type === 'data-terminal-tool-result');
    expect(persistedTerminalParts).toEqual([
      expect.objectContaining({
        type: 'data-terminal-tool-result',
        id: expect.stringMatching(/^terminal-tool-result:[a-f0-9]{64}:\d+$/),
        data: output.terminalToolResult,
      }),
    ]);

    const uiTerminalParts = convertMessages(recalled.messages)
      .to('AIV5.UI')
      .flatMap(message => message.parts)
      .filter(part => part.type === 'data-terminal-tool-result');
    expect(uiTerminalParts).toHaveLength(1);
    expect(uiTerminalParts[0]).toMatchObject({
      type: 'data-terminal-tool-result',
      id: persistedTerminalParts[0]?.id,
      data: output.terminalToolResult,
    });
  });

  it('recalls the terminal answer exactly once in the next provider request', async () => {
    const storage = new InMemoryStore();
    const memory = new MockMemory({ storage, enableMessageHistory: false });
    const memoryStore = await storage.getStore('memory');
    expect(memoryStore).toBeDefined();
    const finalTurnHistory = new MessageHistory({
      storage: memoryStore!,
      persistence: { mode: 'final-turn' },
    });
    const threadId = `terminal-continuity-thread-${engine}`;
    const resourceId = `terminal-continuity-resource-${engine}`;
    const directAnswer = 'Specialist-authored final answer for the continuity test.';
    const answerTool = createTool({
      id: 'terminal_continuity_tool',
      description: 'Return the complete specialist answer.',
      inputSchema: z.object({}),
      terminalResult: {
        isSuccess: () => true,
        project: () => ({
          kind: 'subagent-direct-answer',
          text: directAnswer,
          subagentSessionId: 'continuity-child-session',
        }),
        outputSchema: z.object({
          kind: z.literal('subagent-direct-answer'),
          text: z.string(),
          subagentSessionId: z.string(),
        }),
      },
      execute: async () => ({ ok: true }),
    });

    const first = await runLoopScenario({
      llm: getMock(),
      prompt: 'Delegate and return the specialist answer.',
      tools: { terminal_continuity_tool: answerTool },
      engine,
      memory,
      threadId,
      resourceId,
      outputProcessors: [finalTurnHistory],
      fixtures: llm => {
        llm.on(
          { endpoint: 'chat', hasToolResult: false },
          { toolCalls: [{ id: 'call-terminal-continuity', name: 'terminal_continuity_tool', arguments: {} }] },
        );
      },
    });

    expect(first.requests).toHaveLength(1);
    getMock().clearFixtures();
    getMock().clearRequests();
    getMock().resetMatchCounts();

    const second = await runLoopScenario({
      llm: getMock(),
      prompt: 'Edit that answer to be shorter.',
      engine,
      memory,
      threadId,
      resourceId,
      inputProcessors: [finalTurnHistory],
      fixtures: llm => {
        llm.on({ endpoint: 'chat' }, { content: 'Shorter specialist answer.' });
      },
    });

    expect(second.requests).toHaveLength(1);
    const serialized = JSON.stringify(second.requests[0]?.body?.messages ?? []);
    expect(serialized.match(new RegExp(directAnswer.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'g'))).toHaveLength(1);
    expect(serialized).toContain('Edit that answer to be shorter.');
    expect(serialized).not.toContain('data-terminal-tool-result');
  });

  it('does not spend a user stopWhen callback after a terminal candidate settles', async () => {
    const stopWhen = vi.fn(() => true);
    const answerTool = createTool({
      id: 'terminal_stop_when_tool',
      description: 'Return the complete answer.',
      inputSchema: z.object({}),
      terminalResult: {
        isSuccess: () => true,
        project: output => output,
        outputSchema: z.object({ answer: z.string() }),
      },
      execute: async () => ({ answer: 'complete' }),
    });

    const { output, requests } = await runLoopScenario({
      llm: getMock(),
      prompt: 'Answer without judging the completed result again.',
      tools: { terminal_stop_when_tool: answerTool },
      engine,
      stopWhen,
      fixtures: llm => {
        llm.on(
          { endpoint: 'chat', hasToolResult: false },
          { toolCalls: [{ id: 'call-terminal-stop-when', name: 'terminal_stop_when_tool', arguments: {} }] },
        );
      },
    });

    expect(requests).toHaveLength(1);
    expect(stopWhen).not.toHaveBeenCalled();
    expect(output.terminalToolResult).toBeDefined();
  });

  it('does not inject or persist callback feedback after a terminal result', async () => {
    const memory = new MockMemory();
    const threadId = `terminal-feedback-thread-${engine}`;
    const resourceId = `terminal-feedback-resource-${engine}`;
    const onIterationComplete = vi.fn(() => ({
      continue: true,
      feedback: 'TERMINAL_CALLBACK_FEEDBACK_MUST_NOT_PERSIST',
    }));
    const answerTool = createTool({
      id: 'terminal_callback_tool',
      description: 'Return the complete answer.',
      inputSchema: z.object({}),
      terminalResult: {
        isSuccess: () => true,
        project: () => ({ answer: 'Complete.' }),
        outputSchema: z.object({ answer: z.string() }),
      },
      execute: async () => ({ ok: true }),
    });

    const { output, requests } = await runLoopScenario({
      llm: getMock(),
      prompt: 'Return the complete answer without repair feedback.',
      tools: { terminal_callback_tool: answerTool },
      engine,
      memory,
      threadId,
      resourceId,
      onIterationComplete,
      fixtures: llm => {
        llm.on(
          { endpoint: 'chat', hasToolResult: false },
          { toolCalls: [{ id: 'call-terminal-callback', name: 'terminal_callback_tool', arguments: {} }] },
        );
      },
    });

    expect(requests).toHaveLength(1);
    expect(onIterationComplete).toHaveBeenCalledTimes(1);
    expect(output.terminalToolResult).toBeDefined();
    const recalled = await memory.recall({ threadId, resourceId, perPage: false });
    expect(JSON.stringify(recalled.messages)).not.toContain('TERMINAL_CALLBACK_FEEDBACK_MUST_NOT_PERSIST');
  });

  it('lets an onIterationComplete abort win at the terminal commit point', async () => {
    const abortController = new AbortController();
    const answerTool = createTool({
      id: 'terminal_callback_abort_tool',
      description: 'Return the complete answer.',
      inputSchema: z.object({}),
      terminalResult: {
        isSuccess: () => true,
        project: output => output,
        outputSchema: z.object({ answer: z.string() }),
      },
      execute: async () => ({ answer: 'must not commit' }),
    });

    const { output, requests, chunks } = await runLoopScenario({
      llm: getMock(),
      prompt: 'Cancel during the iteration callback.',
      tools: { terminal_callback_abort_tool: answerTool },
      engine,
      abortSignal: abortController.signal,
      collectChunks: true,
      onIterationComplete: async () => {
        abortController.abort('cancel during terminal callback');
      },
      fixtures: llm => {
        llm.on(
          { endpoint: 'chat', hasToolResult: false },
          {
            toolCalls: [{ id: 'call-terminal-callback-abort', name: 'terminal_callback_abort_tool', arguments: {} }],
          },
        );
      },
    });

    expect(requests).toHaveLength(1);
    expect(output.terminalToolResult).toBeUndefined();
    expect(chunks.filter(chunk => chunk.type === 'data-terminal-tool-result')).toHaveLength(0);
    expect(await output.finishReason).toBe('abort');
  });

  it('repairs a rejected result, then stops directly after the successful retry', async () => {
    const attempts: number[] = [];
    const analysisTool = createTool({
      id: 'analysis_tool',
      description: 'Run a sandboxed analysis.',
      inputSchema: z.object({ attempt: z.number() }),
      outputSchema: z.object({ ok: z.boolean(), answer: z.string() }),
      terminalResult: {
        isSuccess: output => output.ok,
        project: output => ({ answer: output.answer }),
        outputSchema: z.object({ answer: z.string() }),
      },
      execute: async ({ attempt }) => {
        attempts.push(attempt);
        return attempt === 1
          ? { ok: false, answer: 'pathlib is not allowed' }
          : { ok: true, answer: 'analysis complete' };
      },
    });

    const { output, requests } = await runLoopScenario({
      llm: getMock(),
      prompt: 'Run the analysis.',
      tools: { analysis_tool: analysisTool },
      stopWhen: stepCountIs(5),
      engine,
      fixtures: llm => {
        llm.on(
          { endpoint: 'chat', hasToolResult: false },
          { toolCalls: [{ id: 'call-invalid', name: 'analysis_tool', arguments: { attempt: 1 } }] },
        );
        llm.on(
          { endpoint: 'chat', hasToolResult: true },
          { toolCalls: [{ id: 'call-repair', name: 'analysis_tool', arguments: { attempt: 2 } }] },
        );
      },
    });

    expect(requests).toHaveLength(2);
    expect(attempts).toEqual([1, 2]);
    expect(output.terminalToolResult?.items[0]).toMatchObject({
      toolCallId: 'call-repair',
      value: { answer: 'analysis complete' },
    });
  });

  it('retains the model continuation path when output policy could transform delivery', async () => {
    const answerTool = createTool({
      id: 'policy_answer_tool',
      description: 'Return the complete answer.',
      inputSchema: z.object({}),
      outputSchema: z.object({ ok: z.boolean(), answer: z.string() }),
      terminalResult: {
        isSuccess: output => output.ok,
        project: output => ({ answer: output.answer }),
        outputSchema: z.object({ answer: z.string() }),
      },
      execute: async () => ({ ok: true, answer: 'raw answer' }),
    });

    const { output, requests } = await runLoopScenario({
      llm: getMock(),
      prompt: 'Answer under output policy.',
      tools: { policy_answer_tool: answerTool },
      outputProcessors: [
        {
          id: 'delivery-policy',
          async processOutputStream({ part }: { part: unknown }) {
            return part;
          },
        },
      ],
      stopWhen: stepCountIs(5),
      engine,
      fixtures: llm => {
        llm.on(
          { endpoint: 'chat', hasToolResult: false },
          { toolCalls: [{ id: 'call-policy', name: 'policy_answer_tool', arguments: {} }] },
        );
        llm.on({ endpoint: 'chat', hasToolResult: true }, { content: 'policy-safe final answer' });
      },
    });

    expect(requests).toHaveLength(2);
    expect(await output.text).toBe('policy-safe final answer');
    expect(output.terminalToolResult).toBeUndefined();
  });

  it.each(['predicate-throws', 'projection-throws', 'schema-rejects', 'oversized', 'times-out'] as const)(
    'continues through the parent model when terminal policy %s',
    async failureMode => {
      const terminalResult = (() => {
        switch (failureMode) {
          case 'predicate-throws':
            return {
              isSuccess: () => {
                throw new Error('PRIVATE_PREDICATE_FAILURE');
              },
              project: (output: unknown) => output,
              outputSchema: z.any(),
            };
          case 'projection-throws':
            return {
              isSuccess: () => true,
              project: () => {
                throw new Error('PRIVATE_PROJECTION_FAILURE');
              },
              outputSchema: z.any(),
            };
          case 'schema-rejects':
            return {
              isSuccess: () => true,
              outputSchema: z.object({ answer: z.string() }),
              project: () => ({ invalid: true }),
            };
          case 'oversized':
            return {
              isSuccess: () => true,
              project: () => ({ answer: 'x'.repeat(128) }),
              outputSchema: z.object({ answer: z.string() }),
              maxBytes: 32,
            };
          case 'times-out':
            return {
              evaluationTimeoutMs: 20,
              isSuccess: async (_output: unknown, context: { abortSignal: AbortSignal }) =>
                new Promise<boolean>(resolve =>
                  context.abortSignal.addEventListener('abort', () => resolve(true), { once: true }),
                ),
              project: (output: unknown) => output,
              outputSchema: z.any(),
            };
        }
      })();
      const answerTool = createTool({
        id: `terminal_policy_${failureMode}`,
        description: 'Return an answer whose terminal policy is intentionally invalid.',
        inputSchema: z.object({}),
        terminalResult,
        execute: async () => ({ ok: true }),
      });

      const { output, requests, chunks } = await runLoopScenario({
        llm: getMock(),
        prompt: `Exercise terminal policy ${failureMode}.`,
        tools: { terminal_policy_tool: answerTool },
        stopWhen: stepCountIs(5),
        engine,
        collectChunks: true,
        fixtures: llm => {
          llm.on(
            { endpoint: 'chat', hasToolResult: false },
            { toolCalls: [{ id: `call-${failureMode}`, name: 'terminal_policy_tool', arguments: {} }] },
          );
          llm.on({ endpoint: 'chat', hasToolResult: true }, { content: `ordinary continuation: ${failureMode}` });
        },
      });

      expect(requests).toHaveLength(2);
      expect(output.error).toBeUndefined();
      expect(output.terminalToolResult).toBeUndefined();
      expect(chunks?.filter(chunk => chunk.type === 'data-terminal-tool-result')).toHaveLength(0);
      expect(await output.text).toBe(`ordinary continuation: ${failureMode}`);
    },
  );
});

describeForAllEngines(
  'AIMock loop scenario: approved terminal tool result',
  engine => {
    const getMock = useLoopScenarioAimock();

    it('suspends for approval, executes once, and requires a post-approval model call', async () => {
      let executions = 0;
      const approvedTool = createTool({
        id: 'approved_terminal_tool',
        description: 'Apply the approved edit and return the complete result.',
        inputSchema: z.object({ path: z.string() }),
        outputSchema: z.object({ applied: z.boolean(), path: z.string() }),
        requireApproval: true,
        terminalResult: {
          isSuccess: output => output.applied,
          outputSchema: z.object({ applied: z.boolean(), path: z.string() }),
          project: output => output,
        },
        execute: async ({ path }) => {
          executions++;
          return { applied: true, path };
        },
      });

      const { output, approvals, requests, chunks } = await runApprovalScenario({
        llm: getMock(),
        engine,
        prompt: 'Apply the edit.',
        tools: { approved_terminal_tool: approvedTool },
        stopWhen: stepCountIs(5),
        decision: () => true,
        requireToolApproval: false,
        fixtures: llm => {
          llm.on(
            { endpoint: 'chat', hasToolResult: false },
            {
              toolCalls: [{ id: 'call-approved', name: 'approved_terminal_tool', arguments: { path: 'main.tex' } }],
            },
          );
          llm.on({ endpoint: 'chat', hasToolResult: true }, { content: 'The approved edit was applied.' });
        },
      });

      expect(approvals).toEqual(['approve:call-approved']);
      expect(requests).toHaveLength(2);
      expect(executions).toBe(1);
      expect(output.terminalToolResult).toBeUndefined();
      expect(chunks.filter(chunk => chunk.type === 'data-terminal-tool-result')).toHaveLength(0);
      expect(await output.text).toBe('The approved edit was applied.');
    });

    it('declines without executing or committing a terminal result', async () => {
      let executions = 0;
      const declinedTool = createTool({
        id: 'declined_terminal_tool',
        description: 'Apply an edit only when approved.',
        inputSchema: z.object({ path: z.string() }),
        requireApproval: true,
        terminalResult: {
          isSuccess: output => output.applied,
          outputSchema: z.object({ applied: z.boolean(), path: z.string() }),
          project: output => output,
        },
        execute: async ({ path }) => {
          executions++;
          return { applied: true, path };
        },
      });

      const { output, approvals, requests, chunks } = await runApprovalScenario({
        llm: getMock(),
        engine,
        prompt: 'Apply the edit if I approve it.',
        tools: { declined_terminal_tool: declinedTool },
        stopWhen: stepCountIs(5),
        decision: () => false,
        requireToolApproval: false,
        fixtures: llm => {
          llm.on(
            { endpoint: 'chat', hasToolResult: false },
            {
              toolCalls: [{ id: 'call-declined', name: 'declined_terminal_tool', arguments: { path: 'main.tex' } }],
            },
          );
          llm.on({ endpoint: 'chat', hasToolResult: true }, { content: 'The edit was not applied.' });
        },
      });

      expect(approvals).toEqual(['decline:call-declined']);
      expect(requests).toHaveLength(2);
      expect(executions).toBe(0);
      expect(output.terminalToolResult).toBeUndefined();
      expect(chunks.filter(chunk => chunk.type === 'data-terminal-tool-result')).toHaveLength(0);
      expect(await output.text).toBe('The edit was not applied.');
    });
  },
  { skip: ['fs'] },
);

describeForAllEngines(
  'AIMock loop scenario: resumed suspension cannot terminalize',
  engine => {
    const getMock = useLoopScenarioAimock();

    it('returns a resumed terminal-capable tool to the model before completing', async () => {
      let executions = 0;
      const suspendedTool = createTool({
        id: 'suspended_terminal_tool',
        description: 'Pause, then compute the result after an empty resume.',
        inputSchema: z.object({}),
        suspendSchema: z.object({ question: z.string() }),
        resumeSchema: z.unknown().optional(),
        outputSchema: z.object({ ok: z.boolean(), answer: z.string() }),
        terminalResult: {
          isSuccess: output => output.ok,
          project: output => ({ text: output.answer }),
          outputSchema: z.object({ text: z.string() }),
        },
        execute: async (_input, context) => {
          executions++;
          if (executions === 1) {
            return context.agent?.suspend({ question: 'Provide the value.' });
          }
          return { ok: true, answer: 'computed-after-empty-resume' };
        },
      });
      const shared = await createSharedAgent(getMock(), {
        tools: { suspended_terminal_tool: suspendedTool },
        memory: new MockMemory(),
        engine,
      });

      const initial = await runLoopScenario({
        llm: getMock(),
        engine,
        sharedAgent: shared,
        prompt: 'Compute after asking me.',
        threadId: `terminal-suspension-thread-${engine}`,
        resourceId: `terminal-suspension-resource-${engine}`,
        collectChunks: true,
        fixtures: llm => {
          llm.on(
            { endpoint: 'chat', hasToolResult: false },
            { toolCalls: [{ id: 'call-suspended-terminal', name: 'suspended_terminal_tool', arguments: {} }] },
          );
          llm.on({ endpoint: 'chat', hasToolResult: true }, { content: 'The resumed computation is complete.' });
        },
      });
      expect(initial.chunks?.some(chunk => chunk.type === 'tool-call-suspended')).toBe(true);

      const resumed = await shared.agent.resumeStream(undefined, { runId: initial.output.runId });
      const resumedChunks: ChunkType[] = [];
      for await (const chunk of resumed.fullStream as AsyncIterable<ChunkType>) resumedChunks.push(chunk);

      expect(executions).toBe(2);
      expect(getMock().getRequests()).toHaveLength(2);
      expect(resumed.terminalToolResult).toBeUndefined();
      expect(resumedChunks.filter(chunk => chunk.type === 'data-terminal-tool-result')).toHaveLength(0);
      expect(await resumed.text).toBe('The resumed computation is complete.');
    });
  },
  { skip: ['fs'] },
);

describeForAllEngines('AIMock loop scenario: abort wins terminal arbitration', engine => {
  const getMock = useLoopScenarioAimock();

  it('does not persist or publish a terminal result when abort lands after tool settlement', async () => {
    const abortController = new AbortController();
    const answerTool = createTool({
      id: 'abort_terminal_tool',
      description: 'Return an answer while the caller cancels the run.',
      inputSchema: z.object({}),
      outputSchema: z.object({ ok: z.boolean(), answer: z.string() }),
      terminalResult: {
        isSuccess: output => output.ok,
        project: output => ({ answer: output.answer }),
        outputSchema: z.object({ answer: z.string() }),
      },
      execute: async () => {
        abortController.abort('cancel after tool settlement');
        return { ok: true, answer: 'must not be delivered' };
      },
    });

    const { output, requests, chunks } = await runLoopScenario({
      llm: getMock(),
      prompt: 'Answer, but cancel before finalization.',
      tools: { abort_terminal_tool: answerTool },
      engine,
      abortSignal: abortController.signal,
      collectChunks: true,
      fixtures: llm => {
        llm.on(
          { endpoint: 'chat', hasToolResult: false },
          { toolCalls: [{ id: 'call-abort-terminal', name: 'abort_terminal_tool', arguments: {} }] },
        );
      },
    });

    expect(requests).toHaveLength(1);
    expect(output.terminalToolResult).toBeUndefined();
    expect(chunks.filter(chunk => chunk.type === 'data-terminal-tool-result')).toHaveLength(0);
    expect(await output.finishReason).toBe('abort');
  });
});
