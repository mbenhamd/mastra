import { describe, expect, it } from 'vitest';
import { z } from 'zod';

import { Agent } from '../../../agent';
import type { IterationCompleteContext } from '../../../agent';
import { convertArrayToReadableStream, MockLanguageModelV2 } from '../../../agent/__tests__/mock-model';
import { createDurableAgent, createEventedAgent } from '../../../agent/durable';
import {
  AGENT_RESPONSE_RECOVERY_CONTINUATION,
  AGENT_RESPONSE_RECOVERY_STEP,
} from '../../../agent/merge-execution-options';
import { EventEmitterPubSub } from '../../../events';
import { Mastra } from '../../../mastra';
import { InMemoryStore } from '../../../storage';
import { createTool } from '../../../tools';

const usage = { inputTokens: 10, outputTokens: 1, totalTokens: 11 };

function stream(parts: unknown[]) {
  return {
    rawCall: { rawPrompt: null, rawSettings: {} },
    warnings: [],
    stream: convertArrayToReadableStream([
      { type: 'stream-start', warnings: [] },
      { type: 'response-metadata', id: 'r', modelId: 'mock-model-id', timestamp: new Date(0) },
      ...parts,
    ] as any),
  };
}

/**
 * PF-5349: a response-only recovery step never advertises more tools than the
 * latest ordinary request of the run, including when the recovery step is the
 * first provider call after an approval resume (the bound is carried in
 * iteration state, not process memory).
 */
describe('response-only recovery tool ceiling', () => {
  it.each([
    ['normal', 'tool map narrowed by a processor (no activeTools)', 'tools'],
    ['normal', 'active tools narrowed', 'activeTools'],
    ['durable', 'tool map narrowed by a processor (no activeTools)', 'tools'],
    ['durable', 'active tools narrowed', 'activeTools'],
    ['evented', 'tool map narrowed by a processor (no activeTools)', 'tools'],
    ['evented', 'active tools narrowed', 'activeTools'],
  ] as const)(
    'bounds recovery after an approval resume at the step ceiling [%s]: %s',
    async (engine, _label, narrowBy) => {
      const advertised: string[][] = [];
      let calls = 0;
      const model = new MockLanguageModelV2({
        doStream: async options => {
          calls++;
          advertised.push(((options as any).tools ?? []).map((tool: any) => tool.name).sort());
          return calls === 1
            ? stream([
                {
                  type: 'tool-call',
                  toolCallId: 'apply-1',
                  toolName: 'applyChange',
                  input: '{}',
                  providerExecuted: false,
                },
                { type: 'finish', finishReason: 'tool-calls', usage },
              ])
            : stream([
                { type: 'text-start', id: 't' },
                { type: 'text-delta', id: 't', delta: 'Applied.' },
                { type: 'text-end', id: 't' },
                { type: 'finish', finishReason: 'stop', usage },
              ]);
        },
      });
      const applyChange = createTool({
        id: 'applyChange',
        description: 'Apply a change',
        inputSchema: z.object({}),
        requireApproval: true,
        execute: async () => ({ applied: true }),
      });
      const privateTool = createTool({
        id: 'privateTool',
        description: 'A tool the ordinary lane withholds',
        inputSchema: z.object({}),
        execute: async () => ({ ok: true }),
      });
      const agent = new Agent({
        id: 'editor',
        name: 'editor',
        instructions: 'edit',
        model,
        tools: { applyChange, privateTool },
      });
      const pubsub = engine === 'evented' ? new EventEmitterPubSub() : undefined;
      const registered =
        engine === 'durable'
          ? createDurableAgent({ agent })
          : engine === 'evented'
            ? createEventedAgent({ agent, pubsub })
            : agent;
      const mastra = new Mastra({
        agents: { editor: registered as any },
        logger: false,
        storage: new InMemoryStore(),
        ...(pubsub ? { pubsub } : {}),
      });
      const editor = mastra.getAgent('editor');

      let responseOnly = false;
      const options = {
        maxSteps: 1,
        recoveryMaxSteps: 1,
        prepareStep: ({ tools }: { tools?: Record<string, unknown> }) => {
          if (responseOnly) return { [AGENT_RESPONSE_RECOVERY_STEP]: true };
          return narrowBy === 'tools'
            ? { tools: { applyChange: tools?.applyChange } }
            : { activeTools: ['applyChange'] };
        },
        onIterationComplete: (context: IterationCompleteContext) => {
          if (responseOnly) return { continue: false };
          if (context.isFinal && context.text.trim() === '' && context.toolResults.length > 0) {
            responseOnly = true;
            return { continue: true, feedback: 'Report the result.', [AGENT_RESPONSE_RECOVERY_CONTINUATION]: true };
          }
          return undefined;
        },
      } as any;

      const first: any = await editor.stream('apply it', options);
      const firstOutput = first.output ?? first;
      let approvalRequested = false;
      for await (const chunk of firstOutput.fullStream as AsyncIterable<{ type: string }>) {
        if (chunk.type === 'tool-call-approval') {
          approvalRequested = true;
          // A durable run stays suspended; its stream does not end on its own.
          if (engine !== 'normal') break;
        }
      }
      expect(approvalRequested).toBe(true);

      // A durable run keeps the hooks it started with; the regular agent takes
      // them again on resume.
      const resumed: any = await editor.approveToolCall({
        runId: first.runId ?? firstOutput.runId,
        toolCallId: 'apply-1',
        ...(engine === 'normal' ? options : {}),
      });
      for await (const _chunk of (resumed.output ?? resumed).fullStream as AsyncIterable<unknown>) {
        // drain
      }
      first.cleanup?.();
      resumed.cleanup?.();

      expect(advertised[0]).toEqual(['applyChange']);
      // The recovery request (the first call after the resume) offers at most
      // what the ordinary request did.
      expect(advertised).toHaveLength(2);
      expect(advertised[1]).toEqual(['applyChange']);
    },
  );
});

/**
 * PF-5349: with no ordinary request recorded (the only ordinary step was served
 * from a processor's cached response), a response-only recovery step offers no
 * tools rather than whatever its own processors select.
 */
describe('response-only recovery without a recorded request', () => {
  it.each(['normal', 'durable', 'evented'] as const)('is tool-free [%s]', async engine => {
    const advertised: string[][] = [];
    const model = new MockLanguageModelV2({
      doStream: async options => {
        advertised.push(((options as any).tools ?? []).map((tool: any) => tool.name).sort());
        return stream([
          { type: 'text-start', id: 't' },
          { type: 'text-delta', id: 't', delta: 'Done.' },
          { type: 'text-end', id: 't' },
          { type: 'finish', finishReason: 'stop', usage },
        ]);
      },
    });
    let cacheServed = false;
    const agent = new Agent({
      id: 'cached',
      name: 'cached',
      instructions: 'answer',
      model,
      tools: {
        applyChange: createTool({
          id: 'applyChange',
          description: 'Apply a change',
          inputSchema: z.object({}),
          execute: async () => ({ applied: true }),
        }),
      },
      inputProcessors: [
        {
          id: 'response-cache',
          processLLMRequest: () => {
            if (cacheServed) return undefined;
            cacheServed = true;
            // An empty cached answer, so the run asks for one recovery reply.
            return {
              response: {
                chunks: [
                  { type: 'stream-start', payload: { warnings: [] } },
                  { type: 'response-metadata', payload: { id: 'cached', modelId: 'mock', timestamp: new Date(0) } },
                  {
                    type: 'finish',
                    payload: {
                      stepResult: { reason: 'stop' },
                      output: { usage },
                      metadata: {},
                      messages: { all: [], user: [], nonUser: [] },
                    },
                  },
                ],
                warnings: [],
                request: {},
                rawResponse: {},
              },
            } as any;
          },
        },
      ],
    });
    const pubsub = engine === 'evented' ? new EventEmitterPubSub() : undefined;
    const registered =
      engine === 'durable'
        ? createDurableAgent({ agent })
        : engine === 'evented'
          ? createEventedAgent({ agent, pubsub })
          : agent;
    const mastra = new Mastra({
      agents: { cached: registered as any },
      logger: false,
      storage: new InMemoryStore(),
      ...(pubsub ? { pubsub } : {}),
    });

    let responseOnly = false;
    const result: any = await mastra.getAgent('cached').stream('answer', {
      maxSteps: 1,
      recoveryMaxSteps: 1,
      prepareStep: () => (responseOnly ? { [AGENT_RESPONSE_RECOVERY_STEP]: true } : undefined),
      onIterationComplete: (context: IterationCompleteContext) => {
        if (responseOnly) return { continue: false };
        if (context.isFinal && context.text.trim() === '') {
          responseOnly = true;
          return { continue: true, feedback: 'Answer.', [AGENT_RESPONSE_RECOVERY_CONTINUATION]: true };
        }
        return undefined;
      },
    } as any);
    for await (const _chunk of (result.output ?? result).fullStream as AsyncIterable<unknown>) {
      // drain
    }
    result.cleanup?.();

    expect(cacheServed).toBe(true);
    // The only provider request is the recovery step, and it offers no tools.
    expect(advertised).toEqual([[]]);
  });
});
