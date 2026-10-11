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

/**
 * PF-5349: an ordinary step that narrows its tools but is served from a cached
 * response never reaches the provider. The recovery step after it must still
 * honour that narrowing, not the wider tools of the last dispatched request.
 * The ceiling is loop-internal state and never appears in streamed chunks.
 */
describe('response-only recovery after a cache-served narrowing step', () => {
  it.each(['normal', 'durable', 'evented'] as const)(
    'offers only the tools the cached step still selected [%s]',
    async engine => {
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
                  toolCallId: 'pub-1',
                  toolName: 'publicTool',
                  input: '{}',
                  providerExecuted: false,
                },
                { type: 'finish', finishReason: 'tool-calls', usage },
              ])
            : stream([
                { type: 'text-start', id: 't' },
                { type: 'text-delta', id: 't', delta: 'Done.' },
                { type: 'text-end', id: 't' },
                { type: 'finish', finishReason: 'stop', usage },
              ]);
        },
      });
      let llmRequests = 0;
      const agent = new Agent({
        id: 'narrowing',
        name: 'narrowing',
        instructions: 'answer',
        model,
        tools: {
          publicTool: createTool({
            id: 'publicTool',
            description: 'A public tool',
            inputSchema: z.object({}),
            execute: async () => ({ ok: true }),
          }),
          privateTool: createTool({
            id: 'privateTool',
            description: 'A tool the second step withdraws',
            inputSchema: z.object({}),
            execute: async () => ({ ok: true }),
          }),
        },
        inputProcessors: [
          {
            id: 'response-cache',
            processLLMRequest: () => {
              llmRequests++;
              if (llmRequests !== 2) return undefined;
              // The narrowing second step is answered, empty, from a cache.
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
        agents: { narrowing: registered as any },
        logger: false,
        storage: new InMemoryStore(),
        ...(pubsub ? { pubsub } : {}),
      });

      let step = 0;
      let responseOnly = false;
      const result: any = await mastra.getAgent('narrowing').stream('answer', {
        maxSteps: 3,
        recoveryMaxSteps: 1,
        prepareStep: ({ tools }: { tools?: Record<string, unknown> }) => {
          step += 1;
          // Recovery's processors offer every tool again.
          if (responseOnly) return { tools, toolChoice: 'auto', [AGENT_RESPONSE_RECOVERY_STEP]: true };
          return step === 2 ? { activeTools: ['publicTool'] } : undefined;
        },
        onIterationComplete: (context: IterationCompleteContext) => {
          if (responseOnly) return { continue: false };
          if (context.isFinal && context.text.trim() === '') {
            responseOnly = true;
            return { continue: true, feedback: 'Answer.', [AGENT_RESPONSE_RECOVERY_CONTINUATION]: true };
          }
          return undefined;
        },
      } as any);
      const chunks: unknown[] = [];
      for await (const chunk of (result.output ?? result).fullStream as AsyncIterable<unknown>) {
        chunks.push(chunk);
      }
      result.cleanup?.();

      expect(llmRequests).toBeGreaterThanOrEqual(2);
      // Dispatched: the first step and the recovery step. The cached second
      // step withdrew privateTool, so recovery may not offer it.
      expect(advertised).toEqual([['privateTool', 'publicTool'], ['publicTool']]);
      expect(JSON.stringify(chunks)).not.toContain('recoveryToolCeiling');
    },
  );
});

/**
 * PF-5349: a narrowing attempt that fails before dispatch, followed by a
 * fallback model attempt that selects every tool but is served from a cache,
 * must not restore a tool the run had withdrawn before recovery.
 */
describe('response-only recovery after a failed narrowing attempt', () => {
  it.each([
    ['normal', 'a request processor throws'],
    ['durable', 'a request processor throws'],
    ['evented', 'a request processor throws'],
    ['normal', 'it forces the tool it withdrew'],
    ['durable', 'it forces the tool it withdrew'],
    ['evented', 'it forces the tool it withdrew'],
    ['normal', 'it replaces the definition of the tool it keeps'],
    ['durable', 'it replaces the definition of the tool it keeps'],
    ['evented', 'it replaces the definition of the tool it keeps'],
  ] as const)('keeps the withdrawn tool out of recovery [%s] when %s', async (engine, failure) => {
    // Durable agents reject both selections below before request processors
    // run; the regular agent accepts them and answers that attempt from cache.
    const rejectedBySelectionFence = failure !== 'a request processor throws';
    const advertised: string[][] = [];
    let calls = 0;
    const doStream = async (options: unknown) => {
      calls++;
      advertised.push(((options as any).tools ?? []).map((tool: any) => tool.name).sort());
      return calls === 1
        ? stream([
            { type: 'tool-call', toolCallId: 'pub-1', toolName: 'publicTool', input: '{}', providerExecuted: false },
            { type: 'finish', finishReason: 'tool-calls', usage },
          ])
        : stream([
            { type: 'text-start', id: 't' },
            { type: 'text-delta', id: 't', delta: 'Done.' },
            { type: 'text-end', id: 't' },
            { type: 'finish', finishReason: 'stop', usage },
          ]);
    };
    let llmRequests = 0;
    const agent = new Agent({
      id: 'fallback',
      name: 'fallback',
      instructions: 'answer',
      model: [
        { model: new MockLanguageModelV2({ doStream: doStream as any }), maxRetries: 0 },
        { model: new MockLanguageModelV2({ doStream: doStream as any }), maxRetries: 0 },
      ],
      tools: {
        publicTool: createTool({
          id: 'publicTool',
          description: 'A public tool',
          inputSchema: z.object({}),
          execute: async () => ({ ok: true }),
        }),
        privateTool: createTool({
          id: 'privateTool',
          description: 'A tool the second step withdraws',
          inputSchema: z.object({}),
          execute: async () => ({ ok: true }),
        }),
      },
      inputProcessors: [
        {
          id: 'flaky-cache',
          processLLMRequest: () => {
            llmRequests++;
            // The narrowing attempt fails before dispatch (a forced tool outside
            // the selection is rejected before request processors run, where a
            // fence enforces it).
            if (!rejectedBySelectionFence && llmRequests === 2) throw new Error('request processor failed');
            if (llmRequests !== (rejectedBySelectionFence ? 2 : 3)) return undefined;
            // The next attempt is answered, empty, from a cache.
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
      agents: { fallback: registered as any },
      logger: false,
      storage: new InMemoryStore(),
      ...(pubsub ? { pubsub } : {}),
    });

    let narrowed = false;
    let responseOnly = false;
    const result: any = await mastra.getAgent('fallback').stream('answer', {
      maxSteps: 3,
      recoveryMaxSteps: 1,
      prepareStep: ({ tools, stepNumber }: { tools?: Record<string, unknown>; stepNumber: number }) => {
        if (responseOnly) return { tools, toolChoice: 'auto', [AGENT_RESPONSE_RECOVERY_STEP]: true };
        // Only the first attempt of the second step narrows; the fallback
        // attempt selects every tool again.
        if (stepNumber === 1 && !narrowed) {
          narrowed = true;
          if (failure === 'it forces the tool it withdrew') {
            return { tools: { publicTool: tools?.publicTool }, toolChoice: { type: 'tool', toolName: 'privateTool' } };
          }
          if (failure === 'it replaces the definition of the tool it keeps') {
            return { tools: { publicTool: { ...(tools?.publicTool as object), description: 'Changed' } } };
          }
          return { activeTools: ['publicTool'] };
        }
        return undefined;
      },
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

    expect(llmRequests).toBeGreaterThanOrEqual(rejectedBySelectionFence ? 2 : 3);
    expect(advertised).toEqual([['privateTool', 'publicTool'], ['publicTool']]);
  });
});

/**
 * PF-5349: a request processor that narrows the step's retained active-tools
 * selection and answers from a cache narrows the recovery ceiling too.
 */
describe('response-only recovery after request-processor narrowing', () => {
  it.each(['answers from a cache', 'fails before a cache-served fallback'] as const)(
    'keeps the tool the request processor removed out of recovery when it %s',
    async outcome => {
      const fails = outcome === 'fails before a cache-served fallback';
      const advertised: string[][] = [];
      let calls = 0;
      const doStream = async (options: unknown) => {
        calls++;
        advertised.push(((options as any).tools ?? []).map((tool: any) => tool.name).sort());
        return calls === 1
          ? stream([
              { type: 'tool-call', toolCallId: 'pub-1', toolName: 'publicTool', input: '{}', providerExecuted: false },
              { type: 'finish', finishReason: 'tool-calls', usage },
            ])
          : stream([
              { type: 'text-start', id: 't' },
              { type: 'text-delta', id: 't', delta: 'Done.' },
              { type: 'text-end', id: 't' },
              { type: 'finish', finishReason: 'stop', usage },
            ]);
      };
      const selection = ['publicTool', 'privateTool'];
      let narrowedSelectionOffered = false;
      let llmRequests = 0;
      const agent = new Agent({
        id: 'request-narrowing',
        name: 'request-narrowing',
        instructions: 'answer',
        model: [
          { model: new MockLanguageModelV2({ doStream: doStream as any }), maxRetries: 0 },
          { model: new MockLanguageModelV2({ doStream: doStream as any }), maxRetries: 0 },
        ],
        tools: {
          publicTool: createTool({
            id: 'publicTool',
            description: 'A public tool',
            inputSchema: z.object({}),
            execute: async () => ({ ok: true }),
          }),
          privateTool: createTool({
            id: 'privateTool',
            description: 'A tool the request processor withdraws',
            inputSchema: z.object({}),
            execute: async () => ({ ok: true }),
          }),
        },
        inputProcessors: [
          {
            id: 'narrowing-cache',
            processLLMRequest: () => {
              llmRequests++;
              if (llmRequests === 2) {
                selection.splice(selection.indexOf('privateTool'), 1);
                if (fails) throw new Error('request processor failed');
              }
              // Without a failure the narrowing attempt is answered from a cache;
              // with one, the fallback model's attempt is.
              if (llmRequests !== (fails ? 3 : 2)) return undefined;
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
      new Mastra({ agents: { 'request-narrowing': agent }, logger: false, storage: new InMemoryStore() });

      let responseOnly = false;
      const result = await agent.stream('answer', {
        maxSteps: 3,
        recoveryMaxSteps: 1,
        prepareStep: ({ tools, stepNumber }: { tools?: Record<string, unknown>; stepNumber: number }) => {
          if (responseOnly) return { tools, toolChoice: 'auto', [AGENT_RESPONSE_RECOVERY_STEP]: true };
          // Only the first attempt of the second step uses the retained selection;
          // a fallback attempt selects every tool again.
          if (stepNumber !== 1 || narrowedSelectionOffered) return undefined;
          narrowedSelectionOffered = true;
          return { activeTools: selection };
        },
        onIterationComplete: (context: IterationCompleteContext) => {
          if (responseOnly) return { continue: false };
          if (context.isFinal && context.text.trim() === '') {
            responseOnly = true;
            return { continue: true, feedback: 'Answer.', [AGENT_RESPONSE_RECOVERY_CONTINUATION]: true };
          }
          return undefined;
        },
      } as any);
      for await (const _chunk of result.fullStream) {
        // drain
      }

      expect(advertised).toEqual([['privateTool', 'publicTool'], ['publicTool']]);
    },
  );
});
