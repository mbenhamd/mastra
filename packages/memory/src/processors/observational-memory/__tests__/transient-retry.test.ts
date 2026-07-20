/**
 * Integration test for the transient-transport-error retry wrapper in OM.
 *
 * Verifies that a transient `terminated`-style failure on the first observer
 * stream call does not kill the actor turn — the wrapper retries and the
 * agent still produces output normally.
 */

import { Agent } from '@mastra/core/agent';
import { InMemoryStore } from '@mastra/core/storage';
import { createTool } from '@mastra/core/tools';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { z } from 'zod';

import { Memory } from '../../../index';
import { ReflectorRunner } from '../reflector-runner';
import { RETRY_CONFIG } from '../retry';

type StreamPart =
  | { type: 'stream-start'; warnings: unknown[] }
  | { type: 'response-metadata'; id: string; modelId: string; timestamp: Date }
  | { type: 'text-start'; id: string }
  | { type: 'text-delta'; id?: string; delta: string }
  | { type: 'text-end'; id: string }
  | { type: 'tool-call'; toolCallId: string; toolName: string; input: string }
  | {
      type: 'finish';
      finishReason: 'stop' | 'tool-calls';
      usage: { inputTokens: number; outputTokens: number; totalTokens: number };
    };

function createMockActorModel(responseText: string) {
  let callCount = 0;

  return {
    specificationVersion: 'v2' as const,
    provider: 'mock',
    modelId: 'mock-actor-model',
    defaultObjectGenerationMode: undefined,
    supportsImageUrls: false,
    supportedUrls: {},

    async doGenerate() {
      const firstCall = callCount === 0;
      callCount++;
      if (firstCall) {
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          finishReason: 'tool-calls' as const,
          usage: { inputTokens: 50, outputTokens: 20, totalTokens: 70 },
          content: [
            {
              type: 'tool-call' as const,
              toolCallId: `call-${Date.now()}`,
              toolName: 'test',
              input: JSON.stringify({ action: 'trigger' }),
            },
          ],
          warnings: [],
        };
      }
      return {
        rawCall: { rawPrompt: null, rawSettings: {} },
        finishReason: 'stop' as const,
        usage: { inputTokens: 100, outputTokens: 50, totalTokens: 150 },
        content: [{ type: 'text' as const, text: responseText }],
        warnings: [],
      };
    },

    async doStream() {
      const firstCall = callCount === 0;
      callCount++;

      const parts: StreamPart[] = firstCall
        ? [
            { type: 'stream-start', warnings: [] },
            { type: 'response-metadata', id: 'r-1', modelId: 'mock-actor-model', timestamp: new Date() },
            {
              type: 'tool-call',
              toolCallId: `call-${Date.now()}`,
              toolName: 'test',
              input: JSON.stringify({ action: 'trigger' }),
            },
            {
              type: 'finish',
              finishReason: 'tool-calls',
              usage: { inputTokens: 50, outputTokens: 20, totalTokens: 70 },
            },
          ]
        : [
            { type: 'stream-start', warnings: [] },
            { type: 'response-metadata', id: 'r-2', modelId: 'mock-actor-model', timestamp: new Date() },
            { type: 'text-start', id: 'text-1' },
            { type: 'text-delta', id: 'text-1', delta: responseText },
            { type: 'text-end', id: 'text-1' },
            {
              type: 'finish',
              finishReason: 'stop',
              usage: { inputTokens: 100, outputTokens: 50, totalTokens: 150 },
            },
          ];

      const stream = new ReadableStream({
        start(controller) {
          for (const p of parts) controller.enqueue(p);
          controller.close();
        },
      });

      return {
        stream,
        rawCall: { rawPrompt: null, rawSettings: {} },
        warnings: [],
      };
    },
  };
}

/**
 * Observer model that throws a `terminated`-style undici error a configurable
 * number of times before succeeding. Observer/Reflector calls disable the AI
 * SDK retry layer, so the observed call count is also the total provider
 * attempt count for the logical OM operation.
 *
 * With `failureMode: 'stream-error'` the model instead emits an
 * OpenRouter-style mid-stream error part ({ code: 502, message, metadata })
 * — the shape OpenRouter injects into the SSE stream when an upstream
 * provider is unavailable. Mid-stream errors bypass the AI SDK's pRetry
 * entirely, so OM's withRetry is the only retry layer.
 */
function createFlakyObserverModel(
  observationsText: string,
  failuresBeforeSuccess: number,
  failureMode: 'throw' | 'stream-error' = 'throw',
) {
  let callCount = 0;
  let observerCallCount = 0;

  function buildSuccessGenerate() {
    return {
      rawCall: { rawPrompt: null, rawSettings: {} },
      finishReason: 'stop' as const,
      usage: { inputTokens: 100, outputTokens: 50, totalTokens: 150 },
      content: [{ type: 'text' as const, text: observationsText }],
      warnings: [],
    };
  }

  return {
    specificationVersion: 'v2' as const,
    provider: 'mock-flaky-observer',
    modelId: 'mock-flaky-observer-model',
    defaultObjectGenerationMode: undefined,
    supportsImageUrls: false,
    supportedUrls: {},

    get __observerCallCount() {
      return observerCallCount;
    },

    async doGenerate() {
      observerCallCount = ++callCount;
      if (callCount <= failuresBeforeSuccess) {
        if (failureMode === 'stream-error') {
          throw Object.assign(new Error('JSON error injected into SSE stream'), {
            code: 502,
            metadata: { error_type: 'provider_unavailable' },
          });
        }
        throw new TypeError('terminated');
      }
      return buildSuccessGenerate();
    },

    async doStream() {
      observerCallCount = ++callCount;
      if (callCount <= failuresBeforeSuccess) {
        if (failureMode === 'stream-error') {
          // OpenRouter mid-stream error: HTTP 200 established, then the raw
          // error object is injected into the SSE stream as an error part.
          const errorParts: unknown[] = [
            { type: 'stream-start', warnings: [] },
            {
              type: 'response-metadata',
              id: 'obs-err',
              modelId: 'mock-flaky-observer-model',
              timestamp: new Date(),
            },
            {
              type: 'error',
              error: {
                code: 502,
                message: 'JSON error injected into SSE stream',
                metadata: { error_type: 'provider_unavailable' },
              },
            },
          ];
          return {
            stream: new ReadableStream({
              start(controller) {
                for (const p of errorParts) controller.enqueue(p);
                controller.close();
              },
            }),
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
          };
        }
        throw new TypeError('terminated');
      }

      const parts: StreamPart[] = [
        { type: 'stream-start', warnings: [] },
        { type: 'response-metadata', id: 'obs-1', modelId: 'mock-flaky-observer-model', timestamp: new Date() },
        { type: 'text-start', id: 'text-1' },
        { type: 'text-delta', id: 'text-1', delta: observationsText },
        { type: 'text-end', id: 'text-1' },
        {
          type: 'finish',
          finishReason: 'stop',
          usage: { inputTokens: 100, outputTokens: 50, totalTokens: 150 },
        },
      ];

      const stream = new ReadableStream({
        start(controller) {
          for (const p of parts) controller.enqueue(p);
          controller.close();
        },
      });

      return {
        stream,
        rawCall: { rawPrompt: null, rawSettings: {} },
        warnings: [],
      };
    },
  };
}

const omTriggerTool = createTool({
  id: 'test',
  description: 'Trigger tool for OM testing',
  inputSchema: z.object({
    action: z.string().optional(),
  }),
  execute: async () => {
    return { success: true, message: 'Tool executed' };
  },
});

const longResponseText = `I understand your request completely. Let me provide you with a comprehensive and detailed response that covers all the important aspects of what you asked about. Here are my thoughts and recommendations based on the information you provided. I hope this detailed explanation helps clarify everything you need to know about the topic at hand. Please let me know if you have any follow-up questions or need additional clarification on any of these points.`;

const observationsText = `<observations>
## What just happened
- 🟢 User greeted and asked for help
</observations>`;

function createReflectorRunner() {
  return new ReflectorRunner({
    reflectionConfig: {
      model: 'mock/model',
      observationTokens: 1_000,
    } as any,
    observationConfig: {
      model: 'mock/model',
      messageTokens: 1_000,
    } as any,
    tokenCounter: {
      countObservations: () => 1,
    } as any,
    storage: {} as any,
    scope: 'thread',
    buffering: {} as any,
    emitDebugEvent: vi.fn(),
    persistMarkerToStorage: vi.fn(),
    persistMarkerToMessage: vi.fn(),
    getCompressionStartLevel: async () => 0,
    resolveModel: () => ({ model: 'mock/model' as any }),
  });
}

describe('OM transient-error retry', { timeout: 30_000 }, () => {
  const originalConfig = { ...RETRY_CONFIG };

  beforeEach(() => {
    // Shrink the schedule so the test stays fast even when retries fire.
    RETRY_CONFIG.maxRetries = 2;
    RETRY_CONFIG.initialDelayMs = 1;
    RETRY_CONFIG.maxDelayMs = 4;
    RETRY_CONFIG.jitter = 0;
    RETRY_CONFIG.timeoutMs = 10_000;
  });

  afterEach(() => {
    vi.useRealTimers();
    Object.assign(RETRY_CONFIG, originalConfig);
  });

  it('caps a persistent sync observation failure at one three-attempt retry budget', async () => {
    // The model would succeed on its sixth call under the old nested retry
    // behavior. The bounded owner must stop after exactly three provider calls.
    const failuresBeforeSuccess = 5;
    const store = new InMemoryStore();
    const observerModel = createFlakyObserverModel(observationsText, failuresBeforeSuccess);

    const memory = new Memory({
      storage: store,
      options: {
        observationalMemory: {
          enabled: true,
          observation: {
            model: observerModel as any,
            messageTokens: 20,
            // Disable async buffering — test the synchronous observation path
            // (the path that previously killed the actor on `terminated`).
            bufferTokens: false,
          },
          reflection: {
            observationTokens: 50_000,
          },
        },
      },
    });

    const agent = new Agent({
      id: 'transient-retry-test-agent',
      name: 'Transient Retry Test Agent',
      instructions: 'You are a helpful assistant. Always use the test tool first.',
      model: createMockActorModel(longResponseText) as any,
      tools: { test: omTriggerTool },
      memory,
    });

    const result = await agent.generate('Hello, I need help.', {
      memory: {
        thread: 'transient-retry-thread',
        resource: 'transient-retry-resource',
      },
    });

    expect(result.tripwire).toMatchObject({
      processorId: 'observational-memory',
      reason: 'Encountered error during memory observation: terminated',
    });
    expect(observerModel.__observerCallCount).toBe(RETRY_CONFIG.maxRetries + 1);
  });

  it('retries observation on OpenRouter-style mid-stream SSE errors (numeric code 502)', async () => {
    // Mid-stream errors never hit the AI SDK's pRetry (the HTTP request
    // succeeded), so OM's withRetry is the only retry layer. Previously these
    // errors were classified as non-transient and killed the actor turn with
    // "Observational memory observation buffering failed: JSON error injected
    // into SSE stream".
    const failuresBeforeSuccess = 2;
    const store = new InMemoryStore();
    const observerModel = createFlakyObserverModel(observationsText, failuresBeforeSuccess, 'stream-error');

    const memory = new Memory({
      storage: store,
      options: {
        observationalMemory: {
          enabled: true,
          observation: {
            model: observerModel as any,
            messageTokens: 20,
            bufferTokens: false,
          },
          reflection: {
            observationTokens: 50_000,
          },
        },
      },
    });

    const agent = new Agent({
      id: 'sse-error-retry-test-agent',
      name: 'SSE Error Retry Test Agent',
      instructions: 'You are a helpful assistant. Always use the test tool first.',
      model: createMockActorModel(longResponseText) as any,
      tools: { test: omTriggerTool },
      memory,
    });

    const result = await agent.generate('Hello, I need help.', {
      memory: {
        thread: 'sse-error-retry-thread',
        resource: 'sse-error-retry-resource',
      },
    });

    // The actor turn completed normally despite mid-stream provider errors.
    expect(result.tripwire).toBeFalsy();
    expect(result.text).toBe(longResponseText);
    expect(observerModel.__observerCallCount).toBe(RETRY_CONFIG.maxRetries + 1);
  });

  it('gives the reflector one bounded retry owner and disables model-layer retries', async () => {
    vi.useFakeTimers();
    RETRY_CONFIG.initialDelayMs = 1_000;
    RETRY_CONFIG.maxDelayMs = 2_000;
    const reflector = createReflectorRunner();
    const modelSettings: Array<{ maxRetries?: number }> = [];
    let providerAttempts = 0;

    vi.spyOn(reflector as any, 'createAgent').mockReturnValue({
      id: 'observational-memory-reflector',
      stream: async (_prompt: unknown, options: { modelSettings: { maxRetries?: number } }) => {
        providerAttempts++;
        modelSettings.push(options.modelSettings);
        throw new TypeError('terminated');
      },
    });

    const result = reflector.call('existing observations');
    const rejection = result.catch(error => error);

    await vi.advanceTimersByTimeAsync(0);
    expect(providerAttempts).toBe(1);
    await vi.advanceTimersByTimeAsync(999);
    expect(providerAttempts).toBe(1);
    await vi.advanceTimersByTimeAsync(1);
    expect(providerAttempts).toBe(2);
    await vi.advanceTimersByTimeAsync(1_999);
    expect(providerAttempts).toBe(2);
    await vi.advanceTimersByTimeAsync(1);
    expect(await rejection).toMatchObject({ message: 'terminated' });

    expect(providerAttempts).toBe(RETRY_CONFIG.maxRetries + 1);
    expect(modelSettings).toHaveLength(RETRY_CONFIG.maxRetries + 1);
    expect(modelSettings.every(settings => settings.maxRetries === 0)).toBe(true);
  });
});
