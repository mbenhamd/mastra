/**
 * Regular and durable-agent observability integration tests.
 *
 * Unlike the unit test in @mastra/core (which mocks the span tracker, so its
 * wrapStream is identity and nothing nests/closes), these drive a REAL
 * Observability instance + the real ModelSpanTracker through a TestExporter and
 * assert the exported span tree: AGENT_RUN root, one MODEL_GENERATION, MODEL_STEP
 * / MODEL_INFERENCE / MODEL_CHUNK closing, TOOL_CALL nesting under its MODEL_STEP,
 * usage on the generation, and zero open spans — for both DurableAgent and
 * EventedAgent.
 */
import { MockLanguageModelV2, convertArrayToReadableStream } from '@internal/ai-sdk-v5/test';
import { Agent } from '@mastra/core/agent';
import { createDurableAgent, createEventedAgent } from '@mastra/core/agent/durable';
import { Mastra } from '@mastra/core/mastra';
import { MockStore } from '@mastra/core/storage';
import { createTool } from '@mastra/core/tools';
import { describe, it, expect, beforeEach, vi } from 'vitest';
import { z } from 'zod';
import { Observability } from './default';
import { TestExporter } from './exporters';

function textModel(text: string) {
  return new MockLanguageModelV2({
    doStream: async () => ({
      stream: convertArrayToReadableStream([
        { type: 'stream-start', warnings: [] },
        { type: 'response-metadata', id: 'id-0', modelId: 'mock-model-id', timestamp: new Date(0) },
        { type: 'text-start', id: 'text-1' },
        { type: 'text-delta', id: 'text-1', delta: text },
        { type: 'text-end', id: 'text-1' },
        { type: 'finish', finishReason: 'stop', usage: { inputTokens: 10, outputTokens: 20, totalTokens: 30 } },
      ]),
      rawCall: { rawPrompt: null, rawSettings: {} },
      warnings: [],
    }),
  });
}

function retryOnceThenTextModel(text: string) {
  const doStream = vi.fn(async () => {
    if (doStream.mock.calls.length === 1) {
      throw new Error('retryable provider failure');
    }
    return {
      stream: convertArrayToReadableStream([
        { type: 'stream-start', warnings: [] },
        { type: 'response-metadata', id: 'retry-ok', modelId: 'mock-model-id', timestamp: new Date(0) },
        { type: 'text-start', id: 'retry-text' },
        { type: 'text-delta', id: 'retry-text', delta: text },
        { type: 'text-end', id: 'retry-text' },
        { type: 'finish', finishReason: 'stop', usage: { inputTokens: 10, outputTokens: 5, totalTokens: 15 } },
      ]),
      rawCall: { rawPrompt: null, rawSettings: {} },
      warnings: [],
    };
  });
  return { model: new MockLanguageModelV2({ doStream }), doStream };
}

function abortableModel() {
  const doStream = vi.fn(async ({ abortSignal }: { abortSignal?: AbortSignal }) => ({
    stream: new ReadableStream({
      start(controller) {
        controller.enqueue({ type: 'stream-start', warnings: [] });
        controller.enqueue({
          type: 'response-metadata',
          id: 'abort-response',
          modelId: 'mock-model-id',
          timestamp: new Date(0),
        });
        controller.enqueue({ type: 'text-start', id: 'abort-text' });
        abortSignal?.addEventListener(
          'abort',
          () => {
            const error = new Error('Aborted');
            error.name = 'AbortError';
            controller.error(error);
          },
          { once: true },
        );
      },
    }),
    rawCall: { rawPrompt: null, rawSettings: {} },
    warnings: [],
  }));
  return { model: new MockLanguageModelV2({ doStream: doStream as any }), doStream };
}

function streamThenError(parts: unknown[], error: Error) {
  let index = 0;
  return new ReadableStream({
    async pull(controller) {
      // Force each part through the adapter before the terminal rejection so
      // this exercises a real mid-stream failure rather than a queued error.
      await new Promise(resolve => setTimeout(resolve, 0));
      if (index < parts.length) {
        controller.enqueue(parts[index++]);
      } else {
        controller.error(error);
      }
    },
  });
}

function providerAbortChunkModel() {
  const abortError = new Error('Provider aborted the stream');
  abortError.name = 'AbortError';
  const doStream = vi.fn(async () => ({
    stream: streamThenError(
      [
        { type: 'stream-start', warnings: [] },
        {
          type: 'response-metadata',
          id: 'provider-abort-id',
          modelId: 'provider-abort-model',
          timestamp: new Date(0),
        },
        { type: 'text-start', id: 'abort-text' },
        { type: 'text-delta', id: 'abort-text', delta: 'partial' },
      ],
      abortError,
    ),
    rawCall: { rawPrompt: null, rawSettings: {} },
    warnings: [],
  }));
  return {
    model: new MockLanguageModelV2({
      provider: 'abort-provider',
      modelId: 'requested-abort-model',
      doStream: doStream as any,
    }),
    doStream,
  };
}

function responseIdentityModel(responseId: string, responseModel: string, providerMetadata?: Record<string, unknown>) {
  return new MockLanguageModelV2({
    provider: 'identity-provider',
    modelId: 'requested-model',
    doStream: async () => ({
      stream: convertArrayToReadableStream([
        { type: 'stream-start', warnings: [] },
        { type: 'response-metadata', id: responseId, modelId: responseModel, timestamp: new Date(0) },
        { type: 'text-start', id: 'identity-text' },
        { type: 'text-delta', id: 'identity-text', delta: 'served' },
        { type: 'text-end', id: 'identity-text' },
        {
          type: 'finish',
          finishReason: 'stop',
          usage: { inputTokens: 4, outputTokens: 2, totalTokens: 6 },
          providerMetadata,
        },
      ]),
      rawCall: { rawPrompt: null, rawSettings: {} },
      warnings: [],
    }),
  });
}

function fallbackModelPair() {
  const primaryDoStream = vi.fn(async () => {
    throw new Error('primary unavailable');
  });
  const fallbackDoStream = vi.fn(async () => ({
    stream: convertArrayToReadableStream([
      { type: 'stream-start', warnings: [] },
      { type: 'response-metadata', id: 'fallback-id', modelId: 'fallback-model', timestamp: new Date(0) },
      { type: 'text-start', id: 'fallback-text' },
      { type: 'text-delta', id: 'fallback-text', delta: 'fallback response' },
      { type: 'text-end', id: 'fallback-text' },
      { type: 'finish', finishReason: 'stop', usage: { inputTokens: 6, outputTokens: 3, totalTokens: 9 } },
    ]),
    rawCall: { rawPrompt: null, rawSettings: {} },
    warnings: [],
  }));
  return {
    primary: new MockLanguageModelV2({
      provider: 'primary-provider',
      modelId: 'primary-model',
      doStream: primaryDoStream,
    }),
    fallback: new MockLanguageModelV2({
      provider: 'fallback-provider',
      modelId: 'fallback-model',
      doStream: fallbackDoStream,
    }),
    primaryDoStream,
    fallbackDoStream,
  };
}

function partialErrorThenTextModel() {
  const doStream = vi.fn(async () => {
    const firstAttempt = doStream.mock.calls.length === 1;
    return {
      stream: firstAttempt
        ? streamThenError(
            [
              { type: 'stream-start', warnings: [] },
              {
                type: 'response-metadata',
                id: 'partial-failure',
                modelId: 'partial-model',
                timestamp: new Date(0),
              },
              { type: 'text-start', id: 'partial-text' },
              { type: 'text-delta', id: 'partial-text', delta: 'partial' },
            ],
            new Error('stream failed after content'),
          )
        : convertArrayToReadableStream([
            { type: 'stream-start', warnings: [] },
            { type: 'response-metadata', id: 'partial-retry', modelId: 'partial-model', timestamp: new Date(0) },
            { type: 'text-start', id: 'retry-text' },
            { type: 'text-delta', id: 'retry-text', delta: 'recovered' },
            { type: 'text-end', id: 'retry-text' },
            { type: 'finish', finishReason: 'stop', usage: { inputTokens: 7, outputTokens: 3, totalTokens: 10 } },
          ]),
      rawCall: { rawPrompt: null, rawSettings: {} },
      warnings: [],
    };
  });
  return { model: new MockLanguageModelV2({ doStream: doStream as any }), doStream };
}

/** First call requests a tool, second call returns final text — a 2-step agentic loop. */
function toolThenTextModel(toolName: string, toolArgs: object, finalText: string) {
  let call = 0;
  return new MockLanguageModelV2({
    doStream: async () => {
      const first = call++ === 0;
      return {
        stream: convertArrayToReadableStream(
          first
            ? [
                { type: 'stream-start', warnings: [] },
                { type: 'response-metadata', id: 'id-0', modelId: 'mock-model-id', timestamp: new Date(0) },
                {
                  type: 'tool-call',
                  toolCallType: 'function',
                  toolCallId: 'call-1',
                  toolName,
                  input: JSON.stringify(toolArgs),
                  providerExecuted: false,
                },
                {
                  type: 'finish',
                  finishReason: 'tool-calls',
                  usage: { inputTokens: 10, outputTokens: 20, totalTokens: 30 },
                },
              ]
            : [
                { type: 'stream-start', warnings: [] },
                { type: 'response-metadata', id: 'id-1', modelId: 'mock-model-id', timestamp: new Date(0) },
                { type: 'text-start', id: 'text-1' },
                { type: 'text-delta', id: 'text-1', delta: finalText },
                { type: 'text-end', id: 'text-1' },
                { type: 'finish', finishReason: 'stop', usage: { inputTokens: 15, outputTokens: 5, totalTokens: 20 } },
              ],
        ),
        rawCall: { rawPrompt: null, rawSettings: {} },
        warnings: [],
      };
    },
  });
}

function delegatingModel(agentKey: string, prompt: string) {
  let call = 0;
  return new MockLanguageModelV2({
    doStream: async () => ({
      stream: convertArrayToReadableStream(
        call++ === 0
          ? [
              { type: 'stream-start', warnings: [] },
              { type: 'response-metadata', id: 'sup-0', modelId: 'mock-model-id', timestamp: new Date(0) },
              {
                type: 'tool-call',
                toolCallType: 'function',
                toolCallId: 'delegate-1',
                toolName: `agent-${agentKey}`,
                input: JSON.stringify({ prompt }),
                providerExecuted: false,
              },
              {
                type: 'finish',
                finishReason: 'tool-calls',
                usage: { inputTokens: 8, outputTokens: 3, totalTokens: 11 },
              },
            ]
          : [
              { type: 'stream-start', warnings: [] },
              { type: 'response-metadata', id: 'sup-1', modelId: 'mock-model-id', timestamp: new Date(0) },
              { type: 'text-start', id: 'sup-text' },
              { type: 'text-delta', id: 'sup-text', delta: 'Delegation complete' },
              { type: 'text-end', id: 'sup-text' },
              {
                type: 'finish',
                finishReason: 'stop',
                usage: { inputTokens: 12, outputTokens: 4, totalTokens: 16 },
              },
            ],
      ),
      rawCall: { rawPrompt: null, rawSettings: {} },
      warnings: [],
    }),
  });
}

/** Emits an error chunk so the run fails mid-stream (exercises the error path). */
function errorModel() {
  return new MockLanguageModelV2({
    doStream: async () => ({
      stream: convertArrayToReadableStream([
        { type: 'stream-start', warnings: [] },
        { type: 'response-metadata', id: 'id-0', modelId: 'mock-model-id', timestamp: new Date(0) },
        { type: 'error', error: new Error('mock model failure') },
      ]),
      rawCall: { rawPrompt: null, rawSettings: {} },
      warnings: [],
    }),
  });
}

const weatherTool = createTool({
  id: 'get_weather',
  description: 'Get the weather for a city',
  inputSchema: z.object({ city: z.string() }),
  outputSchema: z.object({ city: z.string(), tempC: z.number() }),
  execute: async ({ city }: { city: string }) => ({ city, tempC: 21 }),
});

const cachedChunks = [
  { type: 'stream-start', payload: { warnings: [] } },
  {
    type: 'response-metadata',
    payload: { id: 'cached-response', modelId: 'mock-model-id', timestamp: new Date(0) },
  },
  { type: 'text-start', payload: { id: 'cached-text' } },
  { type: 'text-delta', payload: { id: 'cached-text', text: 'Cached answer' } },
  { type: 'text-end', payload: { id: 'cached-text' } },
  {
    type: 'finish',
    payload: {
      stepResult: { reason: 'stop' },
      output: { usage: { inputTokens: 10, outputTokens: 5, totalTokens: 15 } },
      metadata: {},
      messages: { all: [], user: [], nonUser: [] },
    },
  },
];

function buildMastra(testExporter: TestExporter, agent: Agent, variant: 'durable' | 'evented') {
  const wrapped = variant === 'durable' ? createDurableAgent({ agent }) : createEventedAgent({ agent });
  const mastra = new Mastra({
    agents: { wrapped } as any,
    storage: new MockStore(),
    observability: new Observability({
      configs: { test: { serviceName: 'durable-tracing-it', exporters: [testExporter] } },
    }),
  });
  return { mastra, wrapped: mastra.getAgent('wrapped') as any };
}

function buildRegularMastra(testExporter: TestExporter, agent: Agent) {
  const mastra = new Mastra({
    agents: { agent },
    storage: new MockStore(),
    observability: new Observability({
      configs: { test: { serviceName: 'regular-tracing-it', exporters: [testExporter] } },
    }),
  });
  return mastra.getAgent('agent');
}

/** Wait until span delivery to the exporter quiesces (count stable across two checks),
 *  rather than a fixed sleep. Neutral signal — doesn't pre-judge the assertions. */
async function settle(testExporter: TestExporter, maxMs = 2000) {
  let prev = -1;
  for (let waited = 0; waited < maxMs; waited += 20) {
    const n = testExporter.getAllSpans().length;
    if (n > 0 && n === prev) return;
    prev = n;
    await new Promise(r => setTimeout(r, 20));
  }
}

async function runToCompletion(wrapped: any, prompt: string, testExporter: TestExporter) {
  const res = await wrapped.stream(prompt);
  await res.output.consumeStream();
  await settle(testExporter);
  res.cleanup?.();
}

async function runRegularToCompletion(agent: Agent, prompt: string, testExporter: TestExporter) {
  const res = await agent.stream(prompt);
  await res.consumeStream();
  await settle(testExporter);
}

async function waitForProviderCall(providerCall: ReturnType<typeof vi.fn>, maxMs = 2000) {
  for (let waited = 0; waited < maxMs; waited += 10) {
    if (providerCall.mock.calls.length > 0) return;
    await new Promise(resolve => setTimeout(resolve, 10));
  }
  throw new Error('Timed out waiting for the provider call');
}

const idOf = (s: any) => s.id ?? s.spanId;
const parentOf = (s: any) => s.parentSpanId;

const aggregateFields = [
  ['providerMessageCount', 'providerMessageCountTotal'],
  ['providerMessageBytes', 'providerMessageBytesTotal'],
  ['providerSystemMessageCount', 'providerSystemMessageCountTotal'],
  ['providerSystemMessageBytes', 'providerSystemMessageBytesTotal'],
  ['providerUserMessageCount', 'providerUserMessageCountTotal'],
  ['providerUserMessageBytes', 'providerUserMessageBytesTotal'],
  ['providerAssistantMessageCount', 'providerAssistantMessageCountTotal'],
  ['providerAssistantMessageBytes', 'providerAssistantMessageBytesTotal'],
  ['providerToolMessageCount', 'providerToolMessageCountTotal'],
  ['providerToolMessageBytes', 'providerToolMessageBytesTotal'],
  ['providerOtherMessageCount', 'providerOtherMessageCountTotal'],
  ['providerOtherMessageBytes', 'providerOtherMessageBytesTotal'],
  ['providerInstructionBytes', 'providerInstructionBytesTotal'],
  ['providerToolCount', 'providerToolCountTotal'],
  ['providerToolSchemaBytes', 'providerToolSchemaBytesTotal'],
  ['providerResponseSchemaBytes', 'providerResponseSchemaBytesTotal'],
  ['providerRequestBytes', 'providerRequestBytesTotal'],
  ['providerPreparationMs', 'providerPreparationMsTotal'],
  ['providerMeasurementMs', 'providerMeasurementMsTotal'],
] as const;

function exactAggregateValue(attributes: Record<string, any>, field: (typeof aggregateFields)[number][0]) {
  if (field === 'providerToolSchemaBytes' && attributes.providerToolSchemaState === 'not_applicable') return 0;
  if (field === 'providerResponseSchemaBytes' && attributes.providerResponseSchemaState === 'not_applicable') return 0;
  return attributes[field];
}

function expectExactRequestAggregate(generation: any, inferences: any[]) {
  const generationAttributes = generation?.attributes as Record<string, any> | undefined;
  expect(generationAttributes).toBeDefined();
  const measured = inferences.filter(span => span.attributes?.measurementState === 'measured').length;
  const unknown = inferences.filter(span => span.attributes?.measurementState === 'unknown').length;
  expect(generationAttributes).toMatchObject({
    providerInferenceCount: inferences.length,
    providerMeasuredInferenceCount: measured,
    providerUnknownInferenceCount: unknown,
  });
  for (const [inferenceField, aggregateField] of aggregateFields) {
    const values = inferences.map(span => exactAggregateValue(span.attributes ?? {}, inferenceField));
    if (values.every(value => typeof value === 'number')) {
      expect(generationAttributes?.[aggregateField], aggregateField).toBe(
        values.reduce((total, value) => total + value, 0),
      );
    } else {
      expect(generationAttributes?.[aggregateField], aggregateField).toBeUndefined();
    }
  }
}

describe('regular Agent provider request ledger (real exporter)', () => {
  let testExporter: TestExporter;
  beforeEach(() => {
    testExporter = new TestExporter();
  });

  it('exports each provider retry as a distinct inference and aggregates both requests', async () => {
    const { model, doStream } = retryOnceThenTextModel('Recovered');
    const agent = buildRegularMastra(
      testExporter,
      new Agent({
        id: 'retry',
        name: 'retry',
        instructions: 'x',
        model,
        maxRetries: 1,
      }),
    );

    await runRegularToCompletion(agent, 'hi', testExporter);

    expect(doStream).toHaveBeenCalledTimes(2);
    const inferences = testExporter.getSpansByType('model_inference' as any);
    expect(inferences).toHaveLength(2);
    expect(new Set(inferences.map(idOf))).toHaveLength(2);
    expect(inferences.map(span => span.attributes?.providerAttempt)).toEqual([1, 2]);
    expect(inferences[0]).toMatchObject({
      attributes: {
        measurementState: 'measured',
        providerUsageState: 'provider_not_reported',
        providerOutcome: 'error',
      },
      errorInfo: expect.any(Object),
    });
    expect(inferences[1]?.attributes).toMatchObject({
      measurementState: 'measured',
      providerUsageState: 'reported',
      providerOutcome: 'success',
    });
    const [generation] = testExporter.getSpansByType('model_generation' as any);
    expect(generation?.attributes).toMatchObject({
      providerAggregateMeasurementState: 'measured',
      providerInferenceCount: 2,
      providerUsageState: 'provider_not_reported',
      providerSucceededInferenceCount: 1,
      providerErrorInferenceCount: 1,
      providerAbortedInferenceCount: 0,
    });
    expectExactRequestAggregate(generation, inferences);
    expect(testExporter.getIncompleteSpans()).toHaveLength(0);
  });

  it('closes and classifies a publicly aborted provider attempt', async () => {
    const { model, doStream } = abortableModel();
    const abortController = new AbortController();
    const agent = buildRegularMastra(
      testExporter,
      new Agent({ id: 'regular-abort', name: 'regular-abort', instructions: 'x', model: model as any, maxRetries: 0 }),
    );

    const result = await agent.stream('hi', { abortSignal: abortController.signal });
    await waitForProviderCall(doStream);
    abortController.abort();
    try {
      await result.consumeStream();
    } catch {
      // Public cancellation may reject the consumer after the abort signal wins.
    }
    await settle(testExporter);

    const [inference] = testExporter.getSpansByType('model_inference' as any);
    expect(inference?.attributes).toMatchObject({
      providerOutcome: 'abort',
      finishReason: 'abort',
      providerUsageState: 'provider_not_reported',
      measurementState: 'measured',
    });
    const [generation] = testExporter.getSpansByType('model_generation' as any);
    expect(generation?.attributes).toMatchObject({
      finishReason: 'aborted',
      providerInferenceCount: 1,
      providerSucceededInferenceCount: 0,
      providerErrorInferenceCount: 0,
      providerAbortedInferenceCount: 1,
    });
    expect(testExporter.getIncompleteSpans()).toHaveLength(0);
  });

  it('closes a provider-originated abort stream without waiting for step-finish', async () => {
    const { model, doStream } = providerAbortChunkModel();
    const agent = buildRegularMastra(
      testExporter,
      new Agent({
        id: 'provider-abort',
        name: 'provider-abort',
        instructions: 'x',
        model: model as any,
        maxRetries: 0,
      }),
    );

    try {
      await runRegularToCompletion(agent, 'hi', testExporter);
    } catch {
      await settle(testExporter);
    }

    expect(doStream).toHaveBeenCalledOnce();
    const [inference] = testExporter.getSpansByType('model_inference' as any);
    expect(inference?.attributes).toMatchObject({
      providerOutcome: 'abort',
      finishReason: 'abort',
      completionStartTime: expect.anything(),
    });
    const [generation] = testExporter.getSpansByType('model_generation' as any);
    expect(generation?.attributes).toMatchObject({
      providerSucceededInferenceCount: 0,
      providerErrorInferenceCount: 0,
      providerAbortedInferenceCount: 1,
    });
    expect(testExporter.getIncompleteSpans()).toHaveLength(0);
  });

  it('numbers fallback-model calls cumulatively within one regular model step', async () => {
    const { primary, fallback, primaryDoStream, fallbackDoStream } = fallbackModelPair();
    const agent = buildRegularMastra(
      testExporter,
      new Agent({
        id: 'regular-fallback',
        name: 'regular-fallback',
        instructions: 'x',
        model: [
          { id: 'primary', model: primary as any, maxRetries: 0 },
          { id: 'fallback', model: fallback as any, maxRetries: 0 },
        ],
      }),
    );

    await runRegularToCompletion(agent, 'hi', testExporter);

    expect(primaryDoStream).toHaveBeenCalledOnce();
    expect(fallbackDoStream).toHaveBeenCalledOnce();
    const inferences = testExporter.getSpansByType('model_inference' as any);
    expect(inferences.map(span => span.attributes?.providerAttempt)).toEqual([1, 2]);
    expect(inferences.map(span => span.attributes?.providerOutcome)).toEqual(['error', 'success']);
    const [generation] = testExporter.getSpansByType('model_generation' as any);
    expect(generation?.attributes).toMatchObject({
      providerInferenceCount: 2,
      providerSucceededInferenceCount: 1,
      providerErrorInferenceCount: 1,
      providerAbortedInferenceCount: 0,
    });
    expect(testExporter.getIncompleteSpans()).toHaveLength(0);
  });

  it('attributes the regular inference to normalized provider response identity', async () => {
    const agent = buildRegularMastra(
      testExporter,
      new Agent({
        id: 'regular-response-identity',
        name: 'regular-response-identity',
        instructions: 'x',
        model: responseIdentityModel('served-response', 'requested-response-model', {
          anthropic: { iterations: [{ type: 'fallback_message', model: 'served-fallback-model' }] },
        }) as any,
      }),
    );

    await runRegularToCompletion(agent, 'hi', testExporter);

    const [inference] = testExporter.getSpansByType('model_inference' as any);
    expect(inference?.attributes).toMatchObject({
      responseId: 'served-response',
      responseModel: 'served-fallback-model',
      providerOutcome: 'success',
    });
    expect(testExporter.getIncompleteSpans()).toHaveLength(0);
  });

  it('keeps a regular tool call inside its live MODEL_STEP after inference closes', async () => {
    const agent = buildRegularMastra(
      testExporter,
      new Agent({
        id: 'regular-tool-lifecycle',
        name: 'regular-tool-lifecycle',
        instructions: 'use the tool',
        model: toolThenTextModel('get_weather', { city: 'Paris' }, 'It is 21C in Paris.') as any,
        tools: { get_weather: weatherTool },
      }),
    );

    await runRegularToCompletion(agent, 'weather?', testExporter);

    const [toolCall] = testExporter.getSpansByType('tool_call' as any);
    const owningStep = testExporter.getSpansByType('model_step' as any).find(span => idOf(span) === parentOf(toolCall));
    const owningInference = testExporter
      .getSpansByType('model_inference' as any)
      .find(span => parentOf(span) === idOf(owningStep));
    expect(toolCall).toBeDefined();
    expect(owningStep).toBeDefined();
    expect(owningInference).toBeDefined();
    expect(new Date(owningInference!.endTime!).getTime()).toBeLessThanOrEqual(new Date(toolCall!.startTime!).getTime());
    expect(new Date(owningStep!.endTime!).getTime()).toBeGreaterThanOrEqual(new Date(toolCall!.endTime!).getTime());
    expect(testExporter.getIncompleteSpans()).toHaveLength(0);
  });

  it('exports zero inferences and not_applicable states for a cached response', async () => {
    const providerCall = vi.fn();
    const agent = buildRegularMastra(
      testExporter,
      new Agent({
        id: 'cached',
        name: 'cached',
        instructions: 'x',
        model: new MockLanguageModelV2({ doStream: providerCall }) as any,
        inputProcessors: [
          {
            id: 'response-cache',
            processLLMRequest: () => ({
              response: {
                chunks: cachedChunks,
                warnings: [],
                request: {},
                rawResponse: { status: 200 },
              },
            }),
          },
        ],
      }),
    );

    await runRegularToCompletion(agent, 'hi', testExporter);

    expect(providerCall).not.toHaveBeenCalled();
    expect(testExporter.getSpansByType('model_inference' as any)).toHaveLength(0);
    const [generation] = testExporter.getSpansByType('model_generation' as any);
    expect(generation?.attributes).toMatchObject({
      providerAggregateMeasurementState: 'not_applicable',
      providerInferenceCount: 0,
      providerMeasuredInferenceCount: 0,
      providerUnknownInferenceCount: 0,
      providerUsageState: 'not_applicable',
    });
    expectExactRequestAggregate(generation, []);
    expect(testExporter.getIncompleteSpans()).toHaveLength(0);
  });
});

describe('durable-agent observability — full span tree (real exporter)', () => {
  let testExporter: TestExporter;
  beforeEach(() => {
    testExporter = new TestExporter();
  });

  it('DurableAgent simple run: AGENT_RUN root → generation → step → inference → chunk, all closed', async () => {
    const agent = new Agent({ id: 'a', name: 'a', instructions: 'x', model: textModel('Hello') as any });
    const { wrapped } = buildMastra(testExporter, agent, 'durable');
    await runToCompletion(wrapped, 'hi', testExporter);

    expect(testExporter.getTraceIds()).toHaveLength(1);
    const agentRuns = testExporter.getSpansByType('agent_run' as any);
    const generations = testExporter.getSpansByType('model_generation' as any);
    expect(agentRuns).toHaveLength(1);
    expect(generations).toHaveLength(1);
    // root is the agent_run, generation nests under it
    expect(testExporter.getRootSpans().map(idOf)).toContain(idOf(agentRuns[0]));
    expect(parentOf(generations[0])).toBe(idOf(agentRuns[0]));
    // step / inference / chunk exist and nest
    expect(testExporter.getSpansByType('model_step' as any).length).toBeGreaterThanOrEqual(1);
    const inferences = testExporter.getSpansByType('model_inference' as any);
    expect(inferences).toHaveLength(1);
    expect(inferences[0]?.attributes).toMatchObject({
      measurementState: 'measured',
      providerMessageCount: 2,
      providerRequestBytes: expect.any(Number),
    });
    expect(generations[0]?.attributes).toMatchObject({
      providerAggregateMeasurementState: 'measured',
      providerInferenceCount: 1,
      providerRequestBytesTotal: expect.any(Number),
    });
    // the whole point: nothing dangling
    expect(testExporter.getIncompleteSpans()).toHaveLength(0);
  });

  it('does not report a provider inference when a durable processor replays a cached response', async () => {
    const providerCall = vi.fn();
    const agent = new Agent({
      id: 'cached',
      name: 'cached',
      instructions: 'x',
      model: new MockLanguageModelV2({ doStream: providerCall }) as any,
      inputProcessors: [
        {
          id: 'response-cache',
          processLLMRequest: () => ({
            response: {
              chunks: cachedChunks,
              warnings: [],
              request: {},
              rawResponse: { status: 200 },
            },
          }),
        },
      ],
    });
    const { wrapped } = buildMastra(testExporter, agent, 'durable');
    await runToCompletion(wrapped, 'hi', testExporter);

    expect(providerCall).not.toHaveBeenCalled();
    expect(testExporter.getSpansByType('model_inference' as any)).toHaveLength(0);
    const [generation] = testExporter.getSpansByType('model_generation' as any);
    expect(generation?.attributes).toMatchObject({
      providerAggregateMeasurementState: 'not_applicable',
      providerInferenceCount: 0,
      providerUsageState: 'not_applicable',
    });
    expectExactRequestAggregate(generation, []);
    expect(testExporter.getIncompleteSpans()).toHaveLength(0);
  });

  it('attributes durable processor model and reasoning overrides to the effective provider call', async () => {
    const originalDoStream = vi.fn();
    const replacementDoStream = vi.fn(async () => ({
      stream: convertArrayToReadableStream([
        { type: 'stream-start', warnings: [] },
        { type: 'response-metadata', id: 'replacement-id', modelId: 'replacement-model', timestamp: new Date(0) },
        { type: 'text-start', id: 'replacement-text' },
        { type: 'text-delta', id: 'replacement-text', delta: 'Replacement response' },
        { type: 'text-end', id: 'replacement-text' },
        { type: 'finish', finishReason: 'stop', usage: { inputTokens: 7, outputTokens: 3, totalTokens: 10 } },
      ]),
      rawCall: { rawPrompt: null, rawSettings: {} },
      warnings: [],
    }));
    const originalModel = new MockLanguageModelV2({
      provider: 'original-provider',
      modelId: 'original-model',
      doStream: originalDoStream,
    });
    const replacementModel = new MockLanguageModelV2({
      provider: 'azure.responses',
      modelId: 'replacement-model',
      doStream: replacementDoStream,
    });
    const agent = new Agent({
      id: 'durable-model-override',
      name: 'durable-model-override',
      instructions: 'x',
      model: originalModel as any,
      inputProcessors: [
        {
          id: 'effective-provider-router',
          processInputStep: async () => ({
            model: replacementModel as any,
            providerOptions: { azure: { reasoningEffort: 'low' } },
          }),
        },
      ],
    });
    const { wrapped } = buildMastra(testExporter, agent, 'durable');
    await runToCompletion(wrapped, 'hi', testExporter);

    expect(originalDoStream).not.toHaveBeenCalled();
    expect(replacementDoStream).toHaveBeenCalledOnce();
    expect(replacementDoStream.mock.calls[0]?.[0]).toMatchObject({
      providerOptions: { azure: { reasoningEffort: 'low' } },
    });
    const [generation] = testExporter.getSpansByType('model_generation' as any);
    expect(generation).toMatchObject({
      name: "llm: 'replacement-model'",
      attributes: { model: 'replacement-model', provider: 'azure.responses' },
    });
    const [inference] = testExporter.getSpansByType('model_inference' as any);
    expect(inference?.attributes).toMatchObject({
      model: 'replacement-model',
      provider: 'azure.responses',
      providerReasoningEffortState: 'measured',
      providerReasoningEffort: 'low',
    });
    expect(testExporter.getIncompleteSpans()).toHaveLength(0);
  });

  it('DurableAgent tool call: ONE generation, 2 steps, TOOL_CALL nested under a model_step, all closed', async () => {
    const agent = new Agent({
      id: 'a',
      name: 'a',
      instructions: 'use the tool',
      model: toolThenTextModel('get_weather', { city: 'Paris' }, 'It is 21C in Paris.') as any,
      tools: { get_weather: weatherTool },
    });
    const { wrapped } = buildMastra(testExporter, agent, 'durable');
    await runToCompletion(wrapped, 'weather in paris?', testExporter);

    expect(testExporter.getTraceIds()).toHaveLength(1);
    expect(testExporter.getSpansByType('agent_run' as any)).toHaveLength(1);
    const generations = testExporter.getSpansByType('model_generation' as any);
    expect(generations).toHaveLength(1); // Option A: one generation across the loop
    const steps = testExporter.getSpansByType('model_step' as any);
    expect(steps.length).toBeGreaterThanOrEqual(2); // tool-call step + final step
    const toolCalls = testExporter.getSpansByType('tool_call' as any);
    expect(toolCalls).toHaveLength(1);
    // TOOL_CALL nests under a MODEL_STEP (not agent_run / workflow)
    expect(steps.map(idOf)).toContain(parentOf(toolCalls[0]));
    // generation carries usage; nothing dangling
    expect((generations[0] as any).attributes?.usage).toBeDefined();
    const inferences = testExporter.getSpansByType('model_inference' as any);
    expect(inferences).toHaveLength(2);
    expect(inferences.every(span => span.attributes?.measurementState === 'measured')).toBe(true);
    const orderedInferences = [...inferences].sort(
      (left, right) => (left.attributes?.stepIndex ?? 0) - (right.attributes?.stepIndex ?? 0),
    );
    const [toolRequest, toolResultRequest] = orderedInferences;
    expect(toolRequest?.attributes).toMatchObject({
      providerSystemMessageCount: 1,
      providerUserMessageCount: 1,
      providerAssistantMessageCount: 0,
      providerToolMessageCount: 0,
      providerToolCount: 1,
      providerToolSchemaState: 'measured',
      providerResponseSchemaState: 'not_applicable',
    });
    expect(toolRequest?.attributes?.providerSystemMessageBytes).toBeGreaterThan(0);
    expect(toolRequest?.attributes?.providerUserMessageBytes).toBeGreaterThan(0);
    expect(toolRequest?.attributes?.providerInstructionBytes).toBeGreaterThan(0);
    expect(toolRequest?.attributes?.providerToolSchemaBytes).toBeGreaterThan(0);
    expect(toolRequest?.attributes?.providerRequestBytes).toBeGreaterThan(0);
    expect(toolResultRequest?.attributes?.providerMessageCount).toBeGreaterThan(
      toolRequest?.attributes?.providerMessageCount,
    );
    expect(toolResultRequest?.attributes?.providerAssistantMessageCount).toBeGreaterThan(0);
    expect(toolResultRequest?.attributes?.providerAssistantMessageBytes).toBeGreaterThan(0);
    expect(toolResultRequest?.attributes?.providerToolMessageCount).toBeGreaterThan(0);
    expect(toolResultRequest?.attributes?.providerToolMessageBytes).toBeGreaterThan(0);
    expect(toolResultRequest?.attributes?.providerRequestBytes).toBeGreaterThan(
      toolRequest?.attributes?.providerRequestBytes,
    );
    expect(generations[0]?.attributes).toMatchObject({
      providerAggregateMeasurementState: 'measured',
      providerInferenceCount: 2,
    });
    expectExactRequestAggregate(generations[0], inferences);
    expect(testExporter.getIncompleteSpans()).toHaveLength(0);
  });

  it('retains every durable retry attempt in the generation aggregate', async () => {
    const { model, doStream } = retryOnceThenTextModel('Recovered durably');
    const agent = new Agent({
      id: 'durable-retry',
      name: 'durable-retry',
      instructions: 'x',
      model: [{ id: 'primary', model: model as any, maxRetries: 1 }],
    });
    const { wrapped } = buildMastra(testExporter, agent, 'durable');
    await runToCompletion(wrapped, 'hi', testExporter);

    expect(doStream).toHaveBeenCalledTimes(2);
    const inferences = testExporter.getSpansByType('model_inference' as any);
    expect(inferences).toHaveLength(2);
    expect(new Set(inferences.map(idOf))).toHaveLength(2);
    expect(inferences.map(span => span.attributes?.providerAttempt)).toEqual([1, 2]);
    expect(inferences[0]).toMatchObject({
      attributes: {
        measurementState: 'measured',
        providerUsageState: 'provider_not_reported',
        providerOutcome: 'error',
      },
      errorInfo: expect.any(Object),
    });
    expect(inferences[1]?.attributes).toMatchObject({
      measurementState: 'measured',
      providerUsageState: 'reported',
      providerOutcome: 'success',
    });
    const [generation] = testExporter.getSpansByType('model_generation' as any);
    expect(generation?.attributes).toMatchObject({
      providerAggregateMeasurementState: 'measured',
      providerInferenceCount: 2,
      providerUsageState: 'provider_not_reported',
      providerSucceededInferenceCount: 1,
      providerErrorInferenceCount: 1,
      providerAbortedInferenceCount: 0,
    });
    expectExactRequestAggregate(generation, inferences);
    expect(testExporter.getIncompleteSpans()).toHaveLength(0);
  }, 10000);

  it('DurableAgent fatal model error: root, generation, step AND inference all closed (no dangling)', async () => {
    const agent = new Agent({ id: 'a', name: 'a', instructions: 'x', model: errorModel() as any });
    const { wrapped } = buildMastra(testExporter, agent, 'durable');
    // the run fails; consuming the stream may reject — we only care that spans closed.
    try {
      await runToCompletion(wrapped, 'hi', testExporter);
    } catch {
      await new Promise(r => setTimeout(r, 50));
    }

    expect(testExporter.getSpansByType('agent_run' as any)).toHaveLength(1);
    const [inference] = testExporter.getSpansByType('model_inference' as any);
    expect(inference?.attributes).toMatchObject({
      measurementState: 'measured',
      providerUsageState: 'provider_not_reported',
      providerRequestBytes: expect.any(Number),
      providerOutcome: 'error',
    });
    const [generation] = testExporter.getSpansByType('model_generation' as any);
    expect(generation?.attributes).toMatchObject({
      providerAggregateMeasurementState: 'measured',
      providerInferenceCount: 1,
      providerUsageState: 'provider_not_reported',
      providerSucceededInferenceCount: 0,
      providerErrorInferenceCount: 1,
      providerAbortedInferenceCount: 0,
    });
    expectExactRequestAggregate(generation, [inference]);
    // Regression guard for the durable-specific gap: MODEL_STEP + MODEL_INFERENCE
    // must close on error (reportGenerationError closes its open children).
    expect(testExporter.getIncompleteSpans()).toHaveLength(0);
  });

  it('retains the dispatched durable request in the generation aggregate when aborted', async () => {
    const { model, doStream } = abortableModel();
    const agent = new Agent({ id: 'abort', name: 'abort', instructions: 'x', model: model as any, maxRetries: 0 });
    const { wrapped } = buildMastra(testExporter, agent, 'durable');
    let abortPayload: unknown;
    const result = await wrapped.stream('hi', {
      onAbort: (payload: unknown) => {
        abortPayload = payload;
      },
    });
    await waitForProviderCall(doStream);
    result.abort();
    try {
      await result.output.consumeStream();
    } catch {
      // The durable bridge rejects after delivering the abort callback.
    }
    await settle(testExporter);
    result.cleanup?.();

    expect(doStream).toHaveBeenCalledOnce();
    expect(abortPayload).toBeDefined();
    const inferences = testExporter.getSpansByType('model_inference' as any);
    expect(inferences).toHaveLength(1);
    expect(inferences[0]?.attributes).toMatchObject({
      measurementState: 'measured',
      providerUsageState: 'provider_not_reported',
      providerRequestBytes: expect.any(Number),
      providerOutcome: 'abort',
      finishReason: 'abort',
    });
    const [generation] = testExporter.getSpansByType('model_generation' as any);
    expect(generation?.attributes).toMatchObject({
      providerAggregateMeasurementState: 'measured',
      providerInferenceCount: 1,
      providerUsageState: 'provider_not_reported',
      providerSucceededInferenceCount: 0,
      providerErrorInferenceCount: 0,
      providerAbortedInferenceCount: 1,
    });
    expectExactRequestAggregate(generation, inferences);
    expect(testExporter.getIncompleteSpans().map(info => info.span?.type)).toEqual([]);
  });

  it('records provider request ledgers for both a durable supervisor and its sub-agent', async () => {
    const researchAgent = new Agent({
      id: 'research',
      name: 'research',
      description: 'Research sub-agent',
      instructions: 'Return one concise finding.',
      model: textModel('Sub-agent finding') as any,
    });
    const supervisor = new Agent({
      id: 'supervisor',
      name: 'supervisor',
      instructions: 'Delegate research.',
      model: delegatingModel('research', 'Find the evidence') as any,
      agents: { research: researchAgent },
    });
    const { wrapped } = buildMastra(testExporter, supervisor, 'durable');
    await runToCompletion(wrapped, 'Research this question', testExporter);

    const generations = testExporter.getSpansByType('model_generation' as any);
    expect(generations).toHaveLength(2);
    expect(
      generations.map(span => ({
        state: span.attributes?.providerAggregateMeasurementState,
        inferenceCount: span.attributes?.providerInferenceCount,
      })),
    ).toEqual(
      expect.arrayContaining([
        { state: 'measured', inferenceCount: 1 },
        { state: 'measured', inferenceCount: 2 },
      ]),
    );
    expect(testExporter.getIncompleteSpans()).toHaveLength(0);
  });

  it('EventedAgent (fire-and-forget) closes the full tree at completion', async () => {
    const agent = new Agent({
      id: 'a',
      name: 'a',
      instructions: 'use the tool',
      model: toolThenTextModel('get_weather', { city: 'Paris' }, 'It is 21C in Paris.') as any,
      tools: { get_weather: weatherTool },
    });
    const { wrapped } = buildMastra(testExporter, agent, 'evented');
    await runToCompletion(wrapped, 'weather in paris?', testExporter);

    expect(testExporter.getTraceIds()).toHaveLength(1);
    expect(testExporter.getSpansByType('agent_run' as any)).toHaveLength(1);
    expect(testExporter.getSpansByType('tool_call' as any)).toHaveLength(1);
    expect(testExporter.getIncompleteSpans()).toHaveLength(0);
  });
});
