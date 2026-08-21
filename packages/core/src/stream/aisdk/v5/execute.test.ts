import { convertArrayToReadableStream, MockLanguageModelV2 } from '@internal/ai-sdk-v5/test';
import { describe, expect, it, vi } from 'vitest';
import { z } from 'zod/v4';
import { coreFeatures } from '../../../features';
import {
  isExactJsonMeasurementCandidate,
  MAX_EXACT_JSON_MEASUREMENT_CODE_UNITS,
} from '../../../observability/content-free-measurement';
import { execute, resolveJsonPromptInjection } from './execute';
import { testUsage } from './test-utils';

const inputMessages = [{ role: 'user' as const, content: [{ type: 'text' as const, text: 'Summarize the plan.' }] }];
const schema = z.object({ suggestions: z.array(z.string()).min(1).max(3) });

async function readStream(stream: ReadableStream) {
  const reader = stream.getReader();
  try {
    while (true) {
      const { done } = await reader.read();
      if (done) break;
    }
  } finally {
    reader.releaseLock();
  }
}

describe('execute structured output prompt handling', () => {
  it('resolves automatic prompt injection from tri-state capability data', () => {
    expect(resolveJsonPromptInjection('auto', true)).toBeUndefined();
    expect(resolveJsonPromptInjection('auto', false)).toBe('inline');
    expect(resolveJsonPromptInjection('auto', undefined)).toBe('inline');
    expect(resolveJsonPromptInjection('system', undefined)).toBe('system');
  });

  it('advertises inline JSON prompt injection support', () => {
    expect(coreFeatures.has('json-prompt-injection:inline')).toBe(true);
  });

  it('injects direct structured output schema into the leading system message for boolean and system modes', async () => {
    const capturedPrompts: unknown[] = [];
    const model = new MockLanguageModelV2({
      doStream: async ({ prompt }: any) => {
        capturedPrompts.push(prompt);
        return {
          stream: convertArrayToReadableStream([
            { type: 'stream-start', warnings: [] },
            { type: 'response-metadata', id: 'id-system', modelId: 'mock-model-id', timestamp: new Date(0) },
            { type: 'text-start', id: 'text-1' },
            { type: 'text-delta', id: 'text-1', delta: '{"suggestions":["ship"]}' },
            { type: 'text-end', id: 'text-1' },
            { type: 'finish', finishReason: 'stop', usage: testUsage, providerMetadata: undefined },
          ]),
          request: { body: '' },
          response: { headers: {} },
          warnings: [] as any[],
        };
      },
    });

    for (const jsonPromptInjection of [true, 'system'] as const) {
      const stream = execute({
        runId: `test-run-id-${jsonPromptInjection}`,
        model: model as any,
        inputMessages,
        onResult: () => {},
        methodType: 'stream',
        structuredOutput: {
          schema,
          jsonPromptInjection,
        },
      });
      await readStream(stream);
    }

    expect(capturedPrompts).toHaveLength(2);
    for (const capturedPrompt of capturedPrompts) {
      expect((capturedPrompt as any[])[0].role).toBe('system');
      expect(JSON.stringify((capturedPrompt as any[])[0])).toContain('suggestions');
    }
  });

  it('injects direct structured output schema into the latest user message for inline mode', async () => {
    let capturedPrompt: unknown;
    let capturedResponseFormat: unknown;
    const model = new MockLanguageModelV2({
      doStream: async ({ prompt, responseFormat }: any) => {
        capturedPrompt = prompt;
        capturedResponseFormat = responseFormat;
        return {
          stream: convertArrayToReadableStream([
            { type: 'stream-start', warnings: [] },
            { type: 'response-metadata', id: 'id-inline', modelId: 'mock-model-id', timestamp: new Date(0) },
            { type: 'text-start', id: 'text-1' },
            { type: 'text-delta', id: 'text-1', delta: '{"suggestions":["ship"]}' },
            { type: 'text-end', id: 'text-1' },
            { type: 'finish', finishReason: 'stop', usage: testUsage, providerMetadata: undefined },
          ]),
          request: { body: '' },
          response: { headers: {} },
          warnings: [] as any[],
        };
      },
    });

    const messages = [
      { role: 'system' as const, content: 'Keep this prefix stable.' },
      { role: 'user' as const, content: [{ type: 'text' as const, text: 'First request.' }] },
      { role: 'assistant' as const, content: [{ type: 'text' as const, text: 'First response.' }] },
      { role: 'user' as const, content: [{ type: 'text' as const, text: 'Extract now.' }] },
    ];

    const stream = execute({
      runId: 'test-run-id-inline',
      model: model as any,
      inputMessages: messages,
      onResult: () => {},
      methodType: 'stream',
      structuredOutput: {
        schema,
        jsonPromptInjection: 'inline',
      },
    });

    await readStream(stream);

    expect(capturedResponseFormat).toBeUndefined();
    expect((capturedPrompt as any[])[0]).toEqual(messages[0]);
    expect(JSON.stringify((capturedPrompt as any[])[1])).not.toContain(
      'Return your response as JSON matching this schema',
    );
    expect(JSON.stringify((capturedPrompt as any[])[3])).toContain('Return your response as JSON matching this schema');
    expect(JSON.stringify((capturedPrompt as any[])[3])).toContain('suggestions');
    expect(JSON.stringify((capturedPrompt as any[])[3])).toContain('Extract now.');
  });

  it('adds a user message for inline mode when no user message exists', async () => {
    let capturedPrompt: unknown;
    const model = new MockLanguageModelV2({
      doStream: async ({ prompt }: any) => {
        capturedPrompt = prompt;
        return {
          stream: convertArrayToReadableStream([
            { type: 'stream-start', warnings: [] },
            { type: 'response-metadata', id: 'id-inline-no-user', modelId: 'mock-model-id', timestamp: new Date(0) },
            { type: 'text-start', id: 'text-1' },
            { type: 'text-delta', id: 'text-1', delta: '{"suggestions":["ship"]}' },
            { type: 'text-end', id: 'text-1' },
            { type: 'finish', finishReason: 'stop', usage: testUsage, providerMetadata: undefined },
          ]),
          request: { body: '' },
          response: { headers: {} },
          warnings: [] as any[],
        };
      },
    });

    const stream = execute({
      runId: 'test-run-id-inline-no-user',
      model: model as any,
      inputMessages: [{ role: 'system' as const, content: 'System only.' }],
      onResult: () => {},
      methodType: 'stream',
      structuredOutput: {
        schema,
        jsonPromptInjection: 'inline',
      },
    });

    await readStream(stream);

    expect((capturedPrompt as any[])[0]).toEqual({ role: 'system', content: 'System only.' });
    expect((capturedPrompt as any[])[1].role).toBe('user');
    expect(JSON.stringify((capturedPrompt as any[])[1])).toContain('Return your response as JSON matching this schema');
  });
  it('does not inject processor schema instructions into the main prompt when useAgent is enabled', async () => {
    let capturedPrompt: unknown;
    const model = new MockLanguageModelV2({
      doStream: async ({ prompt }: any) => {
        capturedPrompt = prompt;
        return {
          stream: convertArrayToReadableStream([
            { type: 'stream-start', warnings: [] },
            { type: 'response-metadata', id: 'id-0', modelId: 'mock-model-id', timestamp: new Date(0) },
            { type: 'text-start', id: 'text-1' },
            { type: 'text-delta', id: 'text-1', delta: 'Main agent summary.' },
            { type: 'text-end', id: 'text-1' },
            { type: 'finish', finishReason: 'stop', usage: testUsage, providerMetadata: undefined },
          ]),
          request: { body: '' },
          response: { headers: {} },
          warnings: [] as any[],
        };
      },
    });

    const stream = execute({
      runId: 'test-run-id',
      model: model as any,
      inputMessages,
      onResult: () => {},
      methodType: 'stream',
      structuredOutput: {
        schema,
        model: model as any,
        useAgent: true,
      },
    });

    await readStream(stream);

    expect(capturedPrompt).toEqual(inputMessages);
    expect(JSON.stringify(capturedPrompt)).not.toContain(
      'Your response will be processed by another agent to extract structured data',
    );
  });

  it('injects processor schema instructions into the main prompt when useAgent is disabled', async () => {
    let capturedPrompt: unknown;
    const model = new MockLanguageModelV2({
      doStream: async ({ prompt }: any) => {
        capturedPrompt = prompt;
        return {
          stream: convertArrayToReadableStream([
            { type: 'stream-start', warnings: [] },
            { type: 'response-metadata', id: 'id-0', modelId: 'mock-model-id', timestamp: new Date(0) },
            { type: 'text-start', id: 'text-1' },
            { type: 'text-delta', id: 'text-1', delta: 'Main agent summary.' },
            { type: 'text-end', id: 'text-1' },
            { type: 'finish', finishReason: 'stop', usage: testUsage, providerMetadata: undefined },
          ]),
          request: { body: '' },
          response: { headers: {} },
          warnings: [] as any[],
        };
      },
    });

    const stream = execute({
      runId: 'test-run-id',
      model: model as any,
      inputMessages,
      onResult: () => {},
      methodType: 'stream',
      structuredOutput: {
        schema,
        model: model as any,
      },
    });

    await readStream(stream);

    expect(capturedPrompt).not.toEqual(inputMessages);
    const promptJson = JSON.stringify(capturedPrompt);
    expect(promptJson).toContain('Your response will be processed by another agent to extract structured data');
    expect(promptJson).toContain('suggestions');
  });
});

describe('execute prepared provider request measurements', () => {
  const utf8JsonBytes = (value: unknown) => new TextEncoder().encode(JSON.stringify(value)).byteLength;

  it('fails binary JSON measurement closed before typed-array serialization can expand it', () => {
    expect(isExactJsonMeasurementCandidate(new Uint8Array([1, 2, 3]))).toBe(false);
    expect(isExactJsonMeasurementCandidate({ payload: Buffer.from('private bytes') })).toBe(false);
  });

  it('measures repeated JSON references but rejects cycles and serialization hooks', () => {
    const shared = { value: 'safe' };
    expect(isExactJsonMeasurementCandidate({ first: shared, second: shared })).toBe(true);
    const cyclic: { self?: unknown } = {};
    cyclic.self = cyclic;
    expect(isExactJsonMeasurementCandidate(cyclic)).toBe(false);
    expect(
      isExactJsonMeasurementCandidate({
        toJSON: () => ({ expanded: 'private' }),
      }),
    ).toBe(false);
  });

  it('reports exact content-free sizes for the final request passed to the provider adapter', async () => {
    let capturedRequest: Record<string, unknown> | undefined;
    let measurement: Record<string, unknown> | undefined;
    const model = new MockLanguageModelV2({
      doStream: async (request: any) => {
        capturedRequest = request;
        return {
          stream: convertArrayToReadableStream([
            { type: 'stream-start', warnings: [] },
            { type: 'response-metadata', id: 'id-sized', modelId: 'mock-model-id', timestamp: new Date(0) },
            { type: 'text-start', id: 'text-1' },
            { type: 'text-delta', id: 'text-1', delta: '{"suggestions":["ship"]}' },
            { type: 'text-end', id: 'text-1' },
            { type: 'finish', finishReason: 'stop', usage: testUsage, providerMetadata: undefined },
          ]),
          request: { body: '' },
          response: { headers: {} },
          warnings: [] as any[],
        };
      },
    });
    const messages = [
      { role: 'system' as const, content: 'SYSTEM_INSTRUCTION_CANARY' },
      { role: 'user' as const, content: [{ type: 'text' as const, text: 'PROMPT_SECRET_CANARY café' }] },
      { role: 'assistant' as const, content: [{ type: 'text' as const, text: 'MODEL_OUTPUT_CANARY' }] },
      {
        role: 'tool' as const,
        content: [
          {
            type: 'tool-result' as const,
            toolCallId: 'call-1',
            toolName: 'lookup',
            output: { type: 'text' as const, value: 'TOOL_RESULT_CANARY' },
          },
        ],
      },
    ];

    const stream = execute({
      runId: 'test-run-id-sized',
      model: model as any,
      inputMessages: messages,
      tools: {
        lookup: {
          description: 'TOOL_SCHEMA_CANARY',
          inputSchema: z.object({ query: z.string().describe('TOOL_SCHEMA_FIELD_CANARY') }),
          execute: async () => 'unused',
        },
      },
      toolChoice: 'auto',
      providerOptions: {
        azure: { reasoningEffort: 'low', privateDeployment: 'PROVIDER_OPTION_CANARY' },
      },
      modelSettings: { temperature: 0.2 },
      structuredOutput: { schema },
      onPreparedRequest: value => {
        measurement = value as unknown as Record<string, unknown>;
      },
      onResult: () => {},
      methodType: 'stream',
    });

    await readStream(stream);

    expect(capturedRequest).toBeDefined();
    expect(measurement).toMatchObject({
      measurementState: 'measured',
      providerBreakdownState: 'serialized_components_non_additive',
      providerMessageCount: 4,
      providerSystemMessageCount: 1,
      providerUserMessageCount: 1,
      providerAssistantMessageCount: 1,
      providerToolMessageCount: 1,
      providerOtherMessageCount: 0,
      providerToolCount: 1,
      providerResponseSchemaState: 'measured',
      providerReasoningEffortState: 'measured',
      providerReasoningEffort: 'low',
    });
    expect(measurement?.providerRequestBytes).toBe(utf8JsonBytes(capturedRequest));
    expect(measurement?.providerMessageBytes).toBe(utf8JsonBytes(capturedRequest?.prompt));
    expect(measurement?.providerToolSchemaBytes).toBe(utf8JsonBytes(capturedRequest?.tools));
    expect(measurement?.providerResponseSchemaBytes).toBe(
      utf8JsonBytes((capturedRequest?.responseFormat as { schema?: unknown } | undefined)?.schema),
    );
    expect(measurement?.providerSystemMessageBytes).toBe(utf8JsonBytes(messages[0]));
    expect(measurement?.providerUserMessageBytes).toBe(utf8JsonBytes(messages[1]));
    expect(measurement?.providerAssistantMessageBytes).toBe(utf8JsonBytes(messages[2]));
    expect(measurement?.providerToolMessageBytes).toBe(utf8JsonBytes(messages[3]));
    expect(measurement?.providerPreparationMs).toEqual(expect.any(Number));
    expect(measurement?.providerMeasurementMs).toEqual(expect.any(Number));
    expect(measurement?.providerDispatchTimestampMs).toEqual(expect.any(Number));
    expect(JSON.stringify(measurement)).not.toMatch(
      /PROMPT_SECRET_CANARY|SYSTEM_INSTRUCTION_CANARY|MODEL_OUTPUT_CANARY|TOOL_SCHEMA_CANARY|TOOL_RESULT_CANARY/u,
    );
    expect(JSON.stringify(measurement)).not.toContain('PROVIDER_OPTION_CANARY');
  });

  it('labels inline and absent response schemas without success-shaped zeroes', async () => {
    const measurements: Array<Record<string, unknown>> = [];
    const model = new MockLanguageModelV2({
      doStream: async () => ({
        stream: convertArrayToReadableStream([
          { type: 'stream-start', warnings: [] },
          { type: 'finish', finishReason: 'stop', usage: testUsage, providerMetadata: undefined },
        ]),
        request: { body: '' },
        response: { headers: {} },
        warnings: [] as any[],
      }),
    });

    for (const structuredOutput of [{ schema, jsonPromptInjection: 'inline' as const }, undefined]) {
      const stream = execute({
        runId: `test-run-id-schema-${measurements.length}`,
        model: model as any,
        inputMessages,
        structuredOutput,
        onPreparedRequest: value => measurements.push(value as unknown as Record<string, unknown>),
        onResult: () => {},
        methodType: 'stream',
      });
      await readStream(stream);
    }

    expect(measurements[0]).toMatchObject({
      providerResponseSchemaState: 'inline_in_prompt',
      providerReasoningEffortState: 'provider_default',
    });
    expect(measurements[0]?.providerResponseSchemaBytes).toEqual(expect.any(Number));
    expect(measurements[1]).toMatchObject({ providerResponseSchemaState: 'not_applicable' });
    expect(measurements[1]).not.toHaveProperty('providerResponseSchemaBytes');
  });

  it('retains descriptor-safe reasoning when the provider envelope is too large to measure', async () => {
    const oversizedText = 'x'.repeat(MAX_EXACT_JSON_MEASUREMENT_CODE_UNITS + 1);
    let capturedPrompt: unknown;
    let measurement: Record<string, unknown> | undefined;
    const model = new MockLanguageModelV2({
      doStream: async ({ prompt }: any) => {
        capturedPrompt = prompt;
        return {
          stream: convertArrayToReadableStream([
            { type: 'stream-start', warnings: [] },
            { type: 'finish', finishReason: 'stop', usage: testUsage, providerMetadata: undefined },
          ]),
          request: { body: '' },
          response: { headers: {} },
          warnings: [] as any[],
        };
      },
    });

    const stream = execute({
      runId: 'test-run-id-oversized-measurement',
      model: model as any,
      inputMessages: [{ role: 'user', content: [{ type: 'text', text: oversizedText }] }],
      providerOptions: { azure: { reasoningEffort: 'low' } },
      onPreparedRequest: value => {
        measurement = value as unknown as Record<string, unknown>;
      },
      onResult: () => {},
      methodType: 'stream',
    });
    await readStream(stream);

    expect(JSON.stringify(capturedPrompt)).toContain(oversizedText);
    expect(measurement).toMatchObject({
      measurementState: 'unknown',
      providerMessageCount: 1,
      providerUserMessageCount: 1,
      providerToolCount: 0,
      providerToolSchemaState: 'not_applicable',
      providerResponseSchemaState: 'not_applicable',
      providerReasoningEffortState: 'measured',
      providerReasoningEffort: 'low',
    });
    expect(measurement).not.toHaveProperty('providerMessageBytes');
    expect(measurement).not.toHaveProperty('providerRequestBytes');
    expect(JSON.stringify(measurement)).not.toContain(oversizedText);
  });

  it('does not invoke provider-option accessors while classifying reasoning telemetry', async () => {
    let reasoningAccessorReads = 0;
    const azureOptions = Object.defineProperty({}, 'reasoningEffort', {
      enumerable: true,
      get() {
        reasoningAccessorReads += 1;
        return 'low';
      },
    });
    let readsAtDispatch = -1;
    let measurement: Record<string, unknown> | undefined;
    const model = new MockLanguageModelV2({
      doStream: async () => {
        readsAtDispatch = reasoningAccessorReads;
        return {
          stream: convertArrayToReadableStream([
            { type: 'stream-start', warnings: [] },
            { type: 'finish', finishReason: 'stop', usage: testUsage, providerMetadata: undefined },
          ]),
          request: { body: '' },
          response: { headers: {} },
          warnings: [] as any[],
        };
      },
    });

    const stream = execute({
      runId: 'test-run-id-provider-accessor',
      model: model as any,
      inputMessages,
      providerOptions: { azure: azureOptions } as any,
      onPreparedRequest: value => {
        measurement = value as unknown as Record<string, unknown>;
      },
      onResult: () => {},
      methodType: 'stream',
    });
    await readStream(stream);

    expect(readsAtDispatch).toBeGreaterThanOrEqual(0);
    expect(reasoningAccessorReads).toBe(readsAtDispatch);
    expect(measurement).toMatchObject({
      measurementState: 'unknown',
      providerReasoningEffortState: 'unknown',
    });
  });

  it('dispatches before measuring or notifying and ignores observability callback failures', async () => {
    const order: string[] = [];
    const doStream = vi.fn(async () => {
      order.push('provider-dispatched');
      return {
        stream: convertArrayToReadableStream([
          { type: 'stream-start', warnings: [] },
          { type: 'finish', finishReason: 'stop', usage: testUsage, providerMetadata: undefined },
        ]),
        request: { body: '' },
        response: { headers: {} },
        warnings: [] as any[],
      };
    });
    const stream = execute({
      runId: 'test-run-id-callback-error',
      model: new MockLanguageModelV2({ doStream }) as any,
      inputMessages,
      onPreparedRequest: () => {
        order.push('observer-notified');
        throw new Error('observer failed');
      },
      onResult: () => {},
      methodType: 'stream',
    });

    await readStream(stream);
    expect(doStream).toHaveBeenCalledTimes(1);
    expect(order).toEqual(['provider-dispatched', 'observer-notified']);
  });

  it('measures every provider retry and anchors the successful attempt separately', async () => {
    const measurements: Array<Record<string, unknown>> = [];
    const providerRequests: Array<Record<string, unknown>> = [];
    const startedAttempts: number[] = [];
    const failedAttempts: number[] = [];
    let providerCalls = 0;
    const model = new MockLanguageModelV2({
      doStream: async providerRequest => {
        providerCalls += 1;
        const mutableProviderRequest = providerRequest as unknown as Record<string, unknown>;
        providerRequests.push(mutableProviderRequest);
        expect(mutableProviderRequest.adapterMutation).toBeUndefined();
        if (providerCalls === 1) {
          mutableProviderRequest.adapterMutation = 'failed-attempt-only';
          throw new Error('transient provider failure');
        }
        return {
          stream: convertArrayToReadableStream([
            { type: 'stream-start', warnings: [] },
            { type: 'finish', finishReason: 'stop', usage: testUsage, providerMetadata: undefined },
          ]),
          request: { body: '' },
          response: { headers: {} },
          warnings: [] as any[],
        };
      },
    });
    const stream = execute({
      runId: 'test-run-id-provider-retry',
      model: model as any,
      inputMessages,
      modelSettings: { maxRetries: 1 },
      onPreparedRequest: value => measurements.push(value as unknown as Record<string, unknown>),
      onProviderAttemptStart: providerAttempt => startedAttempts.push(providerAttempt),
      onProviderAttemptError: ({ providerAttempt }) => failedAttempts.push(providerAttempt),
      onResult: () => {},
      methodType: 'stream',
    });

    await readStream(stream);

    expect(providerCalls).toBe(2);
    expect(providerRequests[1]).not.toBe(providerRequests[0]);
    expect(startedAttempts).toEqual([1, 2]);
    expect(failedAttempts).toEqual([1]);
    expect(measurements).toHaveLength(2);
    expect(measurements.map(value => value.providerAttempt)).toEqual([1, 2]);
    const firstDispatchTimestampMs = Number(measurements[0]?.providerDispatchTimestampMs);
    const secondDispatchTimestampMs = Number(measurements[1]?.providerDispatchTimestampMs);
    const secondPreparationMs = Number(measurements[1]?.providerPreparationMs);
    const retryDispatchGapMs = secondDispatchTimestampMs - firstDispatchTimestampMs;
    expect(Number(measurements[0]?.providerPreparationMs)).toBeGreaterThanOrEqual(0);
    expect(secondPreparationMs).toBeGreaterThanOrEqual(0);
    expect(retryDispatchGapMs).toBeGreaterThanOrEqual(900);
    expect(secondPreparationMs).toBeLessThan(retryDispatchGapMs / 2);
  });
});
