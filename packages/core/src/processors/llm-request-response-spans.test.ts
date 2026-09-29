import type { LanguageModelV2Prompt } from '@ai-sdk/provider-v5';
import { describe, expect, it, vi } from 'vitest';
import { EntityType } from '../observability';
import { ProcessorRunner } from './runner';
import { TripWire } from './index';
import type { Processor } from './index';

const mockLogger = {
  debug: () => {},
  info: () => {},
  warn: () => {},
  error: () => {},
  trackException: () => {},
} as any;

function makePrompt(): LanguageModelV2Prompt {
  return [{ role: 'user', content: [{ type: 'text', text: 'hello' }] }];
}

function makeTracing() {
  const end = vi.fn();
  const error = vi.fn();
  const createChildSpan = vi.fn(() => ({ end, error, createChildSpan: vi.fn() }));
  const tracingContext = { currentSpan: { createChildSpan } } as any;
  return { end, error, createChildSpan, tracingContext };
}

function makeRunner(processor: Processor) {
  return new ProcessorRunner({
    inputProcessors: [processor],
    outputProcessors: [],
    logger: mockLogger,
    agentName: 'test-agent',
  });
}

describe('runProcessLLMRequest span instrumentation', () => {
  it('creates and ends a PROCESSOR_RUN span around processLLMRequest', async () => {
    const { end, createChildSpan, tracingContext } = makeTracing();
    let receivedTracingContext: any;
    const processor: Processor = {
      id: 'prompt-pruner',
      name: 'Prompt Pruner',
      processLLMRequest: async args => {
        receivedTracingContext = (args as any).tracingContext;
        return { prompt: [...args.prompt] };
      },
    };

    const canary = 'LLM_REQUEST_PROMPT_CANARY';
    await makeRunner(processor).runProcessLLMRequest({
      prompt: [{ role: 'user', content: [{ type: 'text', text: canary }] }],
      model: {},
      stepNumber: 2,
      steps: [],
      retryCount: 1,
      tracingContext,
    });

    expect(createChildSpan).toHaveBeenCalledTimes(1);
    expect(createChildSpan).toHaveBeenCalledWith(
      expect.objectContaining({
        name: 'llm request processor: prompt-pruner',
        entityType: EntityType.INPUT_STEP_PROCESSOR,
        entityId: 'prompt-pruner',
        entityName: 'Prompt Pruner',
        attributes: expect.objectContaining({ processorIndex: 0 }),
      }),
    );
    expect(end).toHaveBeenCalledTimes(1);
    expect(end).toHaveBeenCalledWith(
      expect.objectContaining({
        attributes: expect.objectContaining({ processorMeasurementState: 'measured' }),
      }),
    );
    // The provider-bound prompt never reaches the span: no input/output payloads, no content.
    const [startOptions] = createChildSpan.mock.calls[0] as unknown as [Record<string, unknown>];
    const [endOptions] = end.mock.calls[0] as unknown as [Record<string, unknown>];
    expect(startOptions.input).toBeUndefined();
    expect(endOptions.output).toBeUndefined();
    expect(JSON.stringify([startOptions, endOptions])).not.toContain(canary);
    // Processor sees the processor span as its current span
    expect(receivedTracingContext?.currentSpan).toBe(createChildSpan.mock.results[0]!.value);
  });

  it('records tripwireAbort attributes when processLLMRequest aborts', async () => {
    const { end, error, tracingContext } = makeTracing();
    const processor: Processor = {
      id: 'guard',
      name: 'Guard',
      processLLMRequest: async ({ abort }) => abort('blocked', { retry: false }),
    };

    await expect(
      makeRunner(processor).runProcessLLMRequest({
        prompt: makePrompt(),
        model: {},
        stepNumber: 0,
        steps: [],
        tracingContext,
      }),
    ).rejects.toThrow(TripWire);

    expect(end).not.toHaveBeenCalled();
    expect(error).toHaveBeenCalledWith(
      expect.objectContaining({
        endSpan: true,
        attributes: expect.objectContaining({
          tripwireAbort: expect.objectContaining({ reason: 'blocked', retry: false }),
        }),
      }),
    );
  });

  it('errors the span on non-tripwire failure', async () => {
    const { error, tracingContext } = makeTracing();
    const processor: Processor = {
      id: 'boom',
      processLLMRequest: async () => {
        throw new Error('boom');
      },
    };

    await expect(
      makeRunner(processor).runProcessLLMRequest({
        prompt: makePrompt(),
        model: {},
        stepNumber: 0,
        steps: [],
        tracingContext,
      }),
    ).rejects.toThrow('boom');

    expect(error).toHaveBeenCalledWith(expect.objectContaining({ endSpan: true }));
  });

  it('runs without a tracing context', async () => {
    const processor: Processor = {
      id: 'no-trace',
      processLLMRequest: async ({ prompt }) => ({ prompt }),
    };

    const result = await makeRunner(processor).runProcessLLMRequest({
      prompt: makePrompt(),
      model: {},
      stepNumber: 0,
      steps: [],
    });

    expect(result.prompt).toHaveLength(1);
  });
});

describe('runProcessLLMResponse span instrumentation', () => {
  it('creates and ends a PROCESSOR_RUN span around processLLMResponse', async () => {
    const { end, createChildSpan, tracingContext } = makeTracing();
    const processor: Processor = {
      id: 'response-cache',
      name: 'Response Cache',
      processLLMResponse: async () => {},
    };

    await makeRunner(processor).runProcessLLMResponse({
      chunks: [{ type: 'text-delta' } as any],
      model: {},
      stepNumber: 3,
      steps: [],
      fromCache: true,
      retryCount: 0,
      tracingContext,
    });

    expect(createChildSpan).toHaveBeenCalledTimes(1);
    expect(createChildSpan).toHaveBeenCalledWith(
      expect.objectContaining({
        name: 'llm response processor: response-cache',
        entityId: 'response-cache',
        input: expect.objectContaining({ stepNumber: 3, fromCache: true, chunkCount: 1 }),
      }),
    );
    expect(end).toHaveBeenCalledTimes(1);
  });

  it('records tripwireAbort attributes when processLLMResponse aborts', async () => {
    const { end, error, tracingContext } = makeTracing();
    const processor: Processor = {
      id: 'resp-guard',
      processLLMResponse: async ({ abort }) => abort('bad response'),
    };

    await expect(
      makeRunner(processor).runProcessLLMResponse({
        chunks: [],
        model: {},
        stepNumber: 0,
        steps: [],
        fromCache: false,
        tracingContext,
      }),
    ).rejects.toThrow(TripWire);

    expect(end).not.toHaveBeenCalled();
    expect(error).toHaveBeenCalledWith(
      expect.objectContaining({
        endSpan: true,
        attributes: expect.objectContaining({
          tripwireAbort: expect.objectContaining({ reason: 'bad response' }),
        }),
      }),
    );
  });

  it('runs without a tracing context', async () => {
    const processLLMResponse = vi.fn(async () => {});
    const processor: Processor = { id: 'no-trace-resp', processLLMResponse };

    await makeRunner(processor).runProcessLLMResponse({
      chunks: [],
      model: {},
      stepNumber: 0,
      steps: [],
      fromCache: false,
    });

    expect(processLLMResponse).toHaveBeenCalledTimes(1);
  });
});
