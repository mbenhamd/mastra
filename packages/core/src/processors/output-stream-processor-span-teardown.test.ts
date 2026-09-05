import { describe, expect, it, vi } from 'vitest';
import { ProcessorRunner, ProcessorState } from './runner';
import type { Processor } from './index';

const mockLogger = {
  debug: () => {},
  info: () => {},
  warn: () => {},
  error: () => {},
  trackException: () => {},
} as any;

describe('output stream processor span teardown', () => {
  it('ends legacy and workflow processor spans when the stream closes without a finish chunk', async () => {
    const legacyEnd = vi.fn();
    const workflowEnd = vi.fn();
    const createChildSpan = vi.fn(() => ({
      end: legacyEnd,
      error: vi.fn(),
      createChildSpan: vi.fn(),
    }));
    const tracingContext = {
      currentSpan: {
        findParent: vi.fn(),
        createChildSpan,
      },
    } as any;
    const processor: Processor = {
      id: 'stream-processor',
      processOutputStream: async ({ part }) => part,
    };
    const runner = new ProcessorRunner({
      inputProcessors: [],
      outputProcessors: [processor],
      logger: mockLogger,
      agentName: 'test-agent',
    });
    const processorStates = new Map<string, ProcessorState>();

    await runner.processPart(
      {
        type: 'tool-result',
        payload: { toolCallId: 'call-1', toolName: 'search', result: { hits: 1 } },
      } as any,
      processorStates,
      { tracingContext },
    );
    await runner.processPart(
      { type: 'text-delta', payload: { text: 'hello world', id: 'text-1' } } as any,
      processorStates,
      { tracingContext },
    );

    const workflowState = new ProcessorState();
    workflowState.setWorkflowOutputStreamSpan('workflow', { end: workflowEnd } as any);
    processorStates.set('workflow', workflowState);

    runner.endStreamProcessorSpans(processorStates);

    expect(legacyEnd).toHaveBeenCalledTimes(1);
    expect(legacyEnd).toHaveBeenCalledWith({
      output: { totalChunks: 2, accumulatedText: 'hello world' },
    });
    expect(workflowEnd).toHaveBeenCalledTimes(1);
  });

  it('preserves processor-owned keys that resemble internal span storage', () => {
    const runner = new ProcessorRunner({
      inputProcessors: [],
      outputProcessors: [],
      logger: mockLogger,
      agentName: 'test-agent',
    });
    const state = new ProcessorState();
    state.customState.__outputStreamSpan_cache = 1;

    expect(() => runner.endStreamProcessorSpans(new Map([['processor', state]]))).not.toThrow();
    expect(state.customState.__outputStreamSpan_cache).toBe(1);
  });

  it('starts a fresh legacy span after teardown without discarding processor state', async () => {
    const spans = [
      { end: vi.fn(), error: vi.fn(), createChildSpan: vi.fn() },
      { end: vi.fn(), error: vi.fn(), createChildSpan: vi.fn() },
    ];
    const createChildSpan = vi.fn(() => spans.shift()!);
    const tracingContext = {
      currentSpan: {
        findParent: vi.fn(),
        createChildSpan,
      },
    } as any;
    const streamPartCounts: number[] = [];
    const processor: Processor = {
      id: 'stateful-stream-processor',
      processOutputStream: async ({ part, state, streamParts }) => {
        state.callCount = ((state.callCount as number | undefined) ?? 0) + 1;
        streamPartCounts.push(streamParts.length);
        return part;
      },
    };
    const runner = new ProcessorRunner({
      inputProcessors: [],
      outputProcessors: [processor],
      logger: mockLogger,
      agentName: 'test-agent',
    });
    const processorStates = new Map<string, ProcessorState>();

    await runner.processPart(
      {
        type: 'tool-result',
        payload: { toolCallId: 'call-1', toolName: 'search', result: { hits: 1 } },
      } as any,
      processorStates,
      { tracingContext },
    );
    runner.endStreamProcessorSpans(processorStates);

    await runner.processPart(
      {
        type: 'tool-result',
        payload: { toolCallId: 'call-2', toolName: 'search', result: { hits: 2 } },
      } as any,
      processorStates,
      { tracingContext },
    );
    runner.endStreamProcessorSpans(processorStates);

    expect(createChildSpan).toHaveBeenCalledTimes(2);
    for (const span of createChildSpan.mock.results.map(result => result.value)) {
      expect(span.end).toHaveBeenCalledTimes(1);
      expect(span.end).toHaveBeenCalledWith({ output: { totalChunks: 1, accumulatedText: '' } });
    }
    expect(processorStates.get(processor.id)?.customState.callCount).toBe(2);
    expect(streamPartCounts).toEqual([1, 2]);
  });
});
