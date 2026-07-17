import { ReadableStream } from 'node:stream/web';
import { describe, expect, it } from 'vitest';
import { WorkflowRunOutput } from './RunOutput';
import { ChunkFrom } from './types';
import type { WorkflowStreamEvent } from './types';

function createWorkflowStream(chunks: WorkflowStreamEvent[]): ReadableStream<WorkflowStreamEvent> {
  return new ReadableStream<WorkflowStreamEvent>({
    start(controller) {
      for (const chunk of chunks) {
        controller.enqueue(chunk);
      }
      controller.close();
    },
  });
}

function createWorkflowStepOutput(usage: Record<string, unknown>): WorkflowStreamEvent {
  return {
    type: 'workflow-step-output',
    runId: 'run-1',
    from: ChunkFrom.WORKFLOW,
    payload: {
      id: 'step-output',
      stepId: 'step-1',
      output: {
        type: 'finish',
        payload: {
          usage,
        },
      },
    },
  } as WorkflowStreamEvent;
}

function createWorkflowFinish(
  status: Extract<WorkflowStreamEvent, { type: 'workflow-finish' }>['payload']['workflowStatus'],
  details: {
    result?: unknown;
    error?: unknown;
    usage?: { inputTokens: number; outputTokens: number; totalTokens: number };
  } = {},
): Extract<WorkflowStreamEvent, { type: 'workflow-finish' }> {
  const errorMessage =
    details.error instanceof Error
      ? details.error.message
      : typeof details.error === 'object' &&
          details.error !== null &&
          typeof (details.error as { message?: unknown }).message === 'string'
        ? (details.error as { message: string }).message
        : undefined;
  return {
    type: 'workflow-finish',
    runId: 'run-1',
    from: ChunkFrom.WORKFLOW,
    payload: {
      workflowStatus: status,
      output: {
        usage: {
          inputTokens: details.usage?.inputTokens ?? 0,
          outputTokens: details.usage?.outputTokens ?? 0,
          totalTokens: details.usage?.totalTokens ?? 0,
        },
      },
      metadata: details.error === undefined ? {} : { error: details.error, errorMessage },
      status,
      ...(details.result === undefined ? {} : { result: details.result }),
      ...(details.error === undefined ? {} : { error: details.error }),
    },
  };
}

async function collect(stream: ReadableStream<WorkflowStreamEvent>): Promise<WorkflowStreamEvent[]> {
  const events: WorkflowStreamEvent[] = [];
  for await (const event of stream) events.push(event);
  return events;
}

describe('WorkflowRunOutput', () => {
  it.each(['failed', 'canceled', 'tripwire'] as const)(
    'retains a rich %s terminal and suppresses every later event',
    async status => {
      const richTerminal = createWorkflowFinish(status, {
        error: { message: 'rich terminal failure' },
        result: { partial: true },
      });
      const output = new WorkflowRunOutput({
        runId: 'run-1',
        workflowId: 'workflow-1',
        stream: createWorkflowStream([
          richTerminal,
          createWorkflowStepOutput({ inputTokens: 999, outputTokens: 999 }),
          createWorkflowFinish('success'),
        ]),
      });

      const terminals = (await collect(output.fullStream)).filter(event => event.type === 'workflow-finish');
      const usage = await output.usage;

      expect(terminals).toHaveLength(1);
      expect(terminals[0]).toMatchObject(richTerminal);
      expect(terminals[0]?.payload.output.usage).toEqual(usage);
      expect(output.status).toBe(status);
      expect(usage.inputTokens).toBe(0);
      expect(usage.outputTokens).toBe(0);
    },
  );

  it('synthesizes one terminal event when the base stream closes without one', async () => {
    const output = new WorkflowRunOutput({
      runId: 'run-1',
      workflowId: 'workflow-1',
      stream: createWorkflowStream([]),
    });

    const terminals = (await collect(output.fullStream)).filter(event => event.type === 'workflow-finish');

    expect(terminals).toMatchObject([
      {
        type: 'workflow-finish',
        runId: 'run-1',
        payload: { workflowStatus: 'success' },
      },
    ]);
  });

  it('replaces a minimal transport finish marker with one canonical terminal', async () => {
    const minimalFinish = {
      type: 'workflow-finish',
      runId: 'run-1',
      from: ChunkFrom.WORKFLOW,
      payload: { runId: 'run-1' },
    } as unknown as WorkflowStreamEvent;
    const output = new WorkflowRunOutput({
      runId: 'run-1',
      workflowId: 'workflow-1',
      stream: createWorkflowStream([minimalFinish]),
    });

    const terminals = (await collect(output.fullStream)).filter(event => event.type === 'workflow-finish');

    expect(terminals).toHaveLength(1);
    expect(terminals[0]).not.toBe(minimalFinish);
    expect(terminals[0]).toMatchObject({
      type: 'workflow-finish',
      runId: 'run-1',
      payload: {
        workflowStatus: 'success',
        metadata: {},
        output: {
          usage: {
            inputTokens: 0,
            outputTokens: 0,
            totalTokens: 0,
          },
        },
      },
    });
  });

  it('uses nonzero terminal usage as the awaited usage fallback', async () => {
    const output = new WorkflowRunOutput({
      runId: 'run-1',
      workflowId: 'workflow-1',
      stream: createWorkflowStream([
        createWorkflowFinish('success', {
          usage: { inputTokens: 7, outputTokens: 3, totalTokens: 10 },
        }),
      ]),
    });

    const terminal = (await collect(output.fullStream)).find(event => event.type === 'workflow-finish');
    const usage = await output.usage;

    expect(terminal?.payload.output.usage).toEqual(usage);
    expect(usage).toMatchObject({ inputTokens: 7, outputTokens: 3, totalTokens: 10 });
  });

  it('settles result and usage once when the stream closes', async () => {
    const output = new WorkflowRunOutput({
      runId: 'run-1',
      workflowId: 'workflow-1',
      stream: createWorkflowStream([]),
    });
    const firstResult = { status: 'success', input: {}, steps: {}, result: { value: 'first' } };
    const secondResult = { status: 'success', input: {}, steps: {}, result: { value: 'second' } };

    output.updateResults(firstResult as never);
    output.updateResults(secondResult as never);
    const resultPromise = output.result;
    const usagePromise = output.usage;
    await collect(output.fullStream);

    await expect(resultPromise).resolves.toEqual(firstResult);
    expect(output.result).toBe(resultPromise);
    await expect(usagePromise).resolves.toMatchObject({ inputTokens: 0, outputTokens: 0, totalTokens: 0 });
    expect(output.usage).toBe(usagePromise);

    output.rejectResults(new Error('late rejection'));
    expect(output.status).toBe('success');
    await expect(output.result).resolves.toEqual(firstResult);
  });

  it('keeps an already resolved result authoritative when the underlying stream later errors', async () => {
    let failStream: ((error: Error) => void) | undefined;
    const stream = new ReadableStream<WorkflowStreamEvent>({
      start(controller) {
        failStream = error => controller.error(error);
      },
    });
    const output = new WorkflowRunOutput({
      runId: 'run-1',
      workflowId: 'workflow-1',
      stream,
    });
    const result = { status: 'success', input: {}, steps: {}, result: { value: 'authoritative' } };
    const eventsPromise = collect(output.fullStream);

    output.updateResults(result as never);
    failStream?.(new Error('late transport failure'));

    await expect(output.result).resolves.toEqual(result);
    const terminals = (await eventsPromise).filter(event => event.type === 'workflow-finish');
    expect(terminals).toMatchObject([
      {
        payload: {
          workflowStatus: 'success',
          metadata: {},
        },
      },
    ]);
    expect(output.status).toBe('success');
  });

  it('resets stream state on resume and preserves the resumed rich terminal', async () => {
    const firstTerminal = createWorkflowFinish('suspended', { result: { checkpoint: 'first' } });
    const output = new WorkflowRunOutput({
      runId: 'run-1',
      workflowId: 'workflow-1',
      stream: createWorkflowStream([
        createWorkflowStepOutput({ inputTokens: 17, outputTokens: 5, totalTokens: 22 }),
        firstTerminal,
      ]),
    });

    const firstTerminals = (await collect(output.fullStream)).filter(event => event.type === 'workflow-finish');
    expect(firstTerminals).toMatchObject([
      {
        payload: {
          workflowStatus: 'suspended',
          status: 'suspended',
          result: { checkpoint: 'first' },
          output: { usage: { inputTokens: 17, outputTokens: 5, totalTokens: 22 } },
        },
      },
    ]);
    const firstUsage = await output.usage;
    expect(firstUsage).toMatchObject({ inputTokens: 17, outputTokens: 5, totalTokens: 22 });
    expect(firstTerminals[0]?.payload.output.usage).toEqual(firstUsage);
    expect(output.status).toBe('suspended');

    const resumedTerminal = createWorkflowFinish('failed', {
      error: { message: 'resumed failure' },
      result: { partial: 'resumed' },
    });
    output.resume(
      createWorkflowStream([
        resumedTerminal,
        createWorkflowStepOutput({ inputTokens: 999, outputTokens: 999, totalTokens: 1998 }),
        firstTerminal,
      ]),
    );

    const resumedEvents = await collect(output.fullStream);
    const resumedUsage = await output.usage;
    const resumedTerminals = resumedEvents.filter(event => event.type === 'workflow-finish');

    expect(resumedEvents.filter(event => event.type === 'workflow-start')).toHaveLength(1);
    expect(resumedTerminals).toHaveLength(1);
    expect(resumedTerminals[0]).toMatchObject(resumedTerminal);
    expect(resumedTerminals[0]?.payload.output.usage).toEqual(resumedUsage);
    expect(resumedUsage).toMatchObject({ inputTokens: 0, outputTokens: 0, totalTokens: 0 });
    expect(output.status).toBe('failed');
  });

  it('synthesizes exactly one terminal for a resumed stream without a terminal', async () => {
    const output = new WorkflowRunOutput({
      runId: 'run-1',
      workflowId: 'workflow-1',
      stream: createWorkflowStream([createWorkflowFinish('suspended')]),
    });

    await collect(output.fullStream);
    output.resume(createWorkflowStream([]));

    const resumedEvents = await collect(output.fullStream);

    expect(resumedEvents.filter(event => event.type === 'workflow-start')).toHaveLength(1);
    expect(resumedEvents.filter(event => event.type === 'workflow-finish')).toMatchObject([
      {
        type: 'workflow-finish',
        runId: 'run-1',
        payload: { workflowStatus: 'success' },
      },
    ]);
  });

  it('should sum cacheCreationInputTokens across workflow step outputs', async () => {
    const output = new WorkflowRunOutput({
      runId: 'run-1',
      workflowId: 'workflow-1',
      stream: createWorkflowStream([
        createWorkflowStepOutput({
          inputTokens: 4557,
          outputTokens: 113,
          totalTokens: 4670,
          cachedInputTokens: 3584,
          cacheCreationInputTokens: 967,
        }),
        createWorkflowStepOutput({
          inputTokens: 4848,
          outputTokens: 117,
          totalTokens: 4965,
          cachedInputTokens: 4551,
          cacheCreationInputTokens: 296,
        }),
        createWorkflowStepOutput({
          inputTokens: 8557,
          outputTokens: 1270,
          totalTokens: 9827,
          cachedInputTokens: 4551,
          cacheCreationInputTokens: 4005,
        }),
      ]),
    });

    const usage = await output.usage;

    expect(usage.inputTokens).toBe(17962);
    expect(usage.outputTokens).toBe(1500);
    expect(usage.cachedInputTokens).toBe(12686);
    expect((usage as { cacheCreationInputTokens?: number }).cacheCreationInputTokens).toBe(5268);
  });
});
