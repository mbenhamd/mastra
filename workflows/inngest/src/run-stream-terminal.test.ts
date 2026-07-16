import { Mastra } from '@mastra/core/mastra';
import { MockStore } from '@mastra/core/storage';
import { ChunkFrom } from '@mastra/core/stream';
import type { StreamEvent, WorkflowStreamEvent } from '@mastra/core/workflows';
import { Inngest } from 'inngest';
import { describe, expect, it, vi } from 'vitest';
import { z } from 'zod/v4';

import { createTerminalStreamGate } from './run';
import type { InngestRun } from './run';
import { createInngestWorkflowTerminalPayload } from './workflow';
import { init } from './index';

const successfulResult = {
  status: 'success',
  input: {},
  steps: {},
  result: { value: 'done' },
};

async function createTestRun(runId: string, result: Record<string, unknown> = successfulResult) {
  const inngest = new Inngest({ id: 'stream-terminal-contract', isDev: true });
  const { createStep, createWorkflow } = init(inngest);
  const step = createStep({
    id: 'step',
    inputSchema: z.object({}),
    outputSchema: z.object({ value: z.string() }),
    execute: async () => ({ value: 'done' }),
  });
  const workflow = createWorkflow({
    id: 'stream-terminal-contract',
    inputSchema: z.object({}),
    outputSchema: z.object({ value: z.string() }),
    steps: [step],
  })
    .then(step)
    .commit();

  new Mastra({
    logger: false,
    storage: new MockStore(),
    workflows: { workflow },
  });

  const run = await workflow.createRun({ runId });
  vi.spyOn(run, '_start').mockResolvedValue(result as never);
  return run;
}

function createTerminalEvent(
  status: Extract<WorkflowStreamEvent, { type: 'workflow-finish' }>['payload']['workflowStatus'],
  details: { result?: unknown; error?: unknown } = {},
): Extract<WorkflowStreamEvent, { type: 'workflow-finish' }> {
  return {
    type: 'workflow-finish',
    runId: 'published-run',
    from: ChunkFrom.WORKFLOW,
    payload: createInngestWorkflowTerminalPayload({ status, ...details }),
  };
}

function emitWatchEvents(run: InngestRun, events: WorkflowStreamEvent[]): void {
  vi.spyOn(run, 'watch').mockImplementation(callback => {
    for (const event of events) {
      void callback(event);
    }
    return () => {};
  });
}

async function collect<T>(stream: ReadableStream<T>): Promise<T[]> {
  const events: T[] = [];
  for await (const event of stream) events.push(event);
  return events;
}

describe('Inngest stream terminal contract', () => {
  it('commits terminal ownership after the write and suppresses every queued later event', async () => {
    const writes: Array<{ type: string }> = [];
    let releaseTerminalWrite: (() => void) | undefined;
    const terminalWritePending = new Promise<void>(resolve => {
      releaseTerminalWrite = resolve;
    });
    const gate = createTerminalStreamGate<{ type: string }>(async event => {
      writes.push(event);
      if (event.type === 'finish') await terminalWritePending;
    });

    const terminalWrite = gate.writeWatchEvent({ type: 'finish' }, 'workflow-finish');
    const laterDataWrite = gate.writeWatchEvent({ type: 'step-output' }, 'workflow-step-output');
    const laterTerminalWrite = gate.writeWatchEvent({ type: 'finish' }, 'workflow-finish');

    await vi.waitFor(() => expect(writes).toEqual([{ type: 'finish' }]));
    releaseTerminalWrite?.();

    await expect(terminalWrite).resolves.toBe(true);
    await expect(laterDataWrite).resolves.toBe(false);
    await expect(laterTerminalWrite).resolves.toBe(false);
    expect(writes).toEqual([{ type: 'finish' }]);
  });

  it('releases a failed watched terminal claim so close can synthesize one terminal', async () => {
    const writes: Array<{ source: string }> = [];
    let failWatchedTerminal = true;
    const gate = createTerminalStreamGate<{ source: string }>(async event => {
      writes.push(event);
      if (failWatchedTerminal) {
        failWatchedTerminal = false;
        throw new Error('watched terminal write failed');
      }
    });

    await expect(gate.writeWatchEvent({ source: 'watch' }, 'workflow-finish')).rejects.toThrow(
      'watched terminal write failed',
    );
    await expect(gate.writeSyntheticFinish({ source: 'close' })).resolves.toBe(true);
    await expect(gate.writeWatchEvent({ source: 'late' }, 'workflow-step-output')).resolves.toBe(false);
    expect(writes).toEqual([{ source: 'watch' }, { source: 'close' }]);
  });

  it('surfaces a coherent close failure when the legacy consumer rejects terminal delivery', async () => {
    const run = await createTestRun('legacy-terminal-write-failure');
    vi.mocked((run as unknown as { _start: () => Promise<never> })._start).mockImplementation(
      () => new Promise<never>(() => {}),
    );
    emitWatchEvents(run, []);

    const { stream } = run.streamLegacy({ inputData: {} });
    const reader = stream.getReader();
    await expect(reader.read()).resolves.toMatchObject({ value: { type: 'start' } });
    await reader.cancel(new Error('consumer rejected stream'));

    await expect((run as unknown as { closeStreamAction: () => Promise<void> }).closeStreamAction()).rejects.toThrow(
      'Failed to finalize legacy workflow stream for run legacy-terminal-write-failure',
    );
  });

  it('retains the first watched vNext terminal and suppresses later finishes', async () => {
    const run = await createTestRun('vnext-watched-terminal');
    const terminalEvent = createTerminalEvent('success', { result: { value: 'rich watched result' } });
    emitWatchEvents(run, [terminalEvent, terminalEvent]);

    const output = run.stream({ inputData: {} });
    const events = await collect(output.fullStream);
    await output.result;

    expect(events.filter(event => event.type === 'workflow-finish')).toMatchObject([
      {
        type: 'workflow-finish',
        runId: 'vnext-watched-terminal',
        payload: {
          workflowStatus: 'success',
          output: { usage: { inputTokens: 0, outputTokens: 0, totalTokens: 0 } },
          metadata: {},
          status: 'success',
          result: { value: 'rich watched result' },
        },
      },
    ]);
  });

  it('preserves failed details in one canonical vNext terminal', async () => {
    const error = new Error('workflow failed');
    const failedResult = { status: 'failed', input: {}, steps: {}, error };
    const run = await createTestRun('vnext-failed-terminal', failedResult);
    const terminalEvent = JSON.parse(JSON.stringify(createTerminalEvent('failed', { error })));
    emitWatchEvents(run, [terminalEvent]);

    const output = run.stream({ inputData: {} });
    const events = await collect(output.fullStream);
    await expect(output.result).resolves.toEqual(failedResult);

    expect(events.filter(event => event.type === 'workflow-finish')).toMatchObject([
      {
        type: 'workflow-finish',
        runId: 'vnext-failed-terminal',
        payload: {
          workflowStatus: 'failed',
          output: { usage: { inputTokens: 0, outputTokens: 0, totalTokens: 0 } },
          metadata: {
            error: { name: 'Error', message: 'workflow failed', stack: expect.any(String) },
            errorMessage: 'workflow failed',
          },
          status: 'failed',
          error: { name: 'Error', message: 'workflow failed', stack: expect.any(String) },
        },
      },
    ]);
  });

  it('synthesizes one vNext terminal event when close observes no watched terminal', async () => {
    const run = await createTestRun('vnext-synthetic-terminal');
    emitWatchEvents(run, []);

    const output = run.stream({ inputData: {} });
    const events = await collect(output.fullStream);
    await output.result;

    expect(events.filter(event => event.type === 'workflow-finish')).toMatchObject([
      {
        type: 'workflow-finish',
        runId: 'vnext-synthetic-terminal',
        payload: { workflowStatus: 'success' },
      },
    ]);
  });

  it('keeps the watched legacy finish and does not append a synthetic duplicate', async () => {
    const run = await createTestRun('legacy-watched-terminal');
    const terminalEvent = createTerminalEvent('success', { result: { value: 'done' } });
    emitWatchEvents(run, [terminalEvent, terminalEvent]);

    const { stream, getWorkflowState } = run.streamLegacy({ inputData: {} });
    const eventsPromise = collect(stream);
    await getWorkflowState();
    const events = await eventsPromise;

    expect(events.filter(event => event.type === 'finish')).toMatchObject([
      {
        type: 'finish',
        payload: { status: 'success', result: { value: 'done' } },
      },
    ] satisfies Partial<StreamEvent>[]);
  });

  it('synthesizes one legacy finish when close observes no watched terminal', async () => {
    const run = await createTestRun('legacy-synthetic-terminal');
    emitWatchEvents(run, []);

    const { stream, getWorkflowState } = run.streamLegacy({ inputData: {} });
    const eventsPromise = collect(stream);
    await getWorkflowState();
    const events = await eventsPromise;

    expect(events.filter(event => event.type === 'finish')).toMatchObject([
      {
        type: 'finish',
        payload: { runId: 'legacy-synthetic-terminal' },
      },
    ] satisfies Partial<StreamEvent>[]);
  });
});
