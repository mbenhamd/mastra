import { stepCountIs } from '@internal/ai-sdk-v5';
import { it, expect, vi } from 'vitest';
import { z } from 'zod';
import { createTool } from '../../../../tools';
import { createSharedAgent, runLoopScenario, useLoopScenarioAimock, describeForAllEngines } from '../aimock-scenario';

/**
 * Tests tool-level background task opt-in.
 *
 * When a tool declares `background: { enabled: true }` and the agent opts it in
 * via `backgroundTasks: { tools: { toolName: true } }`, the tool runs asynchronously
 * and the agent stream emits background-task lifecycle chunks:
 *   - `background-task-started` when the task is dispatched
 *   - `background-task-completed` when it finishes with a result
 *
 * This pins the regression class where tool-level opt-in breaks and tools run
 * synchronously, blocking the agent loop.
 */
describeForAllEngines('background-task-tool-level scenario', engine => {
  const getMock = useLoopScenarioAimock();

  it('emits background-task-started/completed when tool opts in at tool level', async () => {
    // Track whether the tool executed
    let toolExecuted = false;

    const backgroundTool = createTool({
      id: 'background-work',
      description: 'Performs long-running work in the background',
      inputSchema: z.object({ duration: z.number() }),
      outputSchema: z.object({ result: z.string() }),
      background: { enabled: true, timeoutMs: 5000 },
      execute: async ({ duration }) => {
        toolExecuted = true;
        // Simulate some async work
        await new Promise(resolve => setTimeout(resolve, 10));
        return { result: `Completed ${duration}ms work` };
      },
    });

    const { chunks } = await runLoopScenario({
      engine,
      llm: getMock(),
      prompt: 'Run the background work with duration 100',
      tools: { 'background-work': backgroundTool },
      agentBackgroundTasks: { tools: { 'background-work': true } },
      stopWhen: stepCountIs(3),
      backgroundTasks: { enabled: true },
      collectChunks: true,
      fixtures: llm => {
        llm.on(
          { endpoint: 'chat', sequenceIndex: 0 },
          { toolCalls: [{ id: 'call_bg', name: 'background-work', arguments: { duration: 100 } }] },
        );
        llm.on({ endpoint: 'chat', sequenceIndex: 1 }, { content: 'Background task dispatched.' });
      },
    });

    // The tool executed (background tasks still execute, just asynchronously)
    expect(toolExecuted).toBe(true);

    // Verify background-task-started chunk was emitted
    const startedChunk = chunks?.find(c => c.type === 'background-task-started');
    expect(startedChunk).toBeDefined();
    expect(startedChunk?.payload).toMatchObject({
      toolName: 'background-work',
    });

    // The tool-result chunk should be emitted
    const toolResultChunk = chunks?.find(c => c.type === 'tool-result');
    expect(toolResultChunk).toBeDefined();
    expect(toolResultChunk?.payload?.toolName).toBe('background-work');
  });
});

describeForAllEngines(
  'background-task-tool-level suspend/resume scenario',
  engine => {
    const getMock = useLoopScenarioAimock();

    it('suspends an awaited task at a checkpoint and resumes it with the checkpoint data', async () => {
      let executions = 0;
      let resumedCheckpoint: string | undefined;
      const checkpointTool = createTool({
        id: 'background-checkpoint',
        description: 'Runs work that waits at a checkpoint for approval',
        inputSchema: z.object({ topic: z.string() }),
        outputSchema: z.object({ summary: z.string() }),
        background: { enabled: true },
        execute: async ({ topic }, options) => {
          executions++;
          const context = options as
            | {
                agent?: {
                  suspend?: (data?: unknown) => Promise<void>;
                  resumeData?: { checkpoint?: string };
                };
              }
            | undefined;
          if (!context?.agent?.resumeData) {
            await context?.agent?.suspend?.({ checkpoint: 'analyst-review', topic });
            return { summary: '' };
          }
          resumedCheckpoint = context.agent.resumeData.checkpoint;
          return { summary: `${topic}:${resumedCheckpoint}` };
        },
      });

      const shared = await createSharedAgent(getMock(), {
        tools: { 'background-checkpoint': checkpointTool },
        agentBackgroundTasks: { tools: { 'background-checkpoint': true } },
        backgroundTasks: { enabled: true },
        engine,
      });

      const { output, chunks, mastra } = await runLoopScenario({
        engine,
        llm: getMock(),
        sharedAgent: shared,
        prompt: 'Run the checkpoint work for papers',
        collectChunks: true,
        fixtures: llm => {
          llm.on(
            { endpoint: 'chat', sequenceIndex: 0 },
            {
              toolCalls: [
                {
                  id: 'call-checkpoint',
                  name: 'background-checkpoint',
                  arguments: { topic: 'papers', _background: { disposition: 'awaited' } },
                },
              ],
            },
          );
          llm.on(
            { endpoint: 'chat', sequenceIndex: 1 },
            {
              content: 'The checkpoint work is approved and complete.',
            },
          );
        },
      });

      const suspended = chunks?.find(c => c.type === 'tool-call-suspended');
      const started = chunks?.find(c => c.type === 'background-task-started');
      expect(suspended).toBeDefined();
      expect(started).toBeDefined();
      expect((suspended as any)?.payload?.suspendPayload).toMatchObject({ checkpoint: 'analyst-review' });
      expect(executions).toBe(1);
      expect(resumedCheckpoint).toBeUndefined();
      const backgroundManager = mastra?.backgroundTaskManager;
      const taskId = (started as any)?.payload?.taskId;
      await vi.waitFor(async () =>
        expect(await backgroundManager?.getTask(taskId)).toMatchObject({ status: 'suspended' }),
      );

      const resumed = await shared.agent.resumeStream({ checkpoint: 'approved' }, { runId: output.runId });
      for await (const _chunk of resumed.fullStream) {
        // Drain the resumed stream so the background task and reconciliation finish.
      }

      const toolResults = await resumed.toolResults;
      expect(executions).toBe(2);
      expect(resumedCheckpoint).toBe('approved');
      const result = toolResults?.find((chunk: any) => chunk.payload?.toolName === 'background-checkpoint');
      expect(result?.payload?.result).toEqual({ summary: 'papers:approved' });
    });
  },
  { skip: ['durable'] },
);
