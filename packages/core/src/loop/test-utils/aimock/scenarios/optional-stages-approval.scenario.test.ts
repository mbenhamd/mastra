import { describe, expect, it, vi } from 'vitest';
import { z } from 'zod/v4';
import { createScorer } from '../../../../evals/base';
import type { Mastra } from '../../../../mastra';
import { createTool } from '../../../../tools';
import * as executionWorkflowModule from '../../../workflows/agentic-execution';
import { createSharedAgent, useLoopScenarioAimock } from '../aimock-scenario';

/**
 * Optional execution stages must remain compatible with approval resumes.
 *
 * The first stream is built through the real factory with the historical
 * no-op tail options, producing an old eight-entry execution graph before
 * suspending for approval.
 * The resume supplies a scorer for the first time, so the scorer must run on
 * the resumed stream. Direct-engine graph compaction is asserted below;
 * evented execution keeps the historical graph for
 * its retained/current fingerprint comparison.
 */
describe.each(['normal', 'evented'] as const)('AIMock optional stages approval resume [%s]', engine => {
  const getMock = useLoopScenarioAimock();

  it('resumes an old eight-entry snapshot and supplies the scorer first on resume', async () => {
    const previousEvented = process.env.MASTRA_EVENTED_EXECUTION;
    process.env.MASTRA_EVENTED_EXECUTION = String(engine === 'evented');

    const llm = getMock();
    const executions: string[] = [];
    const approvalTool = createTool({
      id: 'approve-change',
      description: 'Apply a change after approval.',
      inputSchema: z.object({ value: z.string() }),
      outputSchema: z.object({ applied: z.string() }),
      requireApproval: true,
      execute: async ({ value }) => {
        executions.push(value);
        return { applied: value };
      },
    });
    let shared: Awaited<ReturnType<typeof createSharedAgent>> | undefined;
    const spies: Array<{ mockRestore: () => void }> = [];
    try {
      // The spy delegates to the production factory and only adds the two
      // historical no-op options while the initial graph is constructed. It
      // remains a delegating observer for resume construction; `initialPhase`
      // ensures the old options are never added to the resumed graph.
      const createdGraphs: number[] = [];
      let initialPhase = true;
      const createCurrent = executionWorkflowModule.createAgenticExecutionWorkflow;
      const workflowFactorySpy = vi.spyOn(executionWorkflowModule, 'createAgenticExecutionWorkflow');
      spies.push(workflowFactorySpy);
      workflowFactorySpy.mockImplementation(params => {
        const workflow = createCurrent(initialPhase ? { ...params, goal: {}, isTaskComplete: {} } : params);
        createdGraphs.push(workflow.serializedStepGraph.length);
        return workflow;
      });

      shared = await createSharedAgent(llm, { tools: { approvalTool }, engine });
      const mastra: Mastra = shared.mastra;
      const workflowsStore = (await mastra.getStorage()!.getStore('workflows'))!;
      const persist = workflowsStore.persistWorkflowSnapshot.bind(workflowsStore);
      const persistedGraphs: number[] = [];
      const persistSpy = vi.spyOn(workflowsStore, 'persistWorkflowSnapshot').mockImplementation(async state => {
        if (state.workflowName === 'executionWorkflow' && typeof state.snapshot !== 'string') {
          persistedGraphs.push(state.snapshot.serializedStepGraph.length);
        }
        return persist(state);
      });
      spies.push(persistSpy);

      llm.on(
        { endpoint: 'chat', hasToolResult: false },
        { toolCalls: [{ id: 'call-approval', name: 'approve-change', arguments: { value: 'approved' } }] },
      );
      llm.on(
        { endpoint: 'chat', toolCallId: 'call-approval', hasToolResult: true },
        { content: 'The change was applied.' },
      );

      const initial = await shared.agent.stream('Apply the change.', {
        requireToolApproval: true,
        maxSteps: 5,
      });
      let toolCallId: string | undefined;
      for await (const chunk of initial.fullStream) {
        if (chunk.type === 'tool-call-approval') toolCallId = chunk.payload.toolCallId;
      }
      expect(toolCallId).toBe('call-approval');

      const initialRuns = (await workflowsStore.listWorkflowRuns({})).runs;
      const initialRun = initialRuns.find(run => run.workflowName === 'executionWorkflow');
      expect(initialRun).toBeDefined();
      if (!initialRun || typeof initialRun.snapshot === 'string')
        throw new Error('missing structured execution snapshot');
      expect(initialRun.snapshot.status).toBe('suspended');
      expect(initialRun.snapshot.serializedStepGraph).toHaveLength(8);
      expect(createdGraphs).toContain(8);

      initialPhase = false;
      const scorerRun = vi.fn().mockReturnValue(1);
      const scorer = createScorer({
        id: 'resume-scorer',
        name: 'Resume scorer',
        description: 'Scores the resumed approval result.',
      })
        .generateScore(scorerRun)
        .generateReason(() => 'The approved change is complete.');
      const resumed = await shared.agent.approveToolCall({
        runId: initial.runId,
        toolCallId,
        isTaskComplete: { scorers: [scorer] },
      });
      for await (const _chunk of resumed.fullStream) {
        // Drain the real resumed stream through the execution engine.
      }

      expect(scorerRun).toHaveBeenCalledTimes(1);
      expect(await resumed.text).toContain('change was applied');
      expect(llm.getRequests()).toHaveLength(2);
      // The initial compatibility graph is eight entries. Supplying the
      // scorer only on resume keeps the direct graph at seven (the scorer is
      // now meaningful) while evented recovery retains all eight entries.
      expect(createdGraphs).toEqual(engine === 'evented' ? [8, 8] : [8, 7]);
      expect(persistedGraphs).toContain(8);

      // Repeat the old-snapshot resume without optional configuration. This
      // is the compact six-entry direct case; evented recovery remains eight.
      llm.clearFixtures();
      llm.clearRequests();
      llm.resetMatchCounts();
      initialPhase = true;
      llm.on(
        { endpoint: 'chat', hasToolResult: false },
        { toolCalls: [{ id: 'call-compact', name: 'approve-change', arguments: { value: 'compact' } }] },
      );
      llm.on(
        { endpoint: 'chat', toolCallId: 'call-compact', hasToolResult: true },
        { content: 'The compact change was applied.' },
      );
      const compactInitial = await shared.agent.stream('Apply the compact change.', {
        requireToolApproval: true,
        maxSteps: 5,
      });
      let compactToolCallId: string | undefined;
      for await (const chunk of compactInitial.fullStream) {
        if (chunk.type === 'tool-call-approval') compactToolCallId = chunk.payload.toolCallId;
      }
      expect(compactToolCallId).toBe('call-compact');
      const compactRuns = (await workflowsStore.listWorkflowRuns({})).runs;
      const compactRun = compactRuns.find(run => run.workflowName === 'executionWorkflow');
      expect(compactRun).toBeDefined();
      if (!compactRun || typeof compactRun.snapshot === 'string') throw new Error('missing compact snapshot');
      expect(compactRun.snapshot.serializedStepGraph).toHaveLength(8);
      initialPhase = false;
      const compactResumed = await shared.agent.approveToolCall({
        runId: compactInitial.runId,
        toolCallId: compactToolCallId,
      });
      for await (const _chunk of compactResumed.fullStream) {
        // Drain the real resumed stream through the execution engine.
      }
      expect(await compactResumed.text).toContain('compact change was applied');
      expect(llm.getRequests()).toHaveLength(2);
      expect(executions).toEqual(['approved', 'compact']);
      expect(createdGraphs).toEqual(engine === 'evented' ? [8, 8, 8, 8] : [8, 7, 8, 6]);
    } finally {
      for (const spy of spies.reverse()) spy.mockRestore();
      if (shared) await shared.mastra.shutdown();
      if (previousEvented === undefined) delete process.env.MASTRA_EVENTED_EXECUTION;
      else process.env.MASTRA_EVENTED_EXECUTION = previousEvented;
    }
  }, 30000);
});
