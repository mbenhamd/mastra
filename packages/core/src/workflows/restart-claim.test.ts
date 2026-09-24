import { describe, expect, it, vi } from 'vitest';
import { z } from 'zod/v4';
import { Mastra } from '../mastra';
import { MockStore } from '../storage/mock';
import type { UpdateWorkflowStateOptions } from '../storage/types';
import { createWorkflow } from './create';
import { createStep } from './workflow';

const getOrCreateSpanMock = vi.fn();
vi.mock('../observability', async importOriginal => {
  const actual = await importOriginal<typeof import('../observability')>();
  return {
    ...actual,
    getOrCreateSpan: (...args: any[]) => getOrCreateSpanMock(...args) ?? (actual.getOrCreateSpan as any)(...args),
  };
});

/**
 * Restart and time travel mint a fresh lifecycle generation, so on stores that
 * fence the row's lifetime discriminator their ownership claim must name the
 * generation it succeeds. An unconditional generation overwrite is
 * indistinguishable from a delayed dead-lifetime write, and a claim that loses
 * must never enter the execution engine — every step persist would land as
 * `stale_execution` and surface as a synthetic cancellation.
 */
describe('restart lifecycle claim', () => {
  function createApprovalWorkflow() {
    let downstreamExecutions = 0;

    const approvalStep = createStep({
      id: 'approval',
      inputSchema: z.object({ item: z.string() }),
      outputSchema: z.object({ item: z.string(), approved: z.boolean() }),
      suspendSchema: z.object({ reason: z.string() }),
      resumeSchema: z.object({ approved: z.boolean() }),
      execute: async ({ inputData, resumeData, suspend }) => {
        if (!resumeData) {
          await suspend({ reason: `Needs approval: ${inputData.item}` });
          return { item: inputData.item, approved: false };
        }
        return { item: inputData.item, approved: (resumeData as { approved: boolean }).approved };
      },
    });

    const downstreamStep = createStep({
      id: 'downstream',
      inputSchema: z.object({ item: z.string(), approved: z.boolean() }),
      outputSchema: z.object({ executions: z.number() }),
      execute: async () => {
        downstreamExecutions++;
        return { executions: downstreamExecutions };
      },
    });

    const workflow = createWorkflow({
      id: 'restart-claim-wf',
      inputSchema: z.object({ item: z.string() }),
      outputSchema: z.object({ executions: z.number() }),
      steps: [approvalStep, downstreamStep],
      options: { validateInputs: false },
    })
      .then(approvalStep)
      .then(downstreamStep)
      .commit();

    return { workflow, getDownstreamExecutions: () => downstreamExecutions };
  }

  /**
   * Produces the stranded-run shape a recovery sweep restarts: a durably
   * `running` snapshot whose owning process is gone. The approval step is
   * marked active so the restart re-executes it and suspends again.
   */
  async function strandedRun() {
    const storage = new MockStore();
    const { workflow, getDownstreamExecutions } = createApprovalWorkflow();
    const mastra = new Mastra({
      storage,
      workflows: { 'restart-claim-wf': workflow },
      logger: false,
    });

    const run = await workflow.createRun();
    const started = await run.start({ inputData: { item: 'widget' } });
    expect(started.status).toBe('suspended');

    const workflowsStore = await storage.getStore('workflows');
    const suspended = await workflowsStore.loadWorkflowSnapshot({
      workflowName: 'restart-claim-wf',
      runId: run.runId,
    });
    expect(suspended?.executionGeneration).toEqual(expect.any(String));

    await workflowsStore.persistWorkflowSnapshot({
      workflowName: 'restart-claim-wf',
      runId: run.runId,
      snapshot: {
        ...suspended!,
        status: 'running',
        suspendedPaths: {},
        activePaths: [0],
        activeStepsPath: { approval: [0] },
      },
    });
    const snapshot = await workflowsStore.loadWorkflowSnapshot({
      workflowName: 'restart-claim-wf',
      runId: run.runId,
    });

    return { mastra, run, storage, workflowsStore, snapshot: snapshot!, getDownstreamExecutions };
  }

  it('names the claimed generation when adopting a stranded run', async () => {
    const { mastra, run, workflowsStore, snapshot } = await strandedRun();
    const updates: UpdateWorkflowStateOptions[] = [];
    const original = workflowsStore.updateWorkflowState.bind(workflowsStore);
    vi.spyOn(workflowsStore, 'updateWorkflowState').mockImplementation(async args => {
      updates.push(args.opts);
      return original(args);
    });

    const result = await run.restart();
    // The restarted run re-executes the approval step and suspends again.
    expect(result.status).toBe('suspended');

    const claim = updates.find(opts => typeof opts.executionGeneration === 'string');
    expect(claim).toBeDefined();
    expect(claim!.executionGeneration).not.toBe(snapshot.executionGeneration);
    expect(claim!.expectedExecutionGeneration).toBe(snapshot.executionGeneration);
    expect(claim!.expectedLifecycleResumeAttempt).toBe(snapshot.lifecycleResumeAttempt);
    expect(claim!.expectedStatus).toBe(snapshot.status);

    const adopted = await workflowsStore.loadWorkflowSnapshot({
      workflowName: 'restart-claim-wf',
      runId: run.runId,
    });
    expect(adopted?.executionGeneration).toBe(claim!.executionGeneration);
    await mastra.shutdown();
  });

  it('throws WORKFLOW_RESTART_NOT_CLAIMED and never executes when the claim loses', async () => {
    const { mastra, run, workflowsStore, snapshot, getDownstreamExecutions } = await strandedRun();
    const original = workflowsStore.updateWorkflowState.bind(workflowsStore);
    let competingAdopted = false;
    vi.spyOn(workflowsStore, 'updateWorkflowState').mockImplementation(async args => {
      // A competing lifetime adopts the row between this run's snapshot load
      // and its claim — the same shape a second recovery sweep produces. The
      // winner names the stored lifetime, matching the fenced-storage
      // succession contract rather than an unconditional overwrite.
      if (!competingAdopted && typeof args.opts.executionGeneration === 'string') {
        competingAdopted = true;
        await original({
          workflowName: args.workflowName,
          runId: args.runId,
          opts: {
            status: 'running',
            executionGeneration: 'competing-generation',
            expectedExecutionGeneration: snapshot.executionGeneration,
            expectedLifecycleResumeAttempt: snapshot.lifecycleResumeAttempt,
            expectedStatus: snapshot.status,
          },
        });
      }
      return original(args);
    });

    const rejected = await run.restart().then(
      () => undefined,
      (error: unknown) => error,
    );
    expect(rejected).toBeInstanceOf(Error);
    expect((rejected as { id?: string }).id).toBe('WORKFLOW_RESTART_NOT_CLAIMED');
    expect(getDownstreamExecutions()).toBe(0);

    const stored = await workflowsStore.loadWorkflowSnapshot({
      workflowName: 'restart-claim-wf',
      runId: run.runId,
    });
    expect(stored?.executionGeneration).toBe('competing-generation');
    expect(stored?.status).toBe('running');
    expect(snapshot.executionGeneration).not.toBe('competing-generation');
    await mastra.shutdown();
  });

  it('stands down when a second claimant supersedes the claim before adoption', async () => {
    const { mastra, run, workflowsStore, getDownstreamExecutions } = await strandedRun();
    const original = workflowsStore.updateWorkflowState.bind(workflowsStore);
    let superseded = false;
    vi.spyOn(workflowsStore, 'updateWorkflowState').mockImplementation(async args => {
      const updated = await original(args);
      // After this run's claim compare-and-set installs its generation, a
      // second claimant claims the row under its own generation before the
      // first caller re-reads it for adoption — the pre-adoption ownership
      // check must reject the superseded lineage rather than execute it.
      if (
        !superseded &&
        updated &&
        args.opts.status === 'running' &&
        typeof args.opts.executionGeneration === 'string' &&
        args.opts.expectedExecutionGeneration !== undefined
      ) {
        superseded = true;
        await original({
          workflowName: args.workflowName,
          runId: args.runId,
          opts: {
            status: 'running',
            executionGeneration: 'superseding-generation',
            expectedStatus: 'running',
            expectedExecutionGeneration: args.opts.executionGeneration,
            expectedLifecycleResumeAttempt: args.opts.lifecycleResumeAttempt ?? 0,
          },
        });
      }
      return updated;
    });

    const rejected = await run.restart().then(
      () => undefined,
      (error: unknown) => error,
    );
    expect(rejected).toBeInstanceOf(Error);
    expect((rejected as { id?: string }).id).toBe('WORKFLOW_RESTART_NOT_CLAIMED');
    expect(getDownstreamExecutions()).toBe(0);

    const stored = await workflowsStore.loadWorkflowSnapshot({
      workflowName: 'restart-claim-wf',
      runId: run.runId,
    });
    expect(stored?.executionGeneration).toBe('superseding-generation');
    expect(stored?.status).toBe('running');
    await mastra.shutdown();
  });

  it('claims unconditionally when the store cannot compare-and-set', async () => {
    const { mastra, run, workflowsStore } = await strandedRun();
    vi.spyOn(workflowsStore, 'supportsConcurrentUpdates').mockReturnValue(false);
    const updates: UpdateWorkflowStateOptions[] = [];
    const original = workflowsStore.updateWorkflowState.bind(workflowsStore);
    vi.spyOn(workflowsStore, 'updateWorkflowState').mockImplementation(async args => {
      updates.push(args.opts);
      return original(args);
    });

    const result = await run.restart();
    expect(result.status).toBe('suspended');

    const claim = updates.find(opts => typeof opts.executionGeneration === 'string');
    expect(claim).toBeDefined();
    expect(claim).not.toHaveProperty('expectedExecutionGeneration');
    expect(claim).not.toHaveProperty('expectedLifecycleResumeAttempt');
    expect(claim).not.toHaveProperty('expectedStatus');
    await mastra.shutdown();
  });

  /**
   * A step that suspends on its first execution and succeeds on re-execution,
   * so the winning restart/time travel ends `success` — distinguishable from
   * the `suspended` in-memory status the shared Run handle held before the race.
   */
  function createOnceSuspendedWorkflow() {
    let workExecutions = 0;

    const workStep = createStep({
      id: 'work',
      inputSchema: z.object({ item: z.string() }),
      outputSchema: z.object({ done: z.boolean() }),
      suspendSchema: z.object({ reason: z.string() }),
      execute: async ({ suspend }) => {
        workExecutions++;
        if (workExecutions === 1) {
          await suspend({ reason: 'pause once' });
          return { done: false };
        }
        return { done: true };
      },
    });

    const workflow = createWorkflow({
      id: 'restart-race-wf',
      inputSchema: z.object({ item: z.string() }),
      outputSchema: z.object({ done: z.boolean() }),
      steps: [workStep],
      options: { validateInputs: false },
    })
      .then(workStep)
      .commit();

    return { workflow, getWorkExecutions: () => workExecutions };
  }

  it('keeps the winning generation on the shared run when two restarts race', async () => {
    getOrCreateSpanMock.mockReset();
    const storage = new MockStore();
    const { workflow, getWorkExecutions } = createOnceSuspendedWorkflow();
    const mastra = new Mastra({
      storage,
      workflows: { 'restart-race-wf': workflow },
      logger: false,
    });

    const run = await workflow.createRun();
    const started = await run.start({ inputData: { item: 'widget' } });
    expect(started.status).toBe('suspended');
    expect(run.workflowRunStatus).toBe('suspended');

    const workflowsStore = await storage.getStore('workflows');
    const suspended = await workflowsStore.loadWorkflowSnapshot({
      workflowName: 'restart-race-wf',
      runId: run.runId,
    });
    // The stranded `running` shape a recovery sweep restarts: both callers read
    // the same generation before either compare-and-set lands.
    await workflowsStore.persistWorkflowSnapshot({
      workflowName: 'restart-race-wf',
      runId: run.runId,
      snapshot: {
        ...suspended!,
        status: 'running',
        suspendedPaths: {},
        activePaths: [0],
        activeStepsPath: { work: [0] },
      },
    });

    const losingSpanError = vi.fn();
    // The second workflow-run span belongs to the losing caller: the first
    // restart() reaches getOrCreateSpan first in this interleaving.
    getOrCreateSpanMock.mockReturnValueOnce(undefined).mockReturnValueOnce({
      id: 'losing-restart-span',
      externalTraceId: 'losing-restart-trace',
      error: losingSpanError,
    });

    const settled = await Promise.allSettled([run.restart(), run.restart()]);
    const fulfilled = settled.filter(
      (outcome): outcome is PromiseFulfilledResult<Awaited<ReturnType<typeof run.restart>>> =>
        outcome.status === 'fulfilled',
    );
    const rejected = settled.filter((outcome): outcome is PromiseRejectedResult => outcome.status === 'rejected');

    expect(fulfilled).toHaveLength(1);
    expect(rejected).toHaveLength(1);
    expect((rejected[0]!.reason as { id?: string }).id).toBe('WORKFLOW_RESTART_NOT_CLAIMED');
    expect(fulfilled[0]!.value.status).toBe('success');

    // The loser never entered the engine: one original suspend plus the
    // winner's re-execution, never a losing pass.
    expect(getWorkExecutions()).toBe(2);

    // The losing candidate must not overwrite the winning lineage on this
    // shared Run handle — the winner's terminal commit still lands on it.
    expect(run.workflowRunStatus).toBe('success');

    // The claim loss is traced on the losing workflow span, which ends with it.
    expect(losingSpanError).toHaveBeenCalledTimes(1);
    const spanError = losingSpanError.mock.calls[0]?.[0] as { error?: { id?: string }; endSpan?: boolean } | undefined;
    expect(spanError?.error?.id).toBe('WORKFLOW_RESTART_NOT_CLAIMED');
    expect(spanError?.endSpan).toBe(true);

    const stored = await workflowsStore.loadWorkflowSnapshot({
      workflowName: 'restart-race-wf',
      runId: run.runId,
    });
    expect(stored?.status).toBe('success');
    await mastra.shutdown();
  });

  it('keeps the winning generation on the shared run when two time travels race', async () => {
    const storage = new MockStore();
    const { workflow, getWorkExecutions } = createOnceSuspendedWorkflow();
    const mastra = new Mastra({
      storage,
      workflows: { 'restart-race-wf': workflow },
      logger: false,
    });

    const run = await workflow.createRun();
    const started = await run.start({ inputData: { item: 'widget' } });
    expect(started.status).toBe('suspended');
    expect(run.workflowRunStatus).toBe('suspended');

    const settled = await Promise.allSettled([run.timeTravel({ step: 'work' }), run.timeTravel({ step: 'work' })]);
    const fulfilled = settled.filter(
      (outcome): outcome is PromiseFulfilledResult<Awaited<ReturnType<typeof run.timeTravel>>> =>
        outcome.status === 'fulfilled',
    );
    const rejected = settled.filter((outcome): outcome is PromiseRejectedResult => outcome.status === 'rejected');

    expect(fulfilled).toHaveLength(1);
    expect(rejected).toHaveLength(1);
    expect((rejected[0]!.reason as { id?: string }).id).toBe('WORKFLOW_RESTART_NOT_CLAIMED');
    expect(fulfilled[0]!.value.status).toBe('success');
    expect(getWorkExecutions()).toBe(2);
    expect(run.workflowRunStatus).toBe('success');
    await mastra.shutdown();
  });

  /**
   * Produces the pre-upgrade stranded shape a recovery sweep can still
   * encounter: a durably `running` snapshot written before lifecycle
   * generations existed, so it carries no lineage fields at all.
   */
  async function strandedLegacyRun() {
    const storage = new MockStore();
    const { workflow, getWorkExecutions } = createOnceSuspendedWorkflow();
    const mastra = new Mastra({
      storage,
      workflows: { 'restart-race-wf': workflow },
      logger: false,
    });

    const run = await workflow.createRun();
    const started = await run.start({ inputData: { item: 'widget' } });
    expect(started.status).toBe('suspended');

    const workflowsStore = await storage.getStore('workflows');
    const suspended = await workflowsStore.loadWorkflowSnapshot({
      workflowName: 'restart-race-wf',
      runId: run.runId,
    });
    const legacy = {
      ...suspended!,
      status: 'running' as const,
      suspendedPaths: {},
      activePaths: [0],
      activeStepsPath: { work: [0] },
    };
    delete legacy.executionGeneration;
    delete legacy.lifecycleResumeAttempt;
    delete legacy.lifecycleStepStates;
    await workflowsStore.persistWorkflowSnapshot({
      workflowName: 'restart-race-wf',
      runId: run.runId,
      snapshot: legacy,
    });

    return { mastra, run, workflowsStore, getWorkExecutions };
  }

  it('lets only one claimant install a generation on a legacy running snapshot', async () => {
    const { mastra, run, workflowsStore, getWorkExecutions } = await strandedLegacyRun();
    const updates: UpdateWorkflowStateOptions[] = [];
    const original = workflowsStore.updateWorkflowState.bind(workflowsStore);
    vi.spyOn(workflowsStore, 'updateWorkflowState').mockImplementation(async args => {
      updates.push(args.opts);
      return original(args);
    });

    const settled = await Promise.allSettled([run.restart(), run.restart()]);
    const fulfilled = settled.filter(
      (outcome): outcome is PromiseFulfilledResult<Awaited<ReturnType<typeof run.restart>>> =>
        outcome.status === 'fulfilled',
    );
    const rejected = settled.filter((outcome): outcome is PromiseRejectedResult => outcome.status === 'rejected');

    expect(fulfilled).toHaveLength(1);
    expect(rejected).toHaveLength(1);
    expect((rejected[0]!.reason as { id?: string }).id).toBe('WORKFLOW_RESTART_NOT_CLAIMED');
    expect(fulfilled[0]!.value.status).toBe('success');
    // The loser never entered the engine: one original suspend plus the
    // winner's re-execution, never a losing pass.
    expect(getWorkExecutions()).toBe(2);

    // The claim still fenced the row: it required the stored generation to
    // stay absent rather than degrading to a status-only guard both racing
    // recovery workers would pass.
    const claim = updates.find(
      opts => opts.expectedStatus === 'running' && typeof opts.executionGeneration === 'string',
    );
    expect(claim).toBeDefined();
    expect(claim!.expectedExecutionGeneration).toBeNull();
    expect(claim!.expectedLifecycleResumeAttempt).toBe(0);

    const stored = await workflowsStore.loadWorkflowSnapshot({
      workflowName: 'restart-race-wf',
      runId: run.runId,
    });
    expect(stored?.status).toBe('success');
    await mastra.shutdown();
  });

  it('honors a cancellation admitted while the restart claim was in flight', async () => {
    const storage = new MockStore();
    const { workflow, getWorkExecutions } = createOnceSuspendedWorkflow();
    const mastra = new Mastra({
      storage,
      workflows: { 'restart-race-wf': workflow },
      logger: false,
    });

    const run = await workflow.createRun();
    const started = await run.start({ inputData: { item: 'widget' } });
    expect(started.status).toBe('suspended');

    const workflowsStore = await storage.getStore('workflows');
    const suspended = await workflowsStore.loadWorkflowSnapshot({
      workflowName: 'restart-race-wf',
      runId: run.runId,
    });
    await workflowsStore.persistWorkflowSnapshot({
      workflowName: 'restart-race-wf',
      runId: run.runId,
      snapshot: {
        ...suspended!,
        status: 'running',
        suspendedPaths: {},
        activePaths: [0],
        activeStepsPath: { work: [0] },
      },
    });

    // Land a durable cancellation inside the claim window: the claim has
    // installed the candidate generation but the shared Run handle has not yet
    // adopted it, so the cancel persists `canceled` while holding the old
    // in-memory controller and generation.
    const original = workflowsStore.updateWorkflowState.bind(workflowsStore);
    let cancelFired = false;
    vi.spyOn(workflowsStore, 'updateWorkflowState').mockImplementation(async args => {
      const updated = await original(args);
      if (
        !cancelFired &&
        updated &&
        args.opts.status === 'running' &&
        typeof args.opts.executionGeneration === 'string'
      ) {
        cancelFired = true;
        await run.cancel();
      }
      return updated;
    });

    const result = await run.restart();
    // Adoption must keep the admitted cancellation instead of resetting it
    // away: the engine resolves canceled without running a single step.
    expect(result.status).toBe('canceled');
    expect(getWorkExecutions()).toBe(1);
    expect(run.workflowRunStatus).toBe('canceled');

    const stored = await workflowsStore.loadWorkflowSnapshot({
      workflowName: 'restart-race-wf',
      runId: run.runId,
    });
    expect(stored?.status).toBe('canceled');
    await mastra.shutdown();
  });

  it('honors a cancellation that lands while the claimed lineage is being adopted', async () => {
    let workExecutions = 0;
    let cancelPromise: Promise<void> | undefined;

    const workStep = createStep({
      id: 'work',
      inputSchema: z.object({ item: z.string() }),
      outputSchema: z.object({ done: z.boolean() }),
      suspendSchema: z.object({ reason: z.string() }),
      execute: async ({ suspend }) => {
        workExecutions++;
        if (workExecutions === 1) {
          await suspend({ reason: 'pause once' });
          return { done: false };
        }
        // Hold the winning execution's only step until the mid-adoption
        // cancellation has fully landed, so the outcome cannot race it.
        await cancelPromise;
        return { done: true };
      },
    });
    const workflow = createWorkflow({
      id: 'restart-adopt-cancel-wf',
      inputSchema: z.object({ item: z.string() }),
      outputSchema: z.object({ done: z.boolean() }),
      steps: [workStep],
      options: { validateInputs: false },
    })
      .then(workStep)
      .commit();

    const storage = new MockStore();
    const mastra = new Mastra({
      storage,
      workflows: { 'restart-adopt-cancel-wf': workflow },
      logger: false,
    });

    const run = await workflow.createRun();
    const started = await run.start({ inputData: { item: 'widget' } });
    expect(started.status).toBe('suspended');

    const workflowsStore = await storage.getStore('workflows');
    const suspended = await workflowsStore.loadWorkflowSnapshot({
      workflowName: 'restart-adopt-cancel-wf',
      runId: run.runId,
    });
    await workflowsStore.persistWorkflowSnapshot({
      workflowName: 'restart-adopt-cancel-wf',
      runId: run.runId,
      snapshot: {
        ...suspended!,
        status: 'running',
        suspendedPaths: {},
        activePaths: [0],
        activeStepsPath: { work: [0] },
      },
    });

    // Fire the cancel at the adoption recheck: its durable write then lands
    // against a lineage this handle has already taken over.
    const originalExecutionState = workflowsStore.getWorkflowExecutionState.bind(workflowsStore);
    vi.spyOn(workflowsStore, 'getWorkflowExecutionState').mockImplementation(async args => {
      cancelPromise ??= run.cancel();
      return originalExecutionState(args);
    });

    const result = await run.restart();
    await cancelPromise;
    expect(result.status).toBe('canceled');
    expect(run.workflowRunStatus).toBe('canceled');

    const stored = await workflowsStore.loadWorkflowSnapshot({
      workflowName: 'restart-adopt-cancel-wf',
      runId: run.runId,
    });
    expect(stored?.status).toBe('canceled');
    await mastra.shutdown();
  });
});
