import { describe, expect, it, vi } from 'vitest';
import { z } from 'zod/v4';
import { Mastra } from '../mastra';
import { toStandardSchema } from '../schema';
import { MockStore } from '../storage/mock';
import { createWorkflow } from './create';
import { createStep } from './workflow';

/**
 * Concurrent resume of a single suspension must run downstream steps exactly once.
 *
 * Two callers can both load the same `suspended` snapshot before either of them starts
 * executing, because the execution engine does not persist `running` until the resumed step
 * begins. Without an atomic claim both callers enter the engine and every downstream step —
 * and every external side effect it performs — runs twice.
 *
 * See https://github.com/mastra-ai/mastra/issues/20443.
 */
describe('concurrent resume', () => {
  /**
   * Builds a workflow that suspends on approval and counts downstream executions.
   *
   * The downstream step blocks on `release` so both resume calls are guaranteed to be
   * in-flight at the same time. This makes the race deterministic instead of timing-dependent:
   * without the fix the second caller enters the engine while the first is still parked.
   */
  function createApprovalWorkflow(options?: {
    shouldPersistSnapshot?: (args: {
      workflowStatus: string;
      stepResults: Record<string, { status?: string }>;
    }) => boolean;
    resumeSchema?: any;
    validateInputs?: boolean;
    downstreamGate?: Promise<void>;
    onDownstream?: () => void;
  }) {
    let downstreamExecutions = 0;
    let releaseDownstream!: () => void;
    const downstreamReleased = new Promise<void>(resolve => {
      releaseDownstream = resolve;
    });

    let downstreamStarted!: () => void;
    const downstreamHasStarted = new Promise<void>(resolve => {
      downstreamStarted = resolve;
    });

    const approvalStep = createStep({
      id: 'approval',
      inputSchema: z.object({ item: z.string() }),
      outputSchema: z.object({ item: z.string(), approved: z.boolean() }),
      suspendSchema: z.object({ reason: z.string() }),
      resumeSchema: options?.resumeSchema ?? z.object({ approved: z.boolean() }),
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
        options?.onDownstream?.();
        downstreamStarted();
        await (options?.downstreamGate ?? downstreamReleased);
        return { executions: downstreamExecutions };
      },
    });

    const workflow = createWorkflow({
      id: 'concurrent-resume-wf',
      inputSchema: z.object({ item: z.string() }),
      outputSchema: z.object({ executions: z.number() }),
      steps: [approvalStep, downstreamStep],
      options: {
        validateInputs: options?.validateInputs ?? false,
        ...(options?.shouldPersistSnapshot ? { shouldPersistSnapshot: options.shouldPersistSnapshot } : {}),
      },
    })
      .then(approvalStep)
      .then(downstreamStep)
      .commit();

    return {
      workflow,
      getDownstreamExecutions: () => downstreamExecutions,
      releaseDownstream,
      downstreamHasStarted,
    };
  }

  async function suspendRun(workflow: ReturnType<typeof createApprovalWorkflow>['workflow']) {
    const storage = new MockStore();
    const mastra = new Mastra({
      storage,
      workflows: { 'concurrent-resume-wf': workflow },
      logger: false,
    });

    const run = await workflow.createRun();
    const started = await run.start({ inputData: { item: 'widget' } });
    expect(started.status).toBe('suspended');

    return { mastra, run, storage };
  }

  it.each([true, false])(
    'runs downstream steps once for independent Run callers when running persistence is %s',
    async persistRunning => {
      let downstreamExecutions = 0;
      let releaseDownstream!: () => void;
      const downstreamGate = new Promise<void>(resolve => {
        releaseDownstream = resolve;
      });
      const make = () =>
        createApprovalWorkflow({
          shouldPersistSnapshot: persistRunning ? undefined : ({ workflowStatus }) => workflowStatus !== 'running',
          downstreamGate,
          onDownstream: () => downstreamExecutions++,
        });
      const first = make();
      const second = make();
      const storage = new MockStore();
      const mastraOne = new Mastra({ storage, workflows: { 'concurrent-resume-wf': first.workflow }, logger: false });
      const mastraTwo = new Mastra({ storage, workflows: { 'concurrent-resume-wf': second.workflow }, logger: false });
      const runOne = await first.workflow.createRun({ runId: 'independent-resume-run' });
      const started = await runOne.start({ inputData: { item: 'widget' } });
      expect(started.status).toBe('suspended');
      const runTwo = await second.workflow.createRun({ runId: 'independent-resume-run' });
      expect(runOne).not.toBe(runTwo);

      try {
        const inFlight = [
          runOne.resume({ step: 'approval', resumeData: { approved: true } }),
          runTwo.resume({ step: 'approval', resumeData: { approved: true } }),
        ];
        await Promise.race([first.downstreamHasStarted, second.downstreamHasStarted]);
        releaseDownstream();
        const results = await Promise.allSettled(inFlight);

        expect(downstreamExecutions).toBe(1);
        expect(results.filter(result => result.status === 'fulfilled')).toHaveLength(1);
        const rejected = results.find(result => result.status === 'rejected') as PromiseRejectedResult;
        expect(rejected.reason.id).toBe('WORKFLOW_RESUME_ALREADY_CLAIMED');
      } finally {
        releaseDownstream();
        await Promise.all([mastraOne.shutdown(), mastraTwo.shutdown()]);
      }
    },
  );

  it('does not let a delayed stale resume claim a later suspension at the same status', async () => {
    let validationCall = 0;
    let firstValidationStarted!: () => void;
    let secondValidationStarted!: () => void;
    let releaseFirstValidation!: () => void;
    let releaseSecondValidation!: () => void;
    const firstStarted = new Promise<void>(resolve => {
      firstValidationStarted = resolve;
    });
    const secondStarted = new Promise<void>(resolve => {
      secondValidationStarted = resolve;
    });
    const firstReleased = new Promise<void>(resolve => {
      releaseFirstValidation = resolve;
    });
    const secondReleased = new Promise<void>(resolve => {
      releaseSecondValidation = resolve;
    });
    const baseResumeSchema = toStandardSchema(z.object({ approved: z.boolean() }));
    const resumeSchema = {
      '~standard': {
        ...baseResumeSchema['~standard'],
        validate: async (value: unknown) => {
          const call = ++validationCall;
          if (call === 1) {
            firstValidationStarted();
            await firstReleased;
          } else if (call === 2) {
            secondValidationStarted();
            await secondReleased;
          }
          return baseResumeSchema['~standard'].validate(value);
        },
      },
    };
    const harness = createApprovalWorkflow({
      resumeSchema,
      validateInputs: true,
      shouldPersistSnapshot: ({ workflowStatus }) => workflowStatus !== 'running',
    });
    const { run, storage } = await suspendRun(harness.workflow);

    const winner = run.resume({ step: 'approval', resumeData: { approved: true } });
    await firstStarted;
    const loser = run.resume({ step: 'approval', resumeData: { approved: true } });
    await secondStarted;

    releaseFirstValidation();
    await harness.downstreamHasStarted;
    harness.releaseDownstream();
    expect((await winner).status).toBe('success');
    expect(run.workflowRunStatus).toBe('success');

    // Recreate the ABA coordinate produced when a winner reaches the next
    // suspension: status and execution generation match the stale snapshot,
    // but the resume attempt has advanced from 0 to 1.
    const workflowsStore = (await storage.getStore('workflows'))!;
    const resuspended = await workflowsStore.updateWorkflowState({
      workflowName: 'concurrent-resume-wf',
      runId: run.runId,
      opts: { status: 'suspended', lifecycleResumeAttempt: 1 },
    });
    expect(resuspended).toMatchObject({ status: 'suspended', lifecycleResumeAttempt: 1 });

    // The loser loaded attempt 0 before validation. A status-only claim would
    // now succeed against this later `suspended` state and repeat side effects.
    releaseSecondValidation();
    await expect(loser).rejects.toMatchObject({
      id: 'WORKFLOW_RESUME_ALREADY_CLAIMED',
      details: expect.objectContaining({
        expectedLifecycleResumeAttempt: 0,
        actualLifecycleResumeAttempt: 1,
      }),
    });
    expect(run.workflowRunStatus).toBe('success');
    expect(harness.getDownstreamExecutions()).toBe(1);
  });

  it('uses a per-run persistence override to claim resumes when the workflow default does not persist', async () => {
    const harness = createApprovalWorkflow({ shouldPersistSnapshot: () => false });
    const storage = new MockStore();
    new Mastra({
      storage,
      workflows: { 'concurrent-resume-wf': harness.workflow },
      logger: false,
    });

    const run = await harness.workflow.createRun({ shouldPersistSnapshot: () => true });
    const started = await run.start({ inputData: { item: 'widget' } });
    expect(started.status).toBe('suspended');

    const inFlight = [
      run.resume({ step: 'approval', resumeData: { approved: true } }),
      run.resume({ step: 'approval', resumeData: { approved: true } }),
    ];
    await harness.downstreamHasStarted;
    harness.releaseDownstream();
    const results = await Promise.allSettled(inFlight);

    expect(harness.getDownstreamExecutions()).toBe(1);
    expect(results.filter(result => result.status === 'fulfilled')).toHaveLength(1);
    const rejected = results.find(result => result.status === 'rejected') as PromiseRejectedResult;
    expect(rejected.reason.id).toBe('WORKFLOW_RESUME_ALREADY_CLAIMED');
  });

  it('claims a resume even when the per-run persistence override excludes running', async () => {
    const harness = createApprovalWorkflow();
    const storage = new MockStore();
    const workflowsStore = storage.stores.workflows as any;
    const updateSpy = vi.spyOn(workflowsStore, 'updateWorkflowState');
    new Mastra({
      storage,
      workflows: { 'concurrent-resume-wf': harness.workflow },
      logger: false,
    });

    const run = await harness.workflow.createRun({
      shouldPersistSnapshot: ({ workflowStatus }) => workflowStatus !== 'running',
    });
    const started = await run.start({ inputData: { item: 'widget' } });
    expect(started.status).toBe('suspended');

    const inFlight = [
      run.resume({ step: 'approval', resumeData: { approved: true } }),
      run.resume({ step: 'approval', resumeData: { approved: true } }),
    ];
    await harness.downstreamHasStarted;
    harness.releaseDownstream();

    const results = await Promise.allSettled(inFlight);
    expect(results.filter(result => result.status === 'fulfilled')).toHaveLength(1);
    expect(results.filter(result => result.status === 'rejected')).toHaveLength(1);
    expect(updateSpy).toHaveBeenCalled();
  });

  it('runs downstream steps once when two resumeStream() calls race', async () => {
    const harness = createApprovalWorkflow();
    const { run, storage } = await suspendRun(harness.workflow);

    // resumeStream returns its output handle synchronously and reports failures through the
    // stream, so the observable guarantee here is that downstream ran exactly once.
    run.resumeStream({ step: 'approval', resumeData: { approved: true } });
    run.resumeStream({ step: 'approval', resumeData: { approved: true } });

    await harness.downstreamHasStarted;
    harness.releaseDownstream();
    await new Promise(resolve => setTimeout(resolve, 50));

    expect(harness.getDownstreamExecutions()).toBe(1);
  });

  it('rejects a resume issued while an earlier resume is still executing', async () => {
    const harness = createApprovalWorkflow();
    const { run } = await suspendRun(harness.workflow);

    const first = run.resume({ step: 'approval', resumeData: { approved: true } });
    await harness.downstreamHasStarted;

    // The first resume owns the suspension and is mid-flight; a late caller must be rejected
    // rather than starting a second continuation.
    await expect(run.resume({ step: 'approval', resumeData: { approved: true } })).rejects.toThrow(
      /was not suspended|already resumed by another caller/,
    );

    harness.releaseDownstream();
    await first;

    expect(harness.getDownstreamExecutions()).toBe(1);
  });

  it('still resumes on stores that cannot claim, without calling updateWorkflowState', async () => {
    const harness = createApprovalWorkflow();

    // Cloudflare D1/KV/DO, ClickHouse and LanceDB report no concurrent-update support and throw
    // from `updateWorkflowState`. Claiming is best-effort, so those stores must keep resuming
    // exactly as they did before rather than having every resume fail.
    const storage = new MockStore();
    const workflowsStore = storage.stores.workflows as any;
    vi.spyOn(workflowsStore, 'supportsConcurrentUpdates').mockReturnValue(false);
    const updateSpy = vi.spyOn(workflowsStore, 'updateWorkflowState').mockImplementation(() => {
      throw new Error('updateWorkflowState is not implemented for Cloudflare D1 storage.');
    });

    const mastra = new Mastra({
      storage,
      workflows: { 'concurrent-resume-wf': harness.workflow },
      logger: false,
    });
    void mastra;

    const run = await harness.workflow.createRun();
    const started = await run.start({ inputData: { item: 'widget' } });
    expect(started.status).toBe('suspended');

    const resumed = run.resume({ step: 'approval', resumeData: { approved: true } });
    await harness.downstreamHasStarted;
    harness.releaseDownstream();

    expect((await resumed).status).toBe('success');
    expect(harness.getDownstreamExecutions()).toBe(1);
    expect(updateSpy).not.toHaveBeenCalled();
  });

  it('retains the claim when the engine fails before executing anything', async () => {
    const harness = createApprovalWorkflow();
    const { run, storage } = await suspendRun(harness.workflow);

    const executeSpy = vi
      .spyOn((run as any).executionEngine, 'execute')
      .mockRejectedValueOnce(new Error('engine boom'));

    await expect(run.resume({ step: 'approval', resumeData: { approved: true } })).rejects.toThrow('engine boom');
    executeSpy.mockRestore();

    await expect(run.resume({ step: 'approval', resumeData: { approved: true } })).rejects.toThrow(
      /was not suspended|already resumed by another caller/,
    );
    await expect(
      (await storage.getStore('workflows'))!.loadWorkflowSnapshot({
        workflowName: 'concurrent-resume-wf',
        runId: run.runId,
      }),
    ).resolves.toMatchObject({ status: 'running', lifecycleResumeAttempt: 1 });
  });

  it('keeps an ambiguous claim consumed when a real terminal write fails after a side effect', async () => {
    let sideEffects = 0;
    const harness = createApprovalWorkflow({
      shouldPersistSnapshot: ({ workflowStatus, stepResults }) =>
        workflowStatus !== 'running' || stepResults.approval?.status === 'suspended',
      onDownstream: () => sideEffects++,
    });
    const { run, storage } = await suspendRun(harness.workflow);
    const workflowsStore = (await storage.getStore('workflows'))!;
    const originalPersist = workflowsStore.persistWorkflowStepUpdate.bind(workflowsStore);
    const persistSpy = vi.spyOn(workflowsStore, 'persistWorkflowStepUpdate').mockImplementation(async args => {
      if (args.snapshot.status === 'success' && sideEffects === 1) {
        throw new Error('terminal persistence failure');
      }
      return originalPersist(args);
    });

    const resumed = run.resume({ step: 'approval', resumeData: { approved: true } });
    await harness.downstreamHasStarted;
    harness.releaseDownstream();
    await expect(resumed).rejects.toThrow('terminal persistence failure');
    expect(sideEffects).toBe(1);
    await expect(run.resume({ step: 'approval', resumeData: { approved: true } })).rejects.toThrow(
      /was not suspended|already resumed by another caller/,
    );
    await expect(
      workflowsStore.loadWorkflowSnapshot({ workflowName: 'concurrent-resume-wf', runId: run.runId }),
    ).resolves.toMatchObject({
      status: 'running',
      lifecycleResumeAttempt: 1,
    });
    persistSpy.mockRestore();
  });

  it('fails closed when a resume snapshot is missing', async () => {
    const harness = createApprovalWorkflow();
    const { run, storage } = await suspendRun(harness.workflow);
    (await storage.getStore('workflows'))!.dangerouslyClearAll();

    await expect(run.resume({ step: 'approval', resumeData: { approved: true } })).rejects.toThrow();
    expect(harness.getDownstreamExecutions()).toBe(0);
  });

  it.each([
    {
      label: 'a newer resume attempt',
      replace: (snapshot: any) => ({
        status: 'running' as const,
        lifecycleResumeAttempt: (snapshot.lifecycleResumeAttempt ?? 0) + 1,
      }),
      expected: { status: 'running', lifecycleResumeAttempt: 2 },
    },
    {
      label: 'a replacement execution generation',
      replace: () => ({ status: 'running' as const, executionGeneration: 'wfeg:replacement' }),
      expected: { status: 'running', executionGeneration: 'wfeg:replacement' },
    },
  ])('does not let failed-resume rollback overwrite $label', async ({ replace, expected }) => {
    const harness = createApprovalWorkflow();
    const { run, storage } = await suspendRun(harness.workflow);
    const workflowsStore = (await storage.getStore('workflows'))!;
    const executeSpy = vi.spyOn((run as any).executionEngine, 'execute').mockImplementationOnce(async () => {
      const claimed = await workflowsStore.loadWorkflowSnapshot({
        workflowName: 'concurrent-resume-wf',
        runId: run.runId,
      });
      expect(claimed).toMatchObject({ status: 'running', lifecycleResumeAttempt: 1 });
      await workflowsStore.updateWorkflowState({
        workflowName: 'concurrent-resume-wf',
        runId: run.runId,
        opts: replace(claimed),
      });
      throw new Error('engine boom after replacement');
    });

    await expect(run.resume({ step: 'approval', resumeData: { approved: true } })).rejects.toThrow(
      'engine boom after replacement',
    );
    executeSpy.mockRestore();

    await expect(
      workflowsStore.loadWorkflowSnapshot({ workflowName: 'concurrent-resume-wf', runId: run.runId }),
    ).resolves.toMatchObject(expected);
  });

  function fakeLogger() {
    return {
      debug: vi.fn(),
      info: vi.fn(),
      warn: vi.fn(),
      error: vi.fn(),
      trackException: vi.fn(),
    } as any;
  }

  function createUnclaimableWorkflow(options: { allowUnclaimedResumes?: boolean }) {
    const approvalStep = createStep({
      id: 'approval',
      inputSchema: z.object({ item: z.string() }),
      outputSchema: z.object({ approved: z.boolean() }),
      suspendSchema: z.object({ reason: z.string() }),
      resumeSchema: z.object({ approved: z.boolean() }),
      execute: async ({ inputData, resumeData, suspend }) => {
        if (!resumeData) {
          await suspend({ reason: `Needs approval: ${inputData.item}` });
          return { approved: false };
        }
        return { approved: resumeData.approved };
      },
    });

    const workflow = createWorkflow({
      id: 'unclaimable-resume-wf',
      inputSchema: z.object({ item: z.string() }),
      outputSchema: z.object({ approved: z.boolean() }),
      options: {
        validateInputs: false,
        // Same persistence shape as the internal agent loop: never persist
        // `running`, so the resume claim cannot be written.
        shouldPersistSnapshot: ({ workflowStatus }) => workflowStatus !== 'running',
        ...options,
      },
    })
      .then(approvalStep)
      .commit();
    return { workflow };
  }

  it('does not warn when a concurrent-capable store claims despite omitted running snapshots', async () => {
    const harness = createUnclaimableWorkflow({});
    const logger = fakeLogger();
    const storage = new MockStore();
    const updateSpy = vi.spyOn((await storage.getStore('workflows'))!, 'updateWorkflowState');
    new Mastra({
      storage,
      workflows: { 'unclaimable-resume-wf': harness.workflow },
      logger,
    });

    const run = await harness.workflow.createRun();
    const started = await run.start({ inputData: { item: 'widget' } });
    expect(started.status).toBe('suspended');

    const result = await run.resume({ step: 'approval', resumeData: { approved: true } });
    expect(result.status).toBe('success');

    expect(updateSpy).toHaveBeenCalled();
    expect(logger.warn).not.toHaveBeenCalledWith(expect.stringContaining('cannot be de-duplicated'));
  });

  it('still claims when allowUnclaimedResumes is true on a concurrent-capable store', async () => {
    const harness = createUnclaimableWorkflow({ allowUnclaimedResumes: true });
    const logger = fakeLogger();
    const storage = new MockStore();
    const updateSpy = vi.spyOn((await storage.getStore('workflows'))!, 'updateWorkflowState');
    new Mastra({
      storage,
      workflows: { 'unclaimable-resume-wf': harness.workflow },
      logger,
    });

    const run = await harness.workflow.createRun();
    const started = await run.start({ inputData: { item: 'widget' } });
    expect(started.status).toBe('suspended');

    const result = await run.resume({ step: 'approval', resumeData: { approved: true } });
    expect(result.status).toBe('success');

    const warnings = logger.warn.mock.calls.map((c: any[]) => String(c[0]));
    expect(warnings.filter((m: string) => m.includes('cannot be de-duplicated'))).toHaveLength(0);
    expect(updateSpy).toHaveBeenCalled();
  });

  it.each([true, false])(
    'only suppresses the unsupported-store warning when allowUnclaimedResumes is %s',
    async allowUnclaimedResumes => {
      const harness = createUnclaimableWorkflow({ allowUnclaimedResumes });
      const logger = fakeLogger();
      const storage = new MockStore();
      const workflowsStore = (await storage.getStore('workflows'))!;
      vi.spyOn(workflowsStore, 'supportsConcurrentUpdates').mockReturnValue(false);
      const updateSpy = vi.spyOn(workflowsStore, 'updateWorkflowState');
      new Mastra({
        storage,
        workflows: { 'unclaimable-resume-wf': harness.workflow },
        logger,
      });

      const run = await harness.workflow.createRun();
      const started = await run.start({ inputData: { item: 'widget' } });
      expect(started.status).toBe('suspended');
      await expect(run.resume({ step: 'approval', resumeData: { approved: true } })).resolves.toMatchObject({
        status: 'success',
      });
      expect(updateSpy).not.toHaveBeenCalled();
      const warnings = logger.warn.mock.calls.map((call: any[]) => String(call[0]));
      expect(warnings.some((message: string) => message.includes('cannot be de-duplicated'))).toBe(
        !allowUnclaimedResumes,
      );
    },
  );

  it('still resumes normally when there is no contention', async () => {
    const harness = createApprovalWorkflow();
    const { run } = await suspendRun(harness.workflow);

    harness.releaseDownstream();
    const result = await run.resume({ step: 'approval', resumeData: { approved: true } });

    expect(result.status).toBe('success');
    expect(harness.getDownstreamExecutions()).toBe(1);
  });
});
