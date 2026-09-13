/**
 * Tests for the persistence guard added to `persistStepUpdate` (issue #19056).
 *
 * The guard's job: never overwrite a `suspended` / `paused` snapshot with a
 * later `running` update from the same run in the same process. It relies on
 * `DefaultExecutionEngine.lastPersistedStatusByRun` as a process-local
 * memory of the previous write.
 */

import { beforeEach, describe, expect, it, vi } from 'vitest';
import { RequestContext } from '../../di';
import { serializeError } from '../../events/codec/error';
import { persistWorkflowStepUpdateRecord } from '../../storage/domains/workflows/resume';
import type {
  PersistWorkflowStepUpdateInput,
  PersistWorkflowStepUpdateResult,
  WorkflowResumeCapabilities,
} from '../../storage/types';
import { DefaultExecutionEngine } from '../default';
import type { ExecutionContext, WorkflowRunState, WorkflowRunStatus } from '../types';

type PersistArgs = PersistWorkflowStepUpdateInput;

interface FakeWorkflowsStore {
  persistWorkflowStepUpdate: (args: PersistArgs) => Promise<PersistWorkflowStepUpdateResult>;
  persistWorkflowSnapshot: (args: PersistArgs) => Promise<void>;
  loadWorkflowSnapshot: () => Promise<WorkflowRunState | null>;
  getWorkflowResumeCapabilities: () => WorkflowResumeCapabilities;
  calls: PersistArgs[];
  legacyCalls: PersistArgs[];
  snapshot: WorkflowRunState | null;
}

function makeFakeMastra(
  capabilities: WorkflowResumeCapabilities = {
    atomicResumeVersion: 1,
    fencedStepUpdateVersion: 1,
  },
) {
  const store: FakeWorkflowsStore = {
    calls: [],
    legacyCalls: [],
    snapshot: null,
    getWorkflowResumeCapabilities: () => capabilities,
    loadWorkflowSnapshot: vi.fn(async () => store.snapshot),
    persistWorkflowSnapshot: vi.fn(async args => {
      store.legacyCalls.push(args);
      store.snapshot = structuredClone(args.snapshot);
    }),
    persistWorkflowStepUpdate: vi.fn(async args => {
      const outcome = persistWorkflowStepUpdateRecord(store.snapshot ?? undefined, args, value =>
        structuredClone(value),
      );
      const { snapshot, ...result } = outcome;
      if (outcome.status === 'persisted' && snapshot) {
        store.calls.push(args);
        store.snapshot = snapshot;
      }
      return result;
    }) as any,
  };
  const mastra = {
    getStorage: () => ({
      getStore: async (_name: string) => store,
    }),
  } as any;
  return { mastra, store };
}

function makeEngine(
  shouldPersistSnapshot: (params: { workflowStatus: WorkflowRunStatus }) => boolean,
  capabilities?: WorkflowResumeCapabilities,
  pruneSnapshot?: (params: { snapshot: WorkflowRunState; workflowStatus: WorkflowRunStatus }) => WorkflowRunState,
) {
  const { mastra, store } = makeFakeMastra(capabilities);
  const engine = new DefaultExecutionEngine({
    mastra,
    options: {
      validateInputs: false,
      shouldPersistSnapshot: shouldPersistSnapshot as any,
      pruneSnapshot,
    },
  });
  return { engine, store };
}

function baseExecutionContext(overrides: Partial<ExecutionContext> = {}): ExecutionContext {
  return {
    workflowId: 'wf',
    runId: 'run-1',
    executionPath: [0],
    activeStepsPath: {},
    suspendedPaths: {},
    resumeLabels: {},
    retryConfig: { attempts: 0, delay: 0 },
    state: {},
    ...overrides,
  };
}

async function persist(
  engine: DefaultExecutionEngine,
  runId: string,
  workflowStatus: WorkflowRunStatus,
  serializedStepGraph: any[] = [],
) {
  await engine.persistStepUpdate({
    workflowId: 'wf',
    runId,
    resourceId: 'resource-1',
    stepResults: {},
    serializedStepGraph,
    executionContext: baseExecutionContext({ runId }),
    workflowStatus,
    requestContext: new RequestContext(),
  });
}

describe('persistStepUpdate — suspended overwrite guard', () => {
  let engine: DefaultExecutionEngine;
  let store: FakeWorkflowsStore;

  beforeEach(() => {
    // Opt into `running` persists — matches the durable-agent policy,
    // plus terminal statuses which are always persisted in production.
    ({ engine, store } = makeEngine(({ workflowStatus }) =>
      ['pending', 'paused', 'suspended', 'running', 'success', 'failed'].includes(workflowStatus),
    ));
  });

  it('persists a running snapshot when the last persisted status was pending', async () => {
    await persist(engine, 'run-1', 'pending');
    await persist(engine, 'run-1', 'running');

    expect(store.calls).toHaveLength(2);
    expect(store.calls[0]!.snapshot.status).toBe('pending');
    expect(store.calls[1]!.snapshot.status).toBe('running');
    expect(engine.getLastPersistedStatus('run-1')).toBe('running');
  });

  it('persists successive running snapshots', async () => {
    await persist(engine, 'run-1', 'pending');
    await persist(engine, 'run-1', 'running');
    await persist(engine, 'run-1', 'running');

    expect(store.calls.map(c => c.snapshot.status)).toEqual(['pending', 'running', 'running']);
  });

  it('persists a suspended snapshot after running (normal suspend)', async () => {
    await persist(engine, 'run-1', 'pending');
    await persist(engine, 'run-1', 'running');
    await persist(engine, 'run-1', 'suspended');

    expect(store.calls.map(c => c.snapshot.status)).toEqual(['pending', 'running', 'suspended']);
    expect(engine.getLastPersistedStatus('run-1')).toBe('suspended');
  });

  it('SKIPS a running snapshot when the last persisted status was suspended', async () => {
    await persist(engine, 'run-1', 'pending');
    await persist(engine, 'run-1', 'running');
    await persist(engine, 'run-1', 'suspended');

    // Simulate a resume: engine ticks running mid-resume — must not clobber the suspended row.
    await persist(engine, 'run-1', 'running');

    expect(store.calls.map(c => c.snapshot.status)).toEqual(['pending', 'running', 'suspended']);
    expect(engine.getLastPersistedStatus('run-1')).toBe('suspended');
  });

  it('SKIPS a running snapshot when the last persisted status was paused', async () => {
    await persist(engine, 'run-1', 'pending');
    await persist(engine, 'run-1', 'paused');
    await persist(engine, 'run-1', 'running');

    expect(store.calls.map(c => c.snapshot.status)).toEqual(['pending', 'paused']);
    expect(engine.getLastPersistedStatus('run-1')).toBe('paused');
  });

  it('allows a suspended → suspended re-suspend write', async () => {
    await persist(engine, 'run-1', 'pending');
    await persist(engine, 'run-1', 'suspended');
    await persist(engine, 'run-1', 'suspended');

    expect(store.calls.map(c => c.snapshot.status)).toEqual(['pending', 'suspended', 'suspended']);
  });

  it('allows terminal statuses to be persisted even after suspended', async () => {
    await persist(engine, 'run-1', 'pending');
    await persist(engine, 'run-1', 'suspended');
    await persist(engine, 'run-1', 'success');

    expect(store.calls.map(c => c.snapshot.status)).toEqual(['pending', 'suspended', 'success']);
  });

  it('tracks runs independently by runId', async () => {
    await persist(engine, 'run-A', 'pending');
    await persist(engine, 'run-A', 'suspended');
    // Different run — must not be blocked by run-A's suspended entry.
    await persist(engine, 'run-B', 'pending');
    await persist(engine, 'run-B', 'running');

    expect(store.calls.map(c => `${c.runId}:${c.snapshot.status}`)).toEqual([
      'run-A:pending',
      'run-A:suspended',
      'run-B:pending',
      'run-B:running',
    ]);
    expect(engine.getLastPersistedStatus('run-A')).toBe('suspended');
    expect(engine.getLastPersistedStatus('run-B')).toBe('running');
  });

  it('respects shouldPersistSnapshot returning false regardless of cache', async () => {
    // Legacy policy that refuses to persist running at all.
    ({ engine, store } = makeEngine(({ workflowStatus }) =>
      ['pending', 'paused', 'suspended'].includes(workflowStatus),
    ));

    await persist(engine, 'run-1', 'pending');
    await persist(engine, 'run-1', 'running');

    expect(store.calls.map(c => c.snapshot.status)).toEqual(['pending']);
    // No running snapshot means the tracker was never updated past pending.
    expect(engine.getLastPersistedStatus('run-1')).toBe('pending');
  });

  it('skips a transient execution even when the durable fallback policy persists', async () => {
    ({ engine, store } = makeEngine(() => true));

    await engine.persistStepUpdate({
      workflowId: 'wf',
      runId: 'transient-run',
      resourceId: 'resource-1',
      stepResults: {},
      serializedStepGraph: [],
      executionContext: baseExecutionContext({ runId: 'transient-run', transientExecution: true }),
      workflowStatus: 'running',
      requestContext: new RequestContext(),
    });

    expect(store.calls).toHaveLength(0);
    expect(store.legacyCalls).toHaveLength(0);
    expect(engine.getLastPersistedStatus('transient-run')).toBeUndefined();
  });

  it('rejects an unhashed ordinary writer while a storage-admitted checkpoint is active', async () => {
    const checkpoint = { version: 1, marker: 'storage-owned' } as never;
    store.snapshot = {
      runId: 'run-1',
      resourceId: 'resource-from-storage',
      status: 'running',
      value: { before: true },
      context: {},
      activePaths: [],
      activeStepsPath: {},
      suspendedPaths: {},
      resumeLabels: {},
      waitingPaths: {},
      serializedStepGraph: [],
      timestamp: 1,
      executionGeneration: 'wfeg:generation',
      lifecycleResumeAttempt: 1,
      lifecycleStepStates: {},
      resumeCheckpoint: checkpoint,
      runOptions: { disableScorers: true },
    } as WorkflowRunState;

    await engine.persistStepUpdate({
      workflowId: 'wf',
      runId: 'run-1',
      resourceId: undefined,
      stepResults: {},
      serializedStepGraph: [],
      executionContext: baseExecutionContext({
        executionGeneration: 'wfeg:generation',
        lifecycleResumeAttempt: 1,
        lifecycleStepStates: { step: { stepCallId: 'wfsc:step', stepAttempt: 1 } },
        state: { after: true },
      }),
      workflowStatus: 'running',
      requestContext: new RequestContext(),
    });

    expect(store.calls).toHaveLength(0);
    expect(store.snapshot).toMatchObject({
      resourceId: 'resource-from-storage',
      runOptions: { disableScorers: true },
      resumeCheckpoint: checkpoint,
      value: { before: true },
    });
  });

  it('persists the first completed result for an ordinary next resume attempt', async () => {
    store.snapshot = {
      runId: 'run-1',
      resourceId: 'resource-1',
      status: 'suspended',
      value: {},
      context: {},
      activePaths: [],
      activeStepsPath: {},
      suspendedPaths: { approval: [0] },
      resumeLabels: {},
      waitingPaths: {},
      serializedStepGraph: [],
      timestamp: 1,
      executionGeneration: 'wfeg:generation',
      lifecycleResumeAttempt: 0,
      lifecycleStepStates: {},
    } as WorkflowRunState;
    const resumedExecutionContext = baseExecutionContext({
      executionGeneration: 'wfeg:generation',
      lifecycleResumeAttempt: 1,
      lifecycleStepStates: { approval: { stepCallId: 'wfsc:approval', stepAttempt: 2 } },
    });

    await engine.persistStepUpdate({
      workflowId: 'wf',
      runId: 'run-1',
      resourceId: 'resource-1',
      stepResults: {},
      serializedStepGraph: [],
      executionContext: resumedExecutionContext,
      workflowStatus: 'running',
      requestContext: new RequestContext(),
    });

    expect(store.calls).toHaveLength(0);
    expect(store.snapshot).toMatchObject({
      status: 'suspended',
      lifecycleResumeAttempt: 0,
    });

    await engine.persistStepUpdate({
      workflowId: 'wf',
      runId: 'run-1',
      resourceId: 'resource-1',
      stepResults: {},
      serializedStepGraph: [],
      executionContext: resumedExecutionContext,
      workflowStatus: 'waiting',
      requestContext: new RequestContext(),
    });

    expect(store.calls).toHaveLength(0);
    expect(store.snapshot).toMatchObject({
      status: 'suspended',
      lifecycleResumeAttempt: 0,
    });

    await engine.persistStepUpdate({
      workflowId: 'wf',
      runId: 'run-1',
      resourceId: 'resource-1',
      stepResults: {},
      serializedStepGraph: [],
      executionContext: resumedExecutionContext,
      workflowStatus: 'suspended',
      requestContext: new RequestContext(),
    });

    expect(store.calls).toHaveLength(1);
    expect(store.calls[0]).toMatchObject({
      expectedExecutionGeneration: 'wfeg:generation',
      expectedLifecycleResumeAttempt: 1,
      expectedResumeOperationHash: undefined,
    });
    expect(store.snapshot).toMatchObject({
      status: 'suspended',
      executionGeneration: 'wfeg:generation',
      lifecycleResumeAttempt: 1,
      lifecycleStepStates: { approval: { stepCallId: 'wfsc:approval', stepAttempt: 2 } },
    });
  });

  it('cannot overwrite a finalized receipt with a stale step writer', async () => {
    const receipt = { version: 1, receiptKey: 'receipt-final' } as never;
    store.snapshot = {
      runId: 'run-1',
      resourceId: 'resource-from-storage',
      status: 'success',
      value: { winner: true },
      context: {},
      activePaths: [],
      activeStepsPath: {},
      suspendedPaths: {},
      resumeLabels: {},
      waitingPaths: {},
      serializedStepGraph: [],
      timestamp: 2,
      executionGeneration: 'wfeg:generation',
      lifecycleResumeAttempt: 1,
      lifecycleStepStates: {},
      resumeResultReceipt: receipt,
    } as WorkflowRunState;

    await engine.persistStepUpdate({
      workflowId: 'wf',
      runId: 'run-1',
      resourceId: 'resource-stale',
      stepResults: {},
      serializedStepGraph: [],
      executionContext: baseExecutionContext({
        executionGeneration: 'wfeg:generation',
        lifecycleResumeAttempt: 1,
        lifecycleStepStates: {},
        state: { stale: true },
      }),
      workflowStatus: 'running',
      requestContext: new RequestContext(),
    });

    expect(store.calls).toHaveLength(0);
    expect(store.snapshot).toMatchObject({
      status: 'success',
      value: { winner: true },
      resumeResultReceipt: receipt,
    });
    expect(engine.getLastPersistedStatus('run-1')).toBeUndefined();
  });

  it('keeps ordinary custom adapters on the legacy snapshot persistence path', async () => {
    ({ engine, store } = makeEngine(() => true, {}));

    await persist(engine, 'run-1', 'pending');
    await persist(engine, 'run-1', 'running');

    expect(store.calls).toHaveLength(0);
    expect(store.legacyCalls.map(call => call.snapshot.status)).toEqual(['pending', 'running']);
    expect(store.snapshot).toMatchObject({ status: 'running' });

    for (const stepId of ['first', 'second']) {
      await engine.persistStepUpdate({
        workflowId: 'wf',
        runId: 'run-1',
        resourceId: 'resource-1',
        stepResults: { [stepId]: { status: 'success', output: { stepId } } },
        serializedStepGraph: [],
        executionContext: baseExecutionContext({ runId: 'run-1' }),
        workflowStatus: 'running',
        requestContext: new RequestContext(),
        lifecycleEvents: [
          {
            type: 'step.completed',
            stepId,
            stepCallId: `wfsc:${stepId}`,
            stepAttempt: 1,
            output: { stepId },
          },
        ],
      });
    }
    expect(store.snapshot?.lifecycleOutbox?.map(event => ('stepId' in event ? event.stepId : undefined))).toEqual([
      'first',
      'second',
    ]);
  });

  it('applies independent lifecycle redaction before legacy adapter persistence', async () => {
    for (const redact of [
      'context',
      'context-in-place',
      'cloned-context',
      'json-context',
      'partial-context',
      'partial-event-in-place',
      'event',
      'event-in-place',
    ] as const) {
      ({ engine, store } = makeEngine(
        () => true,
        {},
        ({ snapshot }) => {
          if (redact === 'context') {
            return { ...snapshot, context: { completed: { status: 'success' } } };
          }
          if (redact === 'context-in-place') {
            delete (snapshot.context.completed as { output?: unknown }).output;
            return snapshot;
          }
          if (redact === 'cloned-context') {
            const cloned = structuredClone(snapshot);
            delete (cloned.context.completed as { output?: unknown }).output;
            return cloned;
          }
          if (redact === 'json-context') {
            const cloned = JSON.parse(JSON.stringify(snapshot));
            delete cloned.context.completed.output;
            return cloned;
          }
          if (redact === 'partial-context') {
            const cloned = structuredClone(snapshot);
            const output = cloned.context.completed.output;
            delete output.secret;
            delete output.diagnostic.cause;
            return cloned;
          }
          if (redact === 'event-in-place') {
            for (const event of snapshot.lifecycleOutbox ?? []) {
              if (event.type === 'step.completed') delete event.output;
            }
            return snapshot;
          }
          if (redact === 'partial-event-in-place') {
            for (const event of snapshot.lifecycleOutbox ?? []) {
              if (event.type === 'step.completed') {
                const output = event.output as { secret?: boolean; diagnostic: Error };
                delete output.secret;
                delete output.diagnostic.cause;
              }
            }
            return snapshot;
          }
          return {
            ...snapshot,
            lifecycleOutbox: snapshot.lifecycleOutbox?.map(event =>
              event.type === 'step.completed' ? { ...event, output: { redacted: true } } : event,
            ),
          };
        },
      ));

      const date = new Date('2026-01-01T00:00:00.000Z');
      const payload = {
        secret: true,
        diagnostic: new Error('public error', { cause: new Error('private cause') }),
        firstDate: date,
        secondDate: date,
      };
      const outcome = await engine.persistStepUpdate({
        workflowId: 'wf',
        runId: 'run-1',
        resourceId: 'resource-1',
        stepResults: { completed: { status: 'success', output: payload } },
        serializedStepGraph: [],
        executionContext: baseExecutionContext({
          executionGeneration: 'wfeg:generation',
          lifecycleStepStates: { completed: { stepCallId: 'wfsc:completed', stepAttempt: 1 } },
        }),
        workflowStatus: 'running',
        requestContext: new RequestContext(),
        lifecycleEvents: [
          {
            type: 'step.completed',
            stepId: 'completed',
            stepCallId: 'wfsc:completed',
            stepAttempt: 1,
            output: payload,
          },
        ],
      });
      const expectedOutput =
        redact === 'event'
          ? { redacted: true }
          : redact === 'partial-context' || redact === 'partial-event-in-place'
            ? expect.objectContaining({ diagnostic: expect.any(Error), firstDate: date, secondDate: date })
            : undefined;

      expect(outcome?.acceptedEvents).toEqual([
        expect.objectContaining({ type: 'step.completed', output: expectedOutput }),
      ]);
      expect(store.legacyCalls[0]?.snapshot.lifecycleOutbox).toEqual([
        expect.objectContaining({ type: 'step.completed', output: expectedOutput }),
      ]);
      expect(payload.secret).toBe(true);
      expect(payload.diagnostic.cause).toMatchObject({ message: 'private cause' });
      if (redact === 'partial-context' || redact === 'partial-event-in-place') {
        const event = outcome?.acceptedEvents?.[0];
        expect(event?.type).toBe('step.completed');
        if (event?.type !== 'step.completed') throw new Error('missing completion');
        expect(event.output).not.toHaveProperty('secret');
        expect(event.output).not.toHaveProperty('diagnostic.cause');
      }
      if (redact === 'context' || redact === 'context-in-place' || redact === 'cloned-context') {
        expect(store.legacyCalls[0]?.snapshot.context.completed.output).toBeUndefined();
      }
    }
  });

  it('does not invoke a non-enumerable Error getter while pruning lifecycle payloads', async () => {
    const cause = new Error('private cause');
    const causeStack = cause.stack;
    const diagnostic = new Error('public error', { cause });
    const diagnosticStack = diagnostic.stack;
    let getterCalls = 0;
    Object.defineProperty(diagnostic, 'privateDetail', {
      configurable: true,
      enumerable: false,
      get() {
        getterCalls += 1;
        throw new Error('private detail accessed');
      },
    });

    ({ engine, store } = makeEngine(
      () => true,
      {},
      ({ snapshot }) => {
        const output = snapshot.context.completed.output as { diagnostic: Error };
        delete (output.diagnostic as Error & Record<string, unknown>).privateDetail;
        return snapshot;
      },
    ));

    const payload = { diagnostic };
    const outcome = await engine.persistStepUpdate({
      workflowId: 'wf',
      runId: 'run-1',
      resourceId: 'resource-1',
      stepResults: { completed: { status: 'success', output: payload } },
      serializedStepGraph: [],
      executionContext: baseExecutionContext({
        executionGeneration: 'wfeg:generation',
        lifecycleStepStates: { completed: { stepCallId: 'wfsc:completed', stepAttempt: 1 } },
      }),
      workflowStatus: 'running',
      requestContext: new RequestContext(),
      lifecycleEvents: [
        {
          type: 'step.completed',
          stepId: 'completed',
          stepCallId: 'wfsc:completed',
          stepAttempt: 1,
          output: payload,
        },
      ],
    });

    expect(getterCalls).toBe(0);
    expect(diagnostic.stack).toBe(diagnosticStack);
    expect(diagnostic.cause).toBe(cause);
    expect(cause.stack).toBe(causeStack);
    const acceptedEvent = outcome?.acceptedEvents?.[0];
    if (acceptedEvent?.type !== 'step.completed') throw new Error('missing completion');
    const acceptedDiagnostic = (acceptedEvent.output as { diagnostic: Error }).diagnostic;
    expect(acceptedDiagnostic.stack).toBe(diagnosticStack);
    expect(acceptedDiagnostic.cause).not.toBe(cause);
    expect(acceptedDiagnostic.cause).toBeInstanceOf(Error);
    expect(acceptedDiagnostic.cause?.stack).toBe(causeStack);
    expect(Object.hasOwn(acceptedDiagnostic, 'privateDetail')).toBe(false);
  });

  it.each(['delete', 'enumerability'] as const)(
    'does not restore a redacted non-enumerable Error property into accepted lifecycle events (%s)',
    async redaction => {
      const privateDetail = 'privateDetail';
      const cause = new Error('private cause');
      const causeStack = cause.stack;
      const diagnostic = new Error('public error', { cause });
      const diagnosticStack = diagnostic.stack;
      Object.defineProperty(diagnostic, privateDetail, {
        configurable: true,
        enumerable: redaction === 'enumerability',
        value: 'private',
        writable: true,
      });

      ({ engine, store } = makeEngine(
        () => true,
        {},
        ({ snapshot }) => {
          const completed = snapshot.context.completed as { output: Record<string, unknown> };
          const sourceDiagnostic = completed.output.diagnostic as Error;
          const redactedDiagnostic = Object.create(Object.getPrototypeOf(sourceDiagnostic)) as Error &
            Record<PropertyKey, unknown>;
          Object.defineProperties(redactedDiagnostic, Object.getOwnPropertyDescriptors(sourceDiagnostic));
          if (redaction === 'delete') {
            delete redactedDiagnostic[privateDetail];
          } else {
            const privateDescriptor = Object.getOwnPropertyDescriptor(redactedDiagnostic, privateDetail);
            if (!privateDescriptor) throw new Error('missing private detail');
            Object.defineProperty(redactedDiagnostic, privateDetail, { ...privateDescriptor, enumerable: false });
          }
          return {
            ...snapshot,
            context: {
              ...snapshot.context,
              completed: {
                ...completed,
                output: { ...completed.output, diagnostic: redactedDiagnostic },
              },
            },
          };
        },
      ));

      const payload = { canonical: 'unchanged', diagnostic };
      const outcome = await engine.persistStepUpdate({
        workflowId: 'wf',
        runId: 'run-1',
        resourceId: 'resource-1',
        stepResults: { completed: { status: 'success', output: payload } },
        serializedStepGraph: [],
        executionContext: baseExecutionContext({
          executionGeneration: 'wfeg:generation',
          lifecycleStepStates: { completed: { stepCallId: 'wfsc:completed', stepAttempt: 1 } },
        }),
        workflowStatus: 'running',
        requestContext: new RequestContext(),
        lifecycleEvents: [
          {
            type: 'step.completed',
            stepId: 'completed',
            stepCallId: 'wfsc:completed',
            stepAttempt: 1,
            output: payload,
          },
        ],
      });

      const acceptedEvent = outcome?.acceptedEvents?.[0];
      if (acceptedEvent?.type !== 'step.completed') throw new Error('missing completion');
      const acceptedOutput = acceptedEvent.output as { canonical: string; diagnostic: Error };
      expect(acceptedOutput.canonical).toBe('unchanged');
      expect(serializeError(acceptedOutput.diagnostic)).not.toHaveProperty(privateDetail);
      if (redaction === 'delete') {
        expect(Object.hasOwn(acceptedOutput.diagnostic, privateDetail)).toBe(false);
      } else {
        expect(Object.getOwnPropertyDescriptor(acceptedOutput.diagnostic, privateDetail)).toMatchObject({
          enumerable: false,
          value: 'private',
        });
      }
      expect(diagnostic.stack).toBe(diagnosticStack);
      expect(diagnostic.cause).toBe(cause);
      expect(cause.stack).toBe(causeStack);
      expect(Object.getOwnPropertyDescriptor(diagnostic, privateDetail)).toMatchObject({
        enumerable: redaction === 'enumerability',
        value: 'private',
      });
    },
  );

  it('returns a terminal disposition instead of overwriting a legacy snapshot', async () => {
    ({ engine, store } = makeEngine(() => true, {}));
    store.snapshot = {
      runId: 'run-1',
      status: 'success',
      value: { remote: true },
      context: {},
      activePaths: [],
      activeStepsPath: {},
      suspendedPaths: {},
      resumeLabels: {},
      waitingPaths: {},
      serializedStepGraph: [],
      timestamp: 1,
      executionGeneration: 'wfeg:generation',
    } as WorkflowRunState;

    const outcome = await engine.persistStepUpdate({
      workflowId: 'wf',
      runId: 'run-1',
      resourceId: 'resource-1',
      stepResults: {},
      serializedStepGraph: [],
      executionContext: baseExecutionContext({
        runId: 'run-1',
        executionGeneration: 'wfeg:generation',
      }),
      workflowStatus: 'running',
      requestContext: new RequestContext(),
    });

    expect(outcome).toMatchObject({ status: 'finalized', disposition: 'success' });
    expect(store.legacyCalls).toHaveLength(0);
  });

  it('fails closed for a resumed write when atomic resume lacks fenced step updates', async () => {
    ({ engine, store } = makeEngine(() => true, { atomicResumeVersion: 1 }));

    await expect(
      engine.persistStepUpdate({
        workflowId: 'wf',
        runId: 'run-1',
        resourceId: 'resource-1',
        stepResults: {},
        serializedStepGraph: [],
        executionContext: baseExecutionContext({
          executionGeneration: 'wfeg:generation',
          lifecycleResumeAttempt: 1,
          lifecycleStepStates: {},
          resumeOperationHash: `sha256:${'a'.repeat(64)}`,
        }),
        workflowStatus: 'running',
        requestContext: new RequestContext(),
      }),
    ).rejects.toThrow('does not support atomic resume admission and fenced resumed step updates');
    expect(store.calls).toHaveLength(0);
    expect(store.legacyCalls).toHaveLength(0);
  });

  it('fails closed for a resumed write when fenced step updates lack atomic resume', async () => {
    ({ engine, store } = makeEngine(() => true, { fencedStepUpdateVersion: 1 }));

    await expect(
      engine.persistStepUpdate({
        workflowId: 'wf',
        runId: 'run-1',
        resourceId: 'resource-1',
        stepResults: {},
        serializedStepGraph: [],
        executionContext: baseExecutionContext({
          executionGeneration: 'wfeg:generation',
          lifecycleResumeAttempt: 1,
          lifecycleStepStates: {},
          resumeOperationHash: `sha256:${'a'.repeat(64)}`,
        }),
        workflowStatus: 'running',
        requestContext: new RequestContext(),
      }),
    ).rejects.toThrow('does not support atomic resume admission and fenced resumed step updates');
    expect(store.calls).toHaveLength(0);
    expect(store.legacyCalls).toHaveLength(0);
  });
});

describe('DefaultExecutionEngine — lastPersistedStatus accessors', () => {
  function makeBareEngine() {
    return new DefaultExecutionEngine({
      mastra: undefined,
      options: {
        validateInputs: false,
        shouldPersistSnapshot: () => true,
      },
    });
  }

  it('returns undefined for an unknown run', () => {
    const engine = makeBareEngine();
    expect(engine.getLastPersistedStatus('nope')).toBeUndefined();
  });

  it('records and clears status via public accessors', () => {
    const engine = makeBareEngine();
    engine.setLastPersistedStatus('run-1', 'running');
    expect(engine.getLastPersistedStatus('run-1')).toBe('running');
    engine.clearLastPersistedStatus('run-1');
    expect(engine.getLastPersistedStatus('run-1')).toBeUndefined();
  });

  // Regression guard for issue #19056: the execute loop must clear the
  // last-persisted-status tracker on early terminal exits (failed, canceled,
  // tripwire) so the process-local map does not grow unbounded across many
  // runs. Suspended and paused runs deliberately keep their entry so a
  // subsequent resume in the same process still refuses to overwrite them
  // with `running` mid-resume.
  it.each(['failed', 'canceled', 'success'] as const)(
    'clearLastPersistedStatus removes the tracker for %s terminal exits',
    status => {
      const engine = makeBareEngine();
      engine.setLastPersistedStatus('run-1', status as WorkflowRunStatus);
      engine.clearLastPersistedStatus('run-1');
      expect(engine.getLastPersistedStatus('run-1')).toBeUndefined();
    },
  );

  it('keeps the tracker for suspended so resume cannot clobber it', () => {
    const engine = makeBareEngine();
    engine.setLastPersistedStatus('run-1', 'suspended');
    // Execute loop deliberately does NOT clear on suspended.
    expect(engine.getLastPersistedStatus('run-1')).toBe('suspended');
  });
});
