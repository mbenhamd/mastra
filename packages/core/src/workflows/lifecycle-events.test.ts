import { describe, expect, it, vi } from 'vitest';
import { z } from 'zod/v4';

import { RequestContext } from '../di';
import { EventEmitterPubSub } from '../events/event-emitter';
import { Mastra } from '../mastra';
import { MockStore } from '../storage/mock';
import { createWorkflow } from './create';
import { DefaultExecutionEngine } from './default';
import { createWorkflow as createEventedWorkflow } from './evented';
import {
  createWorkflowExecutionGeneration,
  getOrCreateWorkflowStepLifecycleState,
  getWorkflowLifecycleEventId,
  getWorkflowLifecycleSemanticKey,
  getWorkflowLifecycleTopic,
  mergeWorkflowStepLifecycleStates,
  parseWorkflowLifecycleRecord,
  publishWorkflowLifecycleEvent,
} from './lifecycle-events';
import type { WorkflowLifecycleEvent, WorkflowLifecycleRecord, WorkflowLifecycleRecordError } from './lifecycle-events';
import { createStep } from './workflow';

type PublishSpy = ReturnType<typeof vi.spyOn<EventEmitterPubSub, 'publish'>>;

function lifecycleRecords(spy: PublishSpy): Array<{ topic: string; record: WorkflowLifecycleRecord }> {
  const records: Array<{ topic: string; record: WorkflowLifecycleRecord }> = [];
  for (const [topic, event] of spy.mock.calls) {
    if (!topic.startsWith('workflow.lifecycle.v1.')) continue;
    records.push({ topic, record: parseWorkflowLifecycleRecord(event.data) });
  }
  return records;
}

function stepEvents(records: Array<{ record: WorkflowLifecycleRecord }>) {
  return records
    .map(item => item.record.event)
    .filter((event): event is Extract<WorkflowLifecycleEvent, { stepCallId: string }> => 'stepCallId' in event);
}

describe('canonical workflow lifecycle model', () => {
  it('clears a terminal lifecycle topic only after its advertised replay retention and minimum reconnect window', async () => {
    class RetainedPubSub extends EventEmitterPubSub {
      override get indexedReplay() {
        return { scope: 'process' as const, retentionMs: 100, maxEvents: 10 };
      }

      override clearTopic = vi.fn(async (_topic: string) => {});
    }

    vi.useFakeTimers();
    try {
      const pubsub = new RetainedPubSub();
      await publishWorkflowLifecycleEvent({
        pubsub,
        workflowId: 'cleanup-workflow',
        runId: 'cleanup-run',
        executionGeneration: 'cleanup-generation',
        event: { type: 'workflow.finished', resumeAttempt: 0, status: 'success' },
      });
      const cleanupTopic = getWorkflowLifecycleTopic({
        workflowId: 'cleanup-workflow',
        runId: 'cleanup-run',
        executionGeneration: 'cleanup-generation',
      });

      await vi.advanceTimersByTimeAsync(29_999);
      expect(pubsub.clearTopic).not.toHaveBeenCalled();
      await vi.advanceTimersByTimeAsync(1);
      expect(pubsub.clearTopic).toHaveBeenCalledWith(cleanupTopic);
    } finally {
      vi.useRealTimers();
    }
  });

  it('clears terminal topics on transports without indexed replay after the reconnect backstop', async () => {
    class PersistentPubSub extends EventEmitterPubSub {
      override clearTopic = vi.fn(async (_topic: string) => {});
    }

    vi.useFakeTimers();
    try {
      const pubsub = new PersistentPubSub();
      await publishWorkflowLifecycleEvent({
        pubsub,
        workflowId: 'plain-cleanup-workflow',
        runId: 'plain-cleanup-run',
        executionGeneration: 'plain-cleanup-generation',
        event: { type: 'workflow.finished', resumeAttempt: 0, status: 'success' },
      });
      const cleanupTopic = getWorkflowLifecycleTopic({
        workflowId: 'plain-cleanup-workflow',
        runId: 'plain-cleanup-run',
        executionGeneration: 'plain-cleanup-generation',
      });

      await vi.advanceTimersByTimeAsync(29_999);
      expect(pubsub.clearTopic).not.toHaveBeenCalled();
      await vi.advanceTimersByTimeAsync(1);
      expect(pubsub.clearTopic).toHaveBeenCalledWith(cleanupTopic);
    } finally {
      vi.useRealTimers();
    }
  });

  it('scopes topics by workflow, run, and execution generation', () => {
    const sharedRun = 'shared.run/1';
    const first = getWorkflowLifecycleTopic({
      workflowId: 'workflow.alpha',
      runId: sharedRun,
      executionGeneration: 'generation.one',
    });
    const secondWorkflow = getWorkflowLifecycleTopic({
      workflowId: 'workflow.beta',
      runId: sharedRun,
      executionGeneration: 'generation.one',
    });
    const secondGeneration = getWorkflowLifecycleTopic({
      workflowId: 'workflow.alpha',
      runId: sharedRun,
      executionGeneration: 'generation.two',
    });

    expect(new Set([first, secondWorkflow, secondGeneration]).size).toBe(3);
    expect(first).not.toContain('workflow.alpha');
  });

  it('clones broker step state and preserves the highest persisted attempt', () => {
    const incoming = {
      coordinate: { stepCallId: 'stable-call', stepAttempt: 1 },
      incomingOnly: { stepCallId: 'incoming-call', stepAttempt: 1 },
    };
    const persisted = {
      coordinate: { stepCallId: 'stable-call', stepAttempt: 3 },
      persistedOnly: { stepCallId: 'persisted-call', stepAttempt: 2 },
    };

    const merged = mergeWorkflowStepLifecycleStates(incoming, persisted);
    expect(merged).toEqual({
      coordinate: { stepCallId: 'stable-call', stepAttempt: 3 },
      incomingOnly: { stepCallId: 'incoming-call', stepAttempt: 1 },
      persistedOnly: { stepCallId: 'persisted-call', stepAttempt: 2 },
    });
    merged.coordinate!.stepAttempt = 4;
    expect(incoming.coordinate.stepAttempt).toBe(1);
    expect(persisted.coordinate.stepAttempt).toBe(3);
  });

  it('rejects conflicting step-call identity for one lifecycle coordinate', () => {
    expect(() =>
      mergeWorkflowStepLifecycleStates(
        { coordinate: { stepCallId: 'incoming', stepAttempt: 1 } },
        { coordinate: { stepCallId: 'persisted', stepAttempt: 1 } },
      ),
    ).toThrow('conflicting step-call identity');
  });

  it('reserves one pre-start execution identity and reuses it for the run', async () => {
    const pubsub = new EventEmitterPubSub();
    const publish = vi.spyOn(pubsub, 'publish');
    const step = createStep({
      id: 'identity-step',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      execute: async () => ({}),
    });
    const workflow = createWorkflow({
      id: 'identity-reservation',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      steps: [step],
    })
      .then(step)
      .commit();
    const run = await workflow.createRun({ runId: 'identity-reservation-run', pubsub });

    const beforeStart = await run.getLifecycleExecutionIdentity();
    expect(await run.getLifecycleExecutionIdentity()).toEqual(beforeStart);
    await expect(run.start({ inputData: {} })).resolves.toMatchObject({ status: 'success' });
    expect(await run.getLifecycleExecutionIdentity()).toEqual(beforeStart);

    const records = lifecycleRecords(publish);
    expect(records.length).toBeGreaterThan(0);
    expect(records.every(item => item.topic === beforeStart.topic)).toBe(true);
    expect(records.every(item => item.record.executionGeneration === beforeStart.executionGeneration)).toBe(true);
  });

  it('fails closed for malformed chunks and canonical identity mismatch', () => {
    expect(() => parseWorkflowLifecycleRecord({ type: 'tool-call', payload: { command: 'echo' } })).toThrowError(
      expect.objectContaining<Partial<WorkflowLifecycleRecordError>>({ reason: 'malformed' }),
    );

    const record: WorkflowLifecycleRecord = {
      schemaVersion: 1,
      workflowId: 'alpha',
      runId: 'shared',
      executionGeneration: 'generation-a',
      event: { type: 'workflow.started', resumeAttempt: 0 },
    };
    expect(() =>
      parseWorkflowLifecycleRecord(record, {
        workflowId: 'beta',
        runId: 'shared',
        executionGeneration: 'generation-a',
      }),
    ).toThrowError(expect.objectContaining<Partial<WorkflowLifecycleRecordError>>({ reason: 'identity-mismatch' }));
  });

  it('deduplicates semantic republishes while keeping resume cycles distinct', () => {
    const record: WorkflowLifecycleRecord = {
      schemaVersion: 1,
      workflowId: 'semantic-workflow',
      runId: 'semantic-run',
      executionGeneration: 'semantic-generation',
      event: { type: 'workflow.suspended', resumeAttempt: 0, suspendedStepIds: ['approval'] },
    };
    const republished = { ...record, event: { ...record.event } } satisfies WorkflowLifecycleRecord;
    const nextCycle: WorkflowLifecycleRecord = {
      ...record,
      event: { type: 'workflow.suspended', resumeAttempt: 1, suspendedStepIds: ['approval'] },
    };

    expect(getWorkflowLifecycleSemanticKey(republished)).toBe(getWorkflowLifecycleSemanticKey(record));
    expect(getWorkflowLifecycleSemanticKey(nextCycle)).not.toBe(getWorkflowLifecycleSemanticKey(record));
    expect(getWorkflowLifecycleEventId(republished)).toBe(getWorkflowLifecycleEventId(record));
    expect(getWorkflowLifecycleEventId(nextCycle)).not.toBe(getWorkflowLifecycleEventId(record));
  });

  it('keeps one call id across attempts and isolates parallel/foreach coordinates', () => {
    const states = {};
    const base = {
      workflowId: 'identity-workflow',
      runId: 'identity-run',
      executionGeneration: createWorkflowExecutionGeneration(),
      stepId: 'worker',
      states,
    };
    const first = getOrCreateWorkflowStepLifecycleState({ ...base, executionPath: [0, 0] });
    first.state.stepAttempt += 1;
    const retry = getOrCreateWorkflowStepLifecycleState({ ...base, executionPath: [0, 0] });
    retry.state.stepAttempt += 1;
    const parallelSibling = getOrCreateWorkflowStepLifecycleState({ ...base, executionPath: [0, 1] });
    const foreachSibling = getOrCreateWorkflowStepLifecycleState({
      ...base,
      executionPath: [1, 0],
      foreachIndex: 1,
    });

    expect(retry.state.stepCallId).toBe(first.state.stepCallId);
    expect(retry.state.stepAttempt).toBe(2);
    expect(
      new Set([first.state.stepCallId, parallelSibling.state.stepCallId, foreachSibling.state.stepCallId]).size,
    ).toBe(3);
  });

  it('assigns unique call identities to actual parallel branches and foreach iterations', async () => {
    const parallelPubsub = new EventEmitterPubSub();
    const parallelPublish = vi.spyOn(parallelPubsub, 'publish');
    const first = createStep({
      id: 'parallel-first',
      inputSchema: z.object({}),
      outputSchema: z.object({ branch: z.string() }),
      execute: async () => ({ branch: 'first' }),
    });
    const second = createStep({
      id: 'parallel-second',
      inputSchema: z.object({}),
      outputSchema: z.object({ branch: z.string() }),
      execute: async () => ({ branch: 'second' }),
    });
    const parallelWorkflow = createWorkflow({
      id: 'parallel-identities',
      inputSchema: z.object({}),
      outputSchema: z.any(),
      steps: [first, second],
    })
      .parallel([first, second])
      .commit();
    const parallelRun = await parallelWorkflow.createRun({
      runId: 'parallel-identities-run',
      pubsub: parallelPubsub,
    });

    await expect(parallelRun.start({ inputData: {} })).resolves.toMatchObject({ status: 'success' });
    const parallelStarts = stepEvents(lifecycleRecords(parallelPublish)).filter(event => event.type === 'step.started');
    expect(parallelStarts.map(event => event.stepId).sort()).toEqual(['parallel-first', 'parallel-second']);
    expect(new Set(parallelStarts.map(event => event.stepCallId)).size).toBe(2);

    const foreachPubsub = new EventEmitterPubSub();
    const foreachPublish = vi.spyOn(foreachPubsub, 'publish');
    const worker = createStep({
      id: 'foreach-worker',
      inputSchema: z.number(),
      outputSchema: z.number(),
      execute: async ({ inputData }) => inputData * 2,
    });
    const foreachWorkflow = createWorkflow({
      id: 'foreach-identities',
      inputSchema: z.array(z.number()),
      outputSchema: z.array(z.number()),
      steps: [worker],
    })
      .foreach(worker, { concurrency: 2 })
      .commit();
    const foreachRun = await foreachWorkflow.createRun({ runId: 'foreach-identities-run', pubsub: foreachPubsub });

    await expect(foreachRun.start({ inputData: [1, 2, 3] })).resolves.toMatchObject({ status: 'success' });
    const foreachStarts = stepEvents(lifecycleRecords(foreachPublish)).filter(
      event => event.type === 'step.started' && event.stepId === 'foreach-worker',
    );
    expect(foreachStarts).toHaveLength(3);
    expect(new Set(foreachStarts.map(event => event.stepCallId)).size).toBe(3);
  });

  it('emits stable default-engine retry, failure, and finish identities', async () => {
    const pubsub = new EventEmitterPubSub();
    const publish = vi.spyOn(pubsub, 'publish');
    let invocation = 0;
    const flaky = createStep({
      id: 'flaky',
      inputSchema: z.object({ value: z.number() }),
      outputSchema: z.object({ value: z.number() }),
      retries: 1,
      execute: async ({ inputData }) => {
        invocation += 1;
        if (invocation === 1) throw new Error('retry me');
        return inputData;
      },
    });
    const workflow = createWorkflow({
      id: 'default-retry',
      inputSchema: z.object({ value: z.number() }),
      outputSchema: z.object({ value: z.number() }),
      steps: [flaky],
    })
      .then(flaky)
      .commit();
    const run = await workflow.createRun({ runId: 'default-retry-run', pubsub });

    await expect(run.start({ inputData: { value: 1 } })).resolves.toMatchObject({ status: 'success' });

    const records = lifecycleRecords(publish);
    const steps = stepEvents(records).filter(event => event.stepId === 'flaky');
    expect(steps.map(event => [event.type, event.stepAttempt])).toEqual([
      ['step.started', 1],
      ['step.retrying', 2],
      ['step.completed', 2],
      ['step.finished', 2],
    ]);
    expect(new Set(steps.map(event => event.stepCallId)).size).toBe(1);
    expect(records.at(0)?.record.event.type).toBe('workflow.started');
    expect(records.at(-1)?.record.event).toMatchObject({ type: 'workflow.finished', status: 'success' });
  });

  it('emits explicit step and workflow failure terminals', async () => {
    const pubsub = new EventEmitterPubSub();
    const publish = vi.spyOn(pubsub, 'publish');
    const failing = createStep({
      id: 'always-fails',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      execute: async () => {
        throw new Error('expected lifecycle failure');
      },
    });
    const workflow = createWorkflow({
      id: 'default-failure',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      steps: [failing],
    })
      .then(failing)
      .commit();
    const run = await workflow.createRun({ runId: 'default-failure-run', pubsub });

    await expect(run.start({ inputData: {} })).resolves.toMatchObject({ status: 'failed' });

    const events = lifecycleRecords(publish).map(item => item.record.event);
    expect(events).toContainEqual(expect.objectContaining({ type: 'step.failed', stepId: 'always-fails' }));
    expect(events).toContainEqual(
      expect.objectContaining({ type: 'step.finished', stepId: 'always-fails', status: 'failed' }),
    );
    expect(events).toContainEqual(expect.objectContaining({ type: 'workflow.failed' }));
    expect(events.at(-1)).toMatchObject({ type: 'workflow.finished', status: 'failed' });
  });

  it('emits workflow.failed and workflow.finished before an empty graph throws', async () => {
    const pubsub = new EventEmitterPubSub();
    const publish = vi.spyOn(pubsub, 'publish');
    const engine = new DefaultExecutionEngine({ mastra: undefined });

    await expect(
      engine.execute({
        workflowId: 'empty-graph',
        runId: 'empty-graph-run',
        executionGeneration: 'empty-graph-generation',
        lifecycleResumeAttempt: 0,
        lifecycleStepStates: {},
        graph: { id: 'empty-graph', steps: [] },
        serializedStepGraph: [],
        input: {},
        initialState: {},
        pubsub,
        requestContext: new RequestContext(),
        abortController: new AbortController(),
      }),
    ).rejects.toThrow('Workflow must have at least one step');

    expect(lifecycleRecords(publish).map(item => item.record.event.type)).toEqual([
      'workflow.started',
      'workflow.failed',
      'workflow.finished',
    ]);
  });

  it('retains generation through two suspend/resume cycles without collapsing transitions', async () => {
    const pubsub = new EventEmitterPubSub();
    const publish = vi.spyOn(pubsub, 'publish');
    const storage = new MockStore();
    const approval = createStep({
      id: 'approval',
      inputSchema: z.object({ value: z.number() }),
      outputSchema: z.object({ value: z.number(), approved: z.boolean() }),
      suspendSchema: z.object({ value: z.number() }),
      resumeSchema: z.object({ round: z.number() }),
      execute: async ({ inputData, resumeData, suspend }) => {
        if (!resumeData) {
          await suspend({ value: inputData.value });
          return { value: inputData.value, approved: false };
        }
        if (resumeData.round === 1) {
          await suspend({ value: inputData.value });
          return { value: inputData.value, approved: false };
        }
        return { value: inputData.value, approved: true };
      },
    });
    const workflow = createWorkflow({
      id: 'default-resume',
      inputSchema: z.object({ value: z.number() }),
      outputSchema: z.object({ value: z.number(), approved: z.boolean() }),
      steps: [approval],
    })
      .then(approval)
      .commit();
    new Mastra({ logger: false, storage, workflows: { defaultResume: workflow } });
    const run = await workflow.createRun({ runId: 'default-resume-run', pubsub });

    await expect(run.start({ inputData: { value: 2 } })).resolves.toMatchObject({ status: 'suspended' });
    await expect(run.resume({ step: 'approval', resumeData: { round: 1 } })).resolves.toMatchObject({
      status: 'suspended',
    });
    await expect(run.resume({ step: 'approval', resumeData: { round: 2 } })).resolves.toMatchObject({
      status: 'success',
    });

    const records = lifecycleRecords(publish);
    expect(new Set(records.map(item => item.record.executionGeneration)).size).toBe(1);
    const steps = stepEvents(records).filter(event => event.stepId === 'approval');
    expect(steps.map(event => [event.type, event.stepAttempt])).toEqual([
      ['step.started', 1],
      ['step.suspended', 1],
      ['step.resumed', 2],
      ['step.suspended', 2],
      ['step.resumed', 3],
      ['step.completed', 3],
      ['step.finished', 3],
    ]);
    expect(new Set(steps.map(event => event.stepCallId)).size).toBe(1);
    const workflowTransitions = records.filter(item => !('stepCallId' in item.record.event));
    expect(workflowTransitions.map(item => [item.record.event.type, item.record.event.resumeAttempt])).toEqual([
      ['workflow.started', 0],
      ['workflow.suspended', 0],
      ['workflow.resumed', 1],
      ['workflow.suspended', 1],
      ['workflow.resumed', 2],
      ['workflow.finished', 2],
    ]);
    expect(new Set(workflowTransitions.map(item => getWorkflowLifecycleSemanticKey(item.record))).size).toBe(
      workflowTransitions.length,
    );
  });

  it('emits step.resumed for a valid falsy resume payload', async () => {
    const pubsub = new EventEmitterPubSub();
    const publish = vi.spyOn(pubsub, 'publish');
    let invocation = 0;
    const approval = createStep({
      id: 'falsy-approval',
      inputSchema: z.object({}),
      outputSchema: z.object({ approved: z.boolean() }),
      suspendSchema: z.object({}),
      resumeSchema: z.boolean(),
      execute: async ({ resumeData, suspend }) => {
        invocation += 1;
        if (invocation === 1) {
          await suspend({});
          return { approved: true };
        }
        return { approved: resumeData };
      },
    });
    const workflow = createWorkflow({
      id: 'default-falsy-resume',
      inputSchema: z.object({}),
      outputSchema: z.object({ approved: z.boolean() }),
      steps: [approval],
    })
      .then(approval)
      .commit();
    new Mastra({ logger: false, storage: new MockStore(), workflows: { defaultFalsyResume: workflow } });
    const run = await workflow.createRun({ runId: 'default-falsy-resume-run', pubsub });

    await expect(run.start({ inputData: {} })).resolves.toMatchObject({ status: 'suspended' });
    await expect(run.resume({ step: 'falsy-approval', resumeData: false })).resolves.toMatchObject({
      status: 'success',
      result: { approved: false },
    });

    const resumed = stepEvents(lifecycleRecords(publish)).find(event => event.type === 'step.resumed');
    expect(resumed).toMatchObject({
      type: 'step.resumed',
      stepId: 'falsy-approval',
      stepAttempt: 2,
      resumeData: false,
    });
  });

  it('maps AbortSignal cancellation to step.canceled and workflow.canceled terminal events', async () => {
    const pubsub = new EventEmitterPubSub();
    const publish = vi.spyOn(pubsub, 'publish');
    let entered!: () => void;
    const started = new Promise<void>(resolve => {
      entered = resolve;
    });
    const blocking = createStep({
      id: 'blocking',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      execute: async ({ abortSignal }) => {
        entered();
        await new Promise<void>(resolve => {
          abortSignal.addEventListener('abort', () => resolve(), { once: true });
        });
        return {};
      },
    });
    const workflow = createWorkflow({
      id: 'default-cancel',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      steps: [blocking],
    })
      .then(blocking)
      .commit();
    const run = await workflow.createRun({ runId: 'default-cancel-run', pubsub });
    const result = run.start({ inputData: {} });
    await started;
    await run.cancel();
    await expect(result).resolves.toMatchObject({ status: 'canceled' });

    const events = lifecycleRecords(publish).map(item => item.record.event);
    expect(events).toContainEqual(expect.objectContaining({ type: 'step.canceled', stepId: 'blocking' }));
    expect(events).toContainEqual(expect.objectContaining({ type: 'workflow.canceled' }));
    expect(events.at(-1)).toMatchObject({ type: 'workflow.finished', status: 'canceled' });
  });

  it('reserves canonical identity when a default-engine run is canceled before start', async () => {
    const pubsub = new EventEmitterPubSub();
    const publish = vi.spyOn(pubsub, 'publish');
    const step = createStep({
      id: 'never-started-step',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      execute: async () => ({}),
    });
    const workflow = createWorkflow({
      id: 'default-cancel-before-start',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      steps: [step],
    })
      .then(step)
      .commit();
    const run = await workflow.createRun({ runId: 'default-pending-cancel', pubsub });

    await run.cancel();

    const identity = await run.getLifecycleExecutionIdentity();
    const records = lifecycleRecords(publish);
    expect(records.map(item => item.record.event.type)).toEqual(['workflow.canceled', 'workflow.finished']);
    expect(records.every(item => item.topic === identity.topic)).toBe(true);
    expect(run.workflowRunStatus).toBe('canceled');
  });

  it('persists cancellation before best-effort lifecycle publication', async () => {
    const pubsub = new EventEmitterPubSub();
    const publish = vi.spyOn(pubsub, 'publish');
    const storage = new MockStore();
    const approval = createStep({
      id: 'cancel-publication-approval',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      execute: async ({ suspend }) => {
        await suspend({});
        return {};
      },
    });
    const workflow = createWorkflow({
      id: 'cancel-publication-failure',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      steps: [approval],
    })
      .then(approval)
      .commit();
    new Mastra({ logger: false, storage, workflows: { cancelPublicationFailure: workflow } });
    const run = await workflow.createRun({ runId: 'cancel-publication-failure-run', pubsub });
    await expect(run.start({ inputData: {} })).resolves.toMatchObject({ status: 'suspended' });
    publish.mockRejectedValue(new Error('transport unavailable'));

    await expect(run.cancel()).resolves.toBeUndefined();

    const workflowsStore = await storage.getStore('workflows');
    const snapshot = await workflowsStore.loadWorkflowSnapshot({
      workflowName: workflow.id,
      runId: run.runId,
    });
    expect(snapshot?.status).toBe('canceled');
  });

  it('isolates two workflows that intentionally use the same run id', async () => {
    const pubsub = new EventEmitterPubSub();
    const publish = vi.spyOn(pubsub, 'publish');
    const makeWorkflow = (id: string) => {
      const step = createStep({
        id: `${id}-step`,
        inputSchema: z.object({}),
        outputSchema: z.object({}),
        execute: async () => ({}),
      });
      return createWorkflow({ id, inputSchema: z.object({}), outputSchema: z.object({}), steps: [step] })
        .then(step)
        .commit();
    };
    const first = makeWorkflow('same-run-first');
    const second = makeWorkflow('same-run-second');
    const [firstRun, secondRun] = await Promise.all([
      first.createRun({ runId: 'shared-run', pubsub }),
      second.createRun({ runId: 'shared-run', pubsub }),
    ]);

    await Promise.all([firstRun.start({ inputData: {} }), secondRun.start({ inputData: {} })]);

    const records = lifecycleRecords(publish);
    expect(new Set(records.map(item => item.topic)).size).toBe(2);
    expect(new Set(records.map(item => item.record.workflowId))).toEqual(
      new Set(['same-run-first', 'same-run-second']),
    );
  });

  it('emits the same retry identity from the evented engine', async () => {
    const pubsub = new EventEmitterPubSub();
    const publish = vi.spyOn(pubsub, 'publish');
    let invocation = 0;
    const flaky = createStep({
      id: 'evented-flaky',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      retries: 1,
      execute: async () => {
        invocation += 1;
        if (invocation === 1) throw new Error('retry evented');
        return {};
      },
    });
    const workflow = createEventedWorkflow({
      id: 'evented-lifecycle',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      steps: [flaky],
    })
      .then(flaky)
      .commit();
    const mastra = new Mastra({
      logger: false,
      storage: new MockStore(),
      workflows: { eventedLifecycle: workflow },
      pubsub,
    });
    await mastra.startWorkers();

    try {
      const run = await workflow.createRun({ runId: 'evented-lifecycle-run' });
      await expect(run.start({ inputData: {} })).resolves.toMatchObject({ status: 'success' });
    } finally {
      await mastra.stopWorkers();
    }

    const steps = stepEvents(lifecycleRecords(publish)).filter(event => event.stepId === 'evented-flaky');
    expect(steps.map(event => [event.type, event.stepAttempt])).toEqual([
      ['step.started', 1],
      ['step.retrying', 2],
      ['step.completed', 2],
      ['step.finished', 2],
    ]);
    expect(new Set(steps.map(event => event.stepCallId)).size).toBe(1);
  });
});
