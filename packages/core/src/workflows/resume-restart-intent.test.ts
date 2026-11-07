/**
 * PF-7806: a restart after a crash continues an accepted resume.
 *
 * The fork keeps a resumed step's suspended record until the step settles (a
 * `running` write over it is refused), so for the whole time the resumed step
 * and the steps after it run, storage shows the run `running` with that step
 * still suspended. A process that dies in that window used to lose the answer:
 * `restart()` re-ran the step without resume data and it suspended again. The
 * resume claim now stores the accepted input, and `restart()` continues the
 * resume with it, once per claim.
 *
 * Each "host" is its own Mastra instance on one shared store. A crash is a
 * host whose step never returns and whose run-ownership lease is released.
 */

import { describe, expect, it } from 'vitest';
import { z } from 'zod/v4';
import { EventEmitterPubSub } from '../events/event-emitter';
import { Mastra } from '../mastra';
import type { MastraStorage } from '../storage';
import { MockStore } from '../storage/mock';
import { createWorkflow } from './create';
import { createStep as createEventedStep, createWorkflow as createEventedWorkflow } from './evented';
import { createStep } from './workflow';

type Behaviour = 'block' | 'run';
type Engine = 'default' | 'evented';

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>(r => (resolve = r));
  return { promise, resolve };
}

/**
 * Host behaviour for the resumed checkpoint (`review`, blocking only once it has
 * an answer) and each step after it, plus everything the host observed.
 */
interface HostBehaviour {
  review: Behaviour;
  apply: Behaviour;
  publish: Behaviour;
}

function hostWorkflow(engine: Engine, behaviour: HostBehaviour) {
  const step = (engine === 'evented' ? createEventedStep : createStep) as typeof createStep;
  const workflowOf = (engine === 'evented' ? createEventedWorkflow : createWorkflow) as typeof createWorkflow;
  const seen = { review: [] as unknown[], apply: [] as unknown[], publish: [] as unknown[] };
  const reviewResumed = deferred();
  const applyStarted = deferred();
  const publishStarted = deferred();
  const never = new Promise<never>(() => {});

  const review = step({
    id: 'review',
    inputSchema: z.object({ draft: z.string() }),
    outputSchema: z.object({ draft: z.string() }),
    suspendSchema: z.object({ draft: z.string() }),
    resumeSchema: z.object({ decision: z.enum(['approve', 'revise']), draft: z.string().optional() }),
    execute: async ({ inputData, resumeData, suspend }) => {
      seen.review.push(resumeData);
      if (!resumeData) return suspend({ draft: inputData.draft });
      reviewResumed.resolve();
      if (behaviour.review === 'block') await never;
      return { draft: resumeData.decision === 'revise' ? resumeData.draft! : inputData.draft };
    },
  });
  const apply = step({
    id: 'apply',
    inputSchema: z.object({ draft: z.string() }),
    outputSchema: z.object({ applied: z.string() }),
    execute: async ({ inputData }) => {
      seen.apply.push(inputData.draft);
      applyStarted.resolve();
      if (behaviour.apply === 'block') await never;
      return { applied: inputData.draft };
    },
  });
  const publish = step({
    id: 'publish',
    inputSchema: z.object({ applied: z.string() }),
    outputSchema: z.object({ published: z.string() }),
    execute: async ({ inputData }) => {
      seen.publish.push(inputData.applied);
      publishStarted.resolve();
      if (behaviour.publish === 'block') await never;
      return { published: inputData.applied };
    },
  });
  const workflow = workflowOf({
    id: 'checkpoint-flow',
    inputSchema: z.object({ draft: z.string() }),
    outputSchema: z.object({ published: z.string() }),
  })
    .then(review)
    .then(apply)
    .then(publish)
    .commit();
  return {
    workflow,
    seen,
    started: { review: reviewResumed.promise, apply: applyStarted.promise, publish: publishStarted.promise },
  };
}

async function host(engine: Engine, storage: MastraStorage, behaviour: HostBehaviour) {
  const h = hostWorkflow(engine, behaviour);
  const mastra = new Mastra({
    logger: false,
    storage,
    workflows: { 'checkpoint-flow': h.workflow },
    ...(engine === 'evented' ? { pubsub: new EventEmitterPubSub() } : {}),
  });
  if (engine === 'evented') await mastra.startWorkers();
  return { ...h, mastra };
}

/**
 * What a dead process leaves behind: it processes nothing more, and its lease
 * stops being renewed and lapses.
 */
async function crash(storage: MastraStorage, runId: string, dead?: Mastra) {
  if (dead) await dead.stopWorkers();
  const store = (await storage.getStore('workflows'))!;
  const owner = await store.getRunOwnership({ runId });
  if (owner?.live) {
    await store.releaseRunOwnership({ runId, generation: owner.generation, ownerId: owner.ownerId });
  }
}

async function load(storage: MastraStorage, runId: string) {
  const store = (await storage.getStore('workflows'))!;
  return store.loadWorkflowSnapshot({ workflowName: 'checkpoint-flow', runId });
}

describe('restart() after a crash during a resume (PF-7806)', () => {
  const approve = { decision: 'approve' as const };
  const revise = { decision: 'revise' as const, draft: 'v2-edited' };
  // The default engine keeps the resumed step suspended in storage until the run suspends
  // or finishes, so a crash in a later step (`apply`) strands the answer too. The evented
  // engine settles each step as it finishes, so only a crash inside the resumed step does.
  it.each([
    ['default', 'apply', approve, 'v1'],
    ['default', 'apply', revise, 'v2-edited'],
    ['default', 'review', revise, 'v2-edited'],
    ['evented', 'review', approve, 'v1'],
    ['evented', 'review', revise, 'v2-edited'],
  ] as const)(
    '%s engine, crash in %s: another host continues the accepted answer %o once',
    async (engine, crashStep, answer, expectedDraft) => {
      const storage = new MockStore();

      // Host A suspends at the checkpoint, accepts the answer, then dies in `crashStep`.
      const a = await host(engine, storage, {
        review: crashStep === 'review' ? 'block' : 'run',
        apply: crashStep === 'apply' ? 'block' : 'run',
        publish: 'run',
      });
      const runA = await a.workflow.createRun();
      expect((await runA.start({ inputData: { draft: 'v1' } })).status).toBe('suspended');
      void runA.resume({ step: 'review', resumeData: answer });
      await a.started[crashStep];

      const stranded = await load(storage, runA.runId);
      expect(stranded?.status).toBe('running');
      expect(stranded?.context.review).toMatchObject({ status: 'suspended', suspendPayload: { draft: 'v1' } });
      await crash(storage, runA.runId, a.mastra);

      // Host B restarts the run: the answer is applied, and B dies inside `publish`.
      const b = await host(engine, storage, { review: 'run', apply: 'run', publish: 'block' });
      void (await b.workflow.createRun({ runId: runA.runId })).restart();
      await b.started.publish;
      expect(b.seen.review).toEqual([answer]);
      expect(b.seen.apply).toEqual([expectedDraft]);
      expect(b.seen.publish).toEqual([expectedDraft]);
      await crash(storage, runA.runId, b.mastra);

      // Host C restarts again: the continued resume already settled, so nothing asks again
      // and the answer is not applied a second time.
      const c = await host(engine, storage, { review: 'run', apply: 'run', publish: 'run' });
      const result = await (await c.workflow.createRun({ runId: runA.runId })).restart();
      if (engine === 'evented') await c.mastra.stopWorkers();
      expect(c.seen.review).toEqual([]);
      expect(c.seen.apply).toEqual([]);
      expect(c.seen.publish).toEqual([expectedDraft]);
      expect(result.status).toBe('success');
      if (result.status === 'success') expect(result.result).toEqual({ published: expectedDraft });
    },
  );

  it('re-executes a run that crashed before it ever suspended', async () => {
    const storage = new MockStore();
    const started = deferred();
    const never = new Promise<never>(() => {});
    const build = (block: boolean, seen: unknown[]) => {
      const prepare = createStep({
        id: 'prepare',
        inputSchema: z.object({ draft: z.string() }),
        outputSchema: z.object({ draft: z.string() }),
        execute: async ({ inputData }) => {
          seen.push(inputData.draft);
          started.resolve();
          if (block) await never;
          return inputData;
        },
      });
      const gate = createStep({
        id: 'gate',
        inputSchema: z.object({ draft: z.string() }),
        outputSchema: z.object({ draft: z.string() }),
        resumeSchema: z.object({ ok: z.boolean() }),
        execute: async ({ inputData, resumeData, suspend }) => (resumeData ? inputData : suspend({})),
      });
      const workflow = createWorkflow({
        id: 'prepare-flow',
        inputSchema: z.object({ draft: z.string() }),
        outputSchema: z.object({ draft: z.string() }),
      })
        .then(prepare)
        .then(gate)
        .commit();
      new Mastra({ logger: false, storage, workflows: { 'prepare-flow': workflow } });
      return workflow;
    };

    const seenA: unknown[] = [];
    const runA = await build(true, seenA).createRun();
    void runA.start({ inputData: { draft: 'v1' } });
    await started.promise;
    await crash(storage, runA.runId);

    const seenB: unknown[] = [];
    const restarted = await (await build(false, seenB).createRun({ runId: runA.runId })).restart();
    expect(seenB).toEqual(['v1']);
    expect(restarted.status).toBe('suspended');
  });
});

describe('restart() after a crash during a nested resume (PF-7806)', () => {
  it('continues the nested step with the accepted resume data', async () => {
    const storage = new MockStore();
    const build = (blockAfter: boolean) => {
      const seen = { gate: [] as unknown[], after: [] as unknown[] };
      const afterStarted = deferred();
      const never = new Promise<never>(() => {});
      const gate = createStep({
        id: 'gate',
        inputSchema: z.object({}),
        outputSchema: z.object({ approved: z.boolean(), note: z.string() }),
        resumeSchema: z.object({ approved: z.boolean(), note: z.string() }),
        execute: async ({ resumeData, suspend }) => {
          seen.gate.push(resumeData);
          if (resumeData === undefined) return suspend({});
          return resumeData;
        },
      });
      const after = createStep({
        id: 'after',
        inputSchema: z.object({ approved: z.boolean(), note: z.string() }),
        outputSchema: z.object({ approved: z.boolean(), note: z.string() }),
        execute: async ({ inputData }) => {
          seen.after.push(inputData);
          afterStarted.resolve();
          if (blockAfter) await never;
          return inputData;
        },
      });
      const nested = createWorkflow({
        id: 'nested',
        inputSchema: z.object({}),
        outputSchema: z.object({ approved: z.boolean(), note: z.string() }),
      })
        .then(gate)
        .then(after)
        .commit();
      const parent = createWorkflow({
        id: 'parent',
        inputSchema: z.object({}),
        outputSchema: z.object({ approved: z.boolean(), note: z.string() }),
      })
        .then(nested)
        .commit();
      new Mastra({ logger: false, storage, workflows: { parent, nested } });
      return { parent, seen, afterStarted: afterStarted.promise };
    };

    const a = build(true);
    const runA = await a.parent.createRun();
    expect((await runA.start({ inputData: {} })).status).toBe('suspended');
    void runA.resume({ step: ['nested', 'gate'], resumeData: { approved: false, note: 'edited' } });
    await a.afterStarted;
    await crash(storage, runA.runId);

    const b = build(false);
    const result = await (await b.parent.createRun({ runId: runA.runId })).restart();
    expect(b.seen.gate).toEqual([{ approved: false, note: 'edited' }]);
    expect(b.seen.after).toEqual([{ approved: false, note: 'edited' }]);
    expect(result.status).toBe('success');
    if (result.status === 'success') expect(result.result).toEqual({ approved: false, note: 'edited' });
  });
});

describe('restart() after a crash during a foreach iteration resume (PF-7806)', () => {
  it('continues only the resumed iteration with its accepted data', async () => {
    const storage = new MockStore();
    const build = (blockOnResume: boolean) => {
      const seen: Array<{ name: string; approved: boolean }> = [];
      const resumed = deferred();
      const never = new Promise<never>(() => {});
      const approval = createStep({
        id: 'approval',
        inputSchema: z.object({ name: z.string() }),
        outputSchema: z.object({ name: z.string(), approved: z.boolean() }),
        resumeSchema: z.object({ approved: z.boolean() }),
        execute: async ({ inputData, resumeData, suspend }) => {
          if (!resumeData) return suspend({});
          seen.push({ name: inputData.name, approved: resumeData.approved });
          resumed.resolve();
          if (blockOnResume) await never;
          return { name: inputData.name, approved: resumeData.approved };
        },
      });
      const workflow = createWorkflow({
        id: 'foreach-flow',
        inputSchema: z.array(z.object({ name: z.string() })),
        outputSchema: z.array(z.object({ name: z.string(), approved: z.boolean() })),
        options: { validateInputs: false },
      })
        .foreach(approval, { concurrency: 2 })
        .commit();
      new Mastra({ logger: false, storage, workflows: { 'foreach-flow': workflow } });
      return { workflow, seen, resumed: resumed.promise };
    };

    const a = build(true);
    const runA = await a.workflow.createRun();
    expect((await runA.start({ inputData: [{ name: 'alpha' }, { name: 'beta' }] })).status).toBe('suspended');
    void runA.resume({ forEachIndex: 1, resumeData: { approved: false } });
    await a.resumed;
    await crash(storage, runA.runId);

    const b = build(false);
    const runB = await b.workflow.createRun({ runId: runA.runId });
    const restarted = await runB.restart();
    expect(b.seen).toEqual([{ name: 'beta', approved: false }]);
    expect(restarted.status).toBe('suspended');

    const finished = await runB.resume({ forEachIndex: 0, resumeData: { approved: true } });
    expect(b.seen).toEqual([
      { name: 'beta', approved: false },
      { name: 'alpha', approved: true },
    ]);
    expect(finished.status).toBe('success');
    if (finished.status === 'success') {
      expect(finished.result).toEqual([
        { name: 'alpha', approved: true },
        { name: 'beta', approved: false },
      ]);
    }
  });
});
