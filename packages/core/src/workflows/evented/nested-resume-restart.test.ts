/**
 * Regression for https://github.com/mastra-ai/mastra/issues/25365 (evented engine).
 *
 * A process that dies while a resumed nested-workflow step is running must be
 * recoverable via restart(): the resumed step re-runs with its original resume
 * data instead of suspending again.
 *
 * PF-5928 adaptation (fork PF-2216, the same semantic on both engines): the
 * fork never records a resumed step as `running` with its resume payload before
 * it runs, so there is no payload for restart() to replay. A resumed step keeps
 * its suspended record until it settles. After a crash mid-resume, restart()
 * fails a nested run without running the resumed step, and a single-step
 * workflow stays suspended so a new resume() finishes it — exactly as on the
 * default engine (workflows/nested-resume-restart.test.ts,
 * workflows/resume-restart-suspend-data.test.ts, nested-resume-pf2216.test.ts).
 */

import { describe, expect, it } from 'vitest';
import { z } from 'zod/v4';
import { EventEmitterPubSub } from '../../events/event-emitter';
import { Mastra } from '../../mastra';
import { MockStore } from '../../storage/mock';
import { createStep, createWorkflow } from '.';

function build(onResume: (data: any) => Promise<void>) {
  // A step before the gate, so the gate's execution path is not the [0] default.
  const pre = createStep({
    id: 'pre',
    inputSchema: z.object({}),
    outputSchema: z.object({}),
    execute: async () => ({}),
  });
  const gate = createStep({
    id: 'gate',
    inputSchema: z.object({}),
    outputSchema: z.object({ approved: z.boolean() }),
    resumeSchema: z.boolean(),
    execute: async ({ resumeData, suspend }) => {
      if (resumeData === undefined) {
        return suspend({});
      }
      await onResume(resumeData);
      return { approved: resumeData };
    },
  });
  const nested = createWorkflow({
    id: 'nested',
    inputSchema: z.object({}),
    outputSchema: z.object({ approved: z.boolean() }),
  })
    .then(pre)
    .then(gate)
    .commit();
  const parent = createWorkflow({
    id: 'parent',
    inputSchema: z.object({}),
    outputSchema: z.object({ approved: z.boolean() }),
  })
    .then(nested)
    .commit();
  return { parent, nested };
}

async function makeHost(storage: MockStore, p: ReturnType<typeof build>) {
  const mastra = new Mastra({
    logger: false,
    storage,
    pubsub: new EventEmitterPubSub(),
    workflows: { parent: p.parent, nested: p.nested },
  });
  await mastra.startWorkers();
  return mastra;
}

async function crashMidResume() {
  const storage = new MockStore();
  let markRunning!: () => void;
  const running = new Promise<void>(r => (markRunning = r));
  const p = build(async () => {
    markRunning();
    await new Promise(() => {}); // process "dies" here
  });
  const mastra = await makeHost(storage, p);
  const run = await p.parent.createRun();
  expect((await run.start({ inputData: {} })).status).toBe('suspended');

  const store = (await storage.getStore('workflows'))!;
  const parentBefore = await store.loadWorkflowSnapshot({ workflowName: 'parent', runId: run.runId });
  const nestedRunId = (parentBefore?.context as any).nested.metadata.nestedRunId as string;
  const suspendedNested = JSON.parse(
    JSON.stringify(await store.loadWorkflowSnapshot({ workflowName: 'nested', runId: nestedRunId })),
  );

  void run.resume({ step: ['nested', 'gate'], resumeData: false });
  await running;

  const parentSnapshot = JSON.parse(
    JSON.stringify(await store.loadWorkflowSnapshot({ workflowName: 'parent', runId: run.runId })),
  );
  const nestedSnapshot = JSON.parse(
    JSON.stringify(await store.loadWorkflowSnapshot({ workflowName: 'nested', runId: nestedRunId })),
  );
  await mastra.stopWorkers();
  return { runId: run.runId, nestedRunId, parentSnapshot, nestedSnapshot, suspendedNested };
}

async function restartOnFreshHost(runId: string, nestedRunId: string, parentSnapshot: any, nestedSnapshot: any) {
  const storage = new MockStore();
  const store = (await storage.getStore('workflows'))!;
  await store.persistWorkflowSnapshot({ workflowName: 'parent', runId, snapshot: parentSnapshot });
  await store.persistWorkflowSnapshot({ workflowName: 'nested', runId: nestedRunId, snapshot: nestedSnapshot });

  const seen: any[] = [];
  const p = build(async data => {
    seen.push(data);
  });
  const mastra = await makeHost(storage, p);
  try {
    const result = await (await p.parent.createRun({ runId })).restart();
    const nestedAfter = await store.loadWorkflowSnapshot({ workflowName: 'nested', runId: nestedRunId });
    return { result, seen, nestedAfter };
  } finally {
    await mastra.stopWorkers();
  }
}

describe('evented nested workflow restart after crash during resume (issue #25365, PF-2216 semantics)', () => {
  it('keeps the resumed step records suspended and records no resume payload ahead of execution', async () => {
    const { parentSnapshot, nestedSnapshot } = await crashMidResume();
    expect(parentSnapshot.status).toBe('running');
    expect(parentSnapshot.context.nested).toMatchObject({ status: 'suspended' });
    expect(parentSnapshot.context.nested.resumePayload).toBeUndefined();
    expect(parentSnapshot.context.nested.suspendPayload.__workflow_meta.runId).toBeTruthy();
    expect(nestedSnapshot.context.gate).toMatchObject({ status: 'suspended' });
    expect(nestedSnapshot.context.gate.resumePayload).toBeUndefined();
  });

  it.each([
    ['as the crash left it', undefined],
    ['before it claimed the resume', 'suspended'],
    // Not covered here: a nested row `running` with cleared paths (upstream's claim shape). The
    // fork's evented resume never writes it (the crash above leaves the nested row suspended), and
    // a restart over that synthetic state currently never settles (PF-5928 open item: the nested
    // workflow.start goes silent, the same family as the evented recover() hang).
  ] as const)(
    'fails the restart without running the resumed nested step when the nested run is %s',
    async (_, nestedStatus) => {
      const { runId, nestedRunId, parentSnapshot, nestedSnapshot, suspendedNested } = await crashMidResume();
      const { result, seen } = await restartOnFreshHost(
        runId,
        nestedRunId,
        parentSnapshot,
        nestedStatus === undefined
          ? nestedSnapshot
          : {
              ...suspendedNested,
              status: nestedStatus,
              ...(nestedStatus === 'running' ? { suspendedPaths: {}, activeStepsPath: {} } : {}),
            },
      );

      expect(result.status).toBe('failed');
      expect(seen).toEqual([]);
    },
  );

  it('keeps a single-step workflow suspended through a crash mid-resume so a new resume() finishes it', async () => {
    const buildSingle = (onResume: (data: any) => Promise<void>) => {
      const ask = createStep({
        id: 'ask',
        inputSchema: z.object({}),
        outputSchema: z.object({ answer: z.string() }),
        resumeSchema: z.object({ answer: z.string() }),
        execute: async ({ resumeData, suspend }) => {
          if (resumeData === undefined) return suspend({});
          await onResume(resumeData);
          return { answer: resumeData.answer };
        },
      });
      const parent = createWorkflow({ id: 'parent', inputSchema: z.object({}), outputSchema: z.any() })
        .then(ask)
        .commit();
      return { parent, nested: parent } as unknown as ReturnType<typeof build>;
    };
    const storage = new MockStore();
    let markRunning!: () => void;
    const running = new Promise<void>(r => (markRunning = r));
    const p1 = buildSingle(async () => {
      markRunning();
      await new Promise(() => {}); // the process "dies" inside the resumed step
    });
    const mastra1 = await makeHost(storage, p1);
    const run = await p1.parent.createRun();
    expect((await run.start({ inputData: {} })).status).toBe('suspended');
    void run.resume({ step: 'ask', resumeData: { answer: 'blue' } });
    await running;
    const store = (await storage.getStore('workflows'))!;
    const crashed = JSON.parse(
      JSON.stringify(await store.loadWorkflowSnapshot({ workflowName: 'parent', runId: run.runId })),
    );
    await mastra1.stopWorkers();
    expect(crashed.context.ask).toMatchObject({ status: 'suspended' });

    const storage2 = new MockStore();
    const store2 = (await storage2.getStore('workflows'))!;
    await store2.persistWorkflowSnapshot({ workflowName: 'parent', runId: run.runId, snapshot: crashed });
    const seen: any[] = [];
    const p2 = buildSingle(async data => {
      seen.push(data);
    });
    const mastra2 = await makeHost(storage2, p2);
    try {
      const run2 = await p2.parent.createRun({ runId: run.runId });
      const restarted = await run2.restart();
      expect(restarted.status).toBe('suspended');
      expect(seen).toEqual([]);
      const resumed = await run2.resume({ step: 'ask', resumeData: { answer: 'blue' } });
      expect(resumed.status).toBe('success');
      expect(seen).toEqual([{ answer: 'blue' }]);
    } finally {
      await mastra2.stopWorkers();
    }
  });
});
