/**
 * Fork PF-2216 contract for resumed nested steps (PF-5928 sync).
 *
 * Upstream #25234/#27012 record the resumed step as `running` with its resume
 * payload before it runs, so restart() can replay it after a crash. The fork's
 * ordinary resume persistence (PF-2216) refuses a `running` write over the
 * suspended row: the suspended snapshot stays authoritative until the resumed
 * step settles, and the resume payload reaches the step through the resume
 * call itself. A crash mid-resume therefore leaves a run that a new resume()
 * finishes with the payload it carries — including falsy payloads.
 */

import { describe, expect, it } from 'vitest';
import { z } from 'zod/v4';
import { Mastra } from '../mastra';
import { MockStore } from '../storage/mock';
import { createWorkflow } from './create';
import { createStep } from './workflow';

function build(onResume: (data: unknown) => Promise<void>) {
  const gate = createStep({
    id: 'gate',
    inputSchema: z.object({}),
    outputSchema: z.object({ approved: z.boolean() }),
    resumeSchema: z.boolean(),
    execute: async ({ resumeData, suspend }) => {
      if (resumeData === undefined) return suspend({});
      await onResume(resumeData);
      return { approved: resumeData };
    },
  });
  const nested = createWorkflow({
    id: 'nested',
    inputSchema: z.object({}),
    outputSchema: z.object({ approved: z.boolean() }),
  })
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

describe('PF-2216 resumed nested step payload', () => {
  it('delivers a falsy resume payload to the resumed nested step', async () => {
    const storage = new MockStore();
    const seen: unknown[] = [];
    const wf = build(async data => {
      seen.push(data);
    });
    new Mastra({ logger: false, storage, workflows: { parent: wf.parent, nested: wf.nested } });
    const run = await wf.parent.createRun();
    expect((await run.start({ inputData: {} })).status).toBe('suspended');

    const result = await run.resume({ step: ['nested', 'gate'], resumeData: false });

    expect(result.status).toBe('success');
    expect(seen).toEqual([false]);
    if (result.status === 'success') expect(result.result).toEqual({ approved: false });
  });

  it('never runs the resumed nested step without its payload after a crash mid-resume', async () => {
    // The fork records no resume payload on the step before it runs (PF-2216), so restart()
    // has nothing to replay. It must fail the run rather than run the resumed step with a
    // missing payload. (Fork e268d40 fails it with WORKFLOW_RUN_NOT_SUSPENDED; the merged
    // restart path reaches the nested step and its resumeSchema rejects the missing payload.)
    const storage1 = new MockStore();
    let markRunning!: () => void;
    const running = new Promise<void>(resolve => (markRunning = resolve));
    const p1 = build(async () => {
      markRunning();
      await new Promise(() => {}); // the process "dies" inside the resumed step
    });
    new Mastra({ logger: false, storage: storage1, workflows: { parent: p1.parent, nested: p1.nested } });
    const run1 = await p1.parent.createRun();
    expect((await run1.start({ inputData: {} })).status).toBe('suspended');
    void run1.resume({ step: ['nested', 'gate'], resumeData: false });
    await running;

    const store1 = (await storage1.getStore('workflows'))!;
    const storage2 = new MockStore();
    const store2 = (await storage2.getStore('workflows'))!;
    const parentSnapshot = await store1.loadWorkflowSnapshot({ workflowName: 'parent', runId: run1.runId });
    // PF-2216: the claimed run is `running`, but the resumed step keeps its suspended record
    // and no resume payload is written ahead of execution.
    expect(parentSnapshot?.status).toBe('running');
    expect(parentSnapshot?.context.nested).toMatchObject({ status: 'suspended' });
    expect((parentSnapshot?.context.nested as { resumePayload?: unknown }).resumePayload).toBeUndefined();
    for (const name of ['parent', 'nested']) {
      const snapshot = await store1.loadWorkflowSnapshot({ workflowName: name, runId: run1.runId });
      await store2.persistWorkflowSnapshot({
        workflowName: name,
        runId: run1.runId,
        snapshot: JSON.parse(JSON.stringify(snapshot)),
      });
    }

    const seen: unknown[] = [];
    const p2 = build(async data => {
      seen.push(data);
    });
    new Mastra({ logger: false, storage: storage2, workflows: { parent: p2.parent, nested: p2.nested } });
    const run2 = await p2.parent.createRun({ runId: run1.runId });
    const restarted = await run2.restart();

    expect(restarted.status).toBe('failed');
    expect(seen).toEqual([]);
  });
});
