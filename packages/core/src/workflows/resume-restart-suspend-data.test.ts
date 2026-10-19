/**
 * Regression for https://github.com/mastra-ai/mastra/issues/27012
 *
 * A resumed step that is re-run after a crash must still receive the payload it suspended with.
 *
 * PF-5928 adaptation (fork PF-2216): the resumed step keeps its suspended record (with its
 * suspendPayload) until it settles, and no resume payload is recorded ahead of execution, so
 * restart() leaves the run suspended on that step (it never re-runs it without its resume
 * payload) and a new resume() finishes it with the suspendData intact. Payload
 * delivery through the fork resume path: nested-resume-pf2216.test.ts and lifecycle-events.test.ts.
 */

import { describe, expect, it } from 'vitest';
import { z } from 'zod/v4';
import { Mastra } from '../mastra';
import { MockStore } from '../storage/mock';
import { createWorkflow } from './create';
import { createStep } from './workflow';

function build(onResume: (args: { resumeData: unknown; suspendData: unknown }) => Promise<void>) {
  const ask = createStep({
    id: 'ask',
    inputSchema: z.object({}),
    outputSchema: z.object({ question: z.string(), answer: z.string() }),
    resumeSchema: z.object({ answer: z.string() }),
    suspendSchema: z.object({ question: z.string() }),
    execute: async ({ resumeData, suspendData, suspend }) => {
      if (resumeData === undefined) {
        return suspend({ question: 'which one?' });
      }
      await onResume({ resumeData, suspendData });
      return { question: suspendData?.question ?? 'missing', answer: resumeData.answer };
    },
  });
  return createWorkflow({
    id: 'ask-flow',
    inputSchema: z.object({}),
    outputSchema: z.object({ question: z.string(), answer: z.string() }),
  })
    .then(ask)
    .commit();
}

describe('resumed step restarted after a crash (issue #27012)', () => {
  it('keeps suspendData and the stored suspendPayload', async () => {
    const storage1 = new MockStore();
    let markRunning!: () => void;
    const running = new Promise<void>(r => (markRunning = r));
    const wf1 = build(async () => {
      markRunning();
      await new Promise(() => {}); // process "dies" here
    });
    new Mastra({ logger: false, storage: storage1, workflows: { 'ask-flow': wf1 } });

    const run1 = await wf1.createRun();
    expect((await run1.start({ inputData: {} })).status).toBe('suspended');
    void run1.resume({ step: 'ask', resumeData: { answer: 'the blue one' } });
    await running;

    const store1 = (await storage1.getStore('workflows'))!;
    const snapshot = await store1.loadWorkflowSnapshot({ workflowName: 'ask-flow', runId: run1.runId });
    expect(snapshot?.context.ask).toMatchObject({ status: 'suspended', suspendPayload: { question: 'which one?' } });

    const storage2 = new MockStore();
    const store2 = (await storage2.getStore('workflows'))!;
    await store2.persistWorkflowSnapshot({
      workflowName: 'ask-flow',
      runId: run1.runId,
      snapshot: JSON.parse(JSON.stringify(snapshot)),
    });

    const seen: any[] = [];
    const wf2 = build(async args => {
      seen.push(args);
    });
    new Mastra({ logger: false, storage: storage2, workflows: { 'ask-flow': wf2 } });

    const run2 = await wf2.createRun({ runId: run1.runId });
    const restarted = await run2.restart();

    expect(seen).toEqual([]);
    expect(restarted.status).toBe('suspended');

    const result = await run2.resume({ step: 'ask', resumeData: { answer: 'the blue one' } });
    expect(seen).toEqual([{ resumeData: { answer: 'the blue one' }, suspendData: { question: 'which one?' } }]);
    expect(result.status).toBe('success');
    if (result.status === 'success') {
      expect(result.result).toEqual({ question: 'which one?', answer: 'the blue one' });
    }
    const finalSnapshot = await store2.loadWorkflowSnapshot({ workflowName: 'ask-flow', runId: run1.runId });
    expect(finalSnapshot?.context.ask).not.toHaveProperty('suspendPayload');
  });
});
