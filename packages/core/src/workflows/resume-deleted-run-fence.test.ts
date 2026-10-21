import { describe, expect, it } from 'vitest';
import { z } from 'zod/v4';
import { Mastra } from '../mastra';
import { InMemoryStore } from '../storage/mock';
import { createWorkflow } from './create';
import { createStep } from './workflow';

/**
 * PF-6186: a lifetime-bound step update that finds its run deleted
 * (`missing_run`) may only create the row for the run's first admission
 * (`Run.start`). A resume, even in a fresh process whose engine has never
 * persisted the run, must never recreate a run that erasure deleted.
 */
describe('a resumed run deleted while its step executes', () => {
  function setup() {
    const storage = new InMemoryStore();
    let duringResume: (() => Promise<void>) | undefined;
    // Persists only suspensions, so the run has no row before its first
    // suspended state: that first-admission write must still create it.
    const build = () => {
      const step = createStep({
        id: 'ask',
        inputSchema: z.object({}),
        outputSchema: z.object({ answer: z.number() }),
        resumeSchema: z.object({ answer: z.number() }),
        suspendSchema: z.object({}),
        execute: async ({ resumeData, suspend }) => {
          if (resumeData) await duringResume?.();
          if (!resumeData || resumeData.answer < 2) return await suspend({});
          return { answer: resumeData.answer };
        },
      });
      const workflow = createWorkflow({
        id: 'suspend-only',
        inputSchema: z.object({}),
        outputSchema: z.object({ answer: z.number() }),
        options: { shouldPersistSnapshot: ({ workflowStatus }) => workflowStatus === 'suspended' },
      })
        .then(step)
        .commit();
      return new Mastra({ storage, workflows: { workflow }, logger: false });
    };
    const row = async () =>
      (await storage.getStore('workflows'))!.getWorkflowRunById({ runId: 'run-1', workflowName: 'suspend-only' });
    return {
      build,
      row,
      deleteDuringResume: () => {
        duringResume = async () => {
          await (await storage.getStore('workflows'))!.deleteWorkflowRunById({
            runId: 'run-1',
            workflowName: 'suspend-only',
          });
        };
      },
    };
  }

  it('creates the row on the first admission of a suspend-only run', async () => {
    const { build, row } = setup();
    const run = await build().getWorkflow('workflow').createRun({ runId: 'run-1' });
    await expect(run.start({ inputData: {} })).resolves.toMatchObject({ status: 'suspended' });
    expect(await row()).not.toBeNull();
  });

  it('never recreates the run from a resume in a fresh process', async () => {
    const { build, row, deleteDuringResume } = setup();
    const first = await build().getWorkflow('workflow').createRun({ runId: 'run-1' });
    await expect(first.start({ inputData: {} })).resolves.toMatchObject({ status: 'suspended' });

    // Another process resumes the run; erasure deletes it while the step runs,
    // and the step then suspends again.
    deleteDuringResume();
    const resumed = await build().getWorkflow('workflow').createRun({ runId: 'run-1' });
    await resumed.resume({ step: 'ask', resumeData: { answer: 1 } }).catch(() => undefined);

    expect(await row()).toBeNull();
  });
});
