import { describe, expect, it, vi } from 'vitest';
import { z } from 'zod/v4';
import { EventEmitterPubSub } from '../events/event-emitter';
import { Mastra } from '../mastra';
import { MockStore } from '../storage/mock';
import { DefaultExecutionEngine } from './default';
import { createStep, createWorkflow } from './index';

const ioSchema = z.object({ value: z.string() });

describe('step-result lifecycle fence', () => {
  it('does not poll snapshots after a fenced step persist on a 3-step run', async () => {
    const storage = new MockStore();
    const pubsub = new EventEmitterPubSub();
    const workflow = createWorkflow({
      id: 'pf-3750-three-step',
      inputSchema: ioSchema,
      outputSchema: ioSchema,
    })
      .then(
        createStep({
          id: 'one',
          inputSchema: ioSchema,
          outputSchema: ioSchema,
          execute: async ({ inputData }) => inputData,
        }),
      )
      .then(
        createStep({
          id: 'two',
          inputSchema: ioSchema,
          outputSchema: ioSchema,
          execute: async ({ inputData }) => inputData,
        }),
      )
      .then(
        createStep({
          id: 'three',
          inputSchema: ioSchema,
          outputSchema: ioSchema,
          execute: async ({ inputData }) => inputData,
        }),
      )
      .commit();

    const mastra = new Mastra({ logger: false, storage, pubsub, workflows: { [workflow.id]: workflow } });
    const authority = vi.spyOn(DefaultExecutionEngine.prototype, 'getAuthoritativeExecutionDisposition');
    const run = await workflow.createRun();

    const result = await run.start({ inputData: { value: 'ok' } });

    expect(result.status).toBe('success');
    expect(result.result).toEqual({ value: 'ok' });
    expect(authority).not.toHaveBeenCalled();
    await mastra.shutdown();
    authority.mockRestore();
  });
});
