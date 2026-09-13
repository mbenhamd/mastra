import type { Event } from '@mastra/core/events';
import { Mastra } from '@mastra/core/mastra';
import { MockStore } from '@mastra/core/storage';
import { Inngest } from 'inngest';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { z } from 'zod';
import { InngestPubSub } from './pubsub';
import { init } from './index';

afterEach(() => vi.restoreAllMocks());

describe('nested workflow writer bubbling', () => {
  it.each([1, 2])('forwards write and custom chunks through %i nested invocations', async depth => {
    const inngest = new Inngest({ id: 'nested-writer-test' });
    // Replace only Inngest transport boundaries: each invoke still executes the
    // real workflow handler and engine with a serialized event payload.
    const handlers = new Map<string, (context: any) => Promise<any>>();
    vi.spyOn(inngest, 'createFunction').mockImplementation((config: any, handler: any) => {
      handlers.set(config.id, handler);
      return { id: config.id } as any;
    });
    const realtimeHandlers = new Map<string, (message: any) => Promise<void> | void>();
    const realtimeSubscribe = vi.fn(async ({ channel, topics, onMessage }: any) => {
      realtimeHandlers.set(`${channel}:${topics[0]}`, onMessage);
      return { close: vi.fn() };
    });
    const parentPubsub = new InngestPubSub(inngest, 'parent', realtimeSubscribe as any);
    const parentEvents: Event[] = [];
    await parentPubsub.subscribe('workflow.events.v2.parent-run', event => {
      parentEvents.push(event);
    });
    const childPubsub = new InngestPubSub(inngest, 'child', realtimeSubscribe as any);
    const childEvents: Event[] = [];
    let childSubscription: Promise<void> | undefined;
    vi.spyOn(inngest.realtime, 'publish').mockImplementation(async (topicRef: any, data: any) => {
      await realtimeHandlers.get(`${topicRef.channel}:${topicRef.topic}`)?.({ data });
    });
    const { createWorkflow, createStep } = init(inngest);
    const schema = z.object({ value: z.string() });
    let receivedWhileRunning = false;
    const leaf = createStep({
      id: 'leaf',
      inputSchema: schema,
      outputSchema: schema,
      execute: async ({ inputData, writer }) => {
        await writer.write({ message: 'nested-write' });
        await writer.custom({ type: 'custom-status', message: 'nested-custom' });
        // Chunks must reach the parent before the child returns its result.
        const parentChunks = parentEvents.map(event => event.data as any);
        receivedWhileRunning =
          parentChunks.some(data => data.payload?.output?.message === 'nested-write') &&
          parentChunks.some(data => data.message === 'nested-custom');
        return inputData;
      },
    });
    let child = createWorkflow({ id: 'child', inputSchema: schema, outputSchema: schema }).then(leaf).commit();
    if (depth === 2) {
      child = createWorkflow({ id: 'middle', inputSchema: schema, outputSchema: schema }).then(child).commit();
    }
    const parent = createWorkflow({ id: 'parent', inputSchema: schema, outputSchema: schema }).then(child).commit();
    const mastra = new Mastra({ logger: false, storage: new MockStore(), workflows: { parent } });
    parent.__registerMastra(mastra);
    parent.getFunction();
    const step = {
      run: async (_id: string, callback: () => Promise<unknown>) => callback(),
      invoke: async (_id: string, { function: fn, data }: any) => {
        if (fn.id === 'workflow.child' && !childSubscription) {
          childSubscription = childPubsub.subscribe(`workflow.events.v2.${data.runId}`, event => {
            childEvents.push(event);
          });
          await childSubscription;
        }
        return handlers.get(fn.id)!({ event: { data: JSON.parse(JSON.stringify(data)) }, step, attempt: 0 });
      },
      sendEvent: vi.fn(),
    };
    const result = await handlers.get('workflow.parent')!({
      event: { data: { runId: 'parent-run', inputData: { value: 'ok' } } },
      step,
      attempt: 0,
    });
    expect(result.result.status).toBe('success');
    expect(receivedWhileRunning).toBe(true);
    const chunks = parentEvents.map(event => event.data as any);
    expect(chunks.filter(chunk => chunk.payload?.output?.message === 'nested-write')).toHaveLength(1);
    expect(chunks.filter(chunk => chunk.message === 'nested-custom')).toHaveLength(1);
    expect(chunks.filter(chunk => chunk.type === 'workflow-step-finish' && chunk.payload.id === child.id)).toHaveLength(
      1,
    );
    expect(childEvents.some(event => (event.data as any).message === 'nested-custom')).toBe(true);
  });
});
