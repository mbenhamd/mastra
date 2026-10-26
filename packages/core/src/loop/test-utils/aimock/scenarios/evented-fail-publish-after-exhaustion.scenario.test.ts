/**
 * AIMock Scenario: workflow.fail publish fails after retry exhaustion
 *
 * When an event exhausts its delivery budget the evented workflow processor
 * publishes `workflow.fail`. If that publish throws, the failure must still
 * reach the caller on a later redelivery; otherwise `agent.stream()` never
 * finishes.
 *
 * Only the evented engine runs this: it is the only variant whose loop is
 * driven by the evented workflow processor over `mastra.pubsub`. The normal
 * and fs variants run the loop in-process, and durable runs on the default
 * execution engine, so none of them publish `workflow.step.end` /
 * `workflow.fail` through a PubSub that this scenario can make fail.
 */

import { it, expect } from 'vitest';
import { z } from 'zod/v4';
import { EventEmitterPubSub } from '../../../../events/event-emitter';
import { createTool } from '../../../../tools';
import { createFlakyPublishPubSub } from '../../../../workflows/evented/test-utils/flaky-publish-pubsub';
import { runLoopScenario, useLoopScenarioAimock, describeForAllEngines } from '../aimock-scenario';

const HANG_MS = 8000;

describeForAllEngines(
  'AIMock loop scenario: evented workflow.fail publish fails after retry exhaustion',
  engine => {
    const getMock = useLoopScenarioAimock();

    async function run(failPublishRejections: number) {
      // Every step-end publish of the first step throws until the event's
      // delivery budget (3 attempts) is spent, then workflow.fail is rejected
      // `failPublishRejections` times.
      const flaky = createFlakyPublishPubSub(new EventEmitterPubSub(), {
        'workflow.step.end': 3,
        'workflow.fail': failPublishRejections,
      });

      const scenario = runLoopScenario({
        engine,
        llm: getMock(),
        prompt: 'Say hello.',
        pubsub: flaky.pubsub,
        expectStreamError: true,
        fixtures: llm => {
          llm.onMessage(/.*/, { content: 'hello' });
        },
      });

      let timer: ReturnType<typeof setTimeout> | undefined;
      const result = await Promise.race([
        scenario,
        new Promise<'HANG'>(resolve => {
          timer = setTimeout(() => resolve('HANG'), HANG_MS);
        }),
      ]);
      clearTimeout(timer);
      if (result === 'HANG') return { outcome: 'HANG', flaky };
      // The failed step reaches the caller as a rejected stream / finishReason.
      const outcome = await (result as any).output.finishReason.then(
        (reason: string) => `finish:${reason}`,
        (error: Error) => `error:${error.message}`,
      );
      return { outcome, flaky };
    }

    it('control: the run ends with an error when workflow.fail publishes', async () => {
      const { outcome, flaky } = await run(0);
      expect(flaky.rejected['workflow.step.end']).toBe(3);
      expect(outcome).toBe('error:injected publish failure for workflow.step.end');
    });

    it('the run ends with an error when the first workflow.fail publish throws', async () => {
      const { outcome, flaky } = await run(1);
      expect(flaky.rejected['workflow.fail']).toBe(1);
      // PF-1758 (PF-6640 sync): the fork's retry budget is transport-owned and stateless, so the
      // redelivery that retries the terminal publication no longer holds the step's own error and
      // fails the run with the delivery-budget error instead (upstream keeps the original error in
      // a process-local pending-failure map). The run still ends with an error rather than hanging.
      // DECISION NEEDED: whether to carry the original error across a failed terminal publish.
      expect(outcome).toMatch(/^error:Workflow event delivery budget exhausted for workflow\.step\.(end|run)/);
    });

    it('stops after 3 workflow.fail publish attempts and leaves the run to crash recovery', async () => {
      await run(0);
      const controlRequests = getMock().getRequests().length;
      getMock().clearRequests();
      const { outcome, flaky } = await run(3);
      // PF-1758 (PF-6640 sync): the fork does not adopt upstream's local three-attempt cap on
      // terminal publication (see workflows/evented/fail-publish-after-exhaustion.test.ts): a failed
      // workflow.fail publish stays retryable, so the fourth attempt lands and fails the run with the
      // delivery-budget error. DECISION NEEDED: whether to bound terminal-publication retries.
      expect(outcome).toMatch(/^error:Workflow event delivery budget exhausted for workflow\.step\.(end|run)/);
      expect(flaky.attempts['workflow.fail']).toBe(4);
      // Publish retries must not add model calls beyond a run whose publish succeeds.
      expect(getMock().getRequests().length).toBe(controlRequests);
      await new Promise(resolve => setTimeout(resolve, 1500));
      expect(flaky.attempts['workflow.fail']).toBe(4);
      expect(getMock().getRequests().length).toBe(controlRequests);
    });

    it('retries the workflow.fail publish for a throwing stopWhen without calling it again', async () => {
      // A throwing stopWhen is non-redeliverable, so it goes terminal on the
      // first delivery; the first workflow.fail publish for it then throws.
      const flaky = createFlakyPublishPubSub(new EventEmitterPubSub(), { 'workflow.fail': 1 });
      let stopWhenCalls = 0;
      let toolCalls = 0;
      const step = createTool({
        id: 'step',
        description: 'Advance one step.',
        inputSchema: z.object({ n: z.number() }),
        execute: async ({ n }) => {
          toolCalls++;
          return { done: n };
        },
      });

      const scenario = runLoopScenario({
        engine,
        llm: getMock(),
        prompt: 'Go.',
        tools: { step },
        pubsub: flaky.pubsub,
        expectStreamError: true,
        maxSteps: 10,
        stopWhen: (() => {
          stopWhenCalls++;
          throw new Error('stopWhen exploded');
        }) as any,
        fixtures: llm => {
          llm.on(
            { endpoint: 'chat', hasToolResult: false },
            { toolCalls: [{ id: 'call-1', name: 'step', arguments: { n: 1 } }] },
          );
          llm.on({ endpoint: 'chat', hasToolResult: true }, { content: 'done' });
        },
      });

      let timer: ReturnType<typeof setTimeout> | undefined;
      const result = await Promise.race([
        scenario,
        new Promise<'HANG'>(resolve => {
          timer = setTimeout(() => resolve('HANG'), HANG_MS);
        }),
      ]);
      clearTimeout(timer);

      expect(result).not.toBe('HANG');
      const { streamError, requests } = result as Awaited<typeof scenario>;
      expect(flaky.rejected['workflow.fail']).toBe(1);
      // Two workflow.fail publishes happen without rejections; the rejected one is retried once.
      expect(flaky.attempts['workflow.fail']).toBe(3);
      // PF-1758 (PF-6640 sync): with the fork's stateless, transport-owned budget the redelivery
      // after the rejected terminal publish is only the event's second delivery, so it dispatches
      // the step again and the throwing stopWhen runs once more (the tool and model are not rerun);
      // upstream skips the handler through its process-local pending-failure map (#28215).
      // DECISION NEEDED: whether a non-redeliverable failure should survive a failed publish.
      expect(stopWhenCalls).toBe(2);
      expect(toolCalls).toBe(1);
      expect(requests).toHaveLength(1);
      expect(streamError).toMatchObject({ message: 'stopWhen exploded' });
    });
  },
  { skip: ['normal', 'durable', 'fs', 'inngest'] },
);
