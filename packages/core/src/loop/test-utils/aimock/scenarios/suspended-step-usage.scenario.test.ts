import { expect, it } from 'vitest';
import { z } from 'zod/v4';
import type { ChunkType } from '../../../../stream/types';
import { createTool } from '../../../../tools';
import {
  createSharedAgent,
  describeForAllEngines,
  isDurableEngineVariant,
  useLoopScenarioAimock,
} from '../aimock-scenario';

/**
 * Token usage when a tool call suspends (#27232).
 *
 * A run that stops on a tool calling `suspend()` or waiting for approval still
 * paid for the model step that produced the tool call. Users read that cost
 * from `usage`/`totalUsage` on the suspended result, and expect the resumed
 * run's `totalUsage` to be cumulative without counting the suspended step twice.
 *
 * Regression classes:
 * - Suspended stream()/generate() results drop the suspended step's tokens
 * - Resumed totalUsage omits the steps before the suspension
 * - Resumed totalUsage double-counts the suspended step
 * - A tool that suspends again on resume loses the suspended step's tokens
 */

type Usage = { inputTokens: number; outputTokens: number; totalTokens: number };

const PRIOR = { inputTokens: 7, outputTokens: 3, totalTokens: 10 };
const SUSPENDED = { inputTokens: 100, outputTokens: 20, totalTokens: 120 };
const RESUMED = { inputTokens: 50, outputTokens: 10, totalTokens: 60 };

const add = (...usages: Usage[]): Usage =>
  usages.reduce((a, u) => ({
    inputTokens: a.inputTokens + u.inputTokens,
    outputTokens: a.outputTokens + u.outputTokens,
    totalTokens: a.totalTokens + u.totalTokens,
  }));

const wire = (u: Usage) => ({ input_tokens: u.inputTokens, output_tokens: u.outputTokens });
const tokens = (u: any) => ({
  inputTokens: u?.inputTokens,
  outputTokens: u?.outputTokens,
  totalTokens: u?.totalTokens,
});

type Kind = 'suspend' | 'approval';

const askTool = createTool({
  id: 'ask',
  description: 'Ask a person',
  inputSchema: z.object({}),
  resumeSchema: z.object({ answer: z.string() }),
  execute: async (_input, { agent }) => {
    const answer = (agent?.resumeData as { answer?: string } | undefined)?.answer;
    if (!answer || answer === 'again') return agent!.suspend({ question: 'which one?' });
    return { answer };
  },
});

const approveTool = createTool({
  id: 'approve',
  description: 'Needs approval',
  inputSchema: z.object({}),
  requireApproval: true,
  execute: async () => ({ ok: true }),
});

const lookupTool = createTool({
  id: 'lookup',
  description: 'Plain tool',
  inputSchema: z.object({}),
  execute: async () => ({ found: true }),
});

type RunHandle = { output?: any; fullStream: AsyncIterable<ChunkType>; runId: string };

const settled = <T>(value: Promise<T> | T) =>
  Promise.race([Promise.resolve(value), new Promise<undefined>(r => setTimeout(() => r(undefined), 1000))]);

describeForAllEngines('AIMock loop scenario: token usage when a tool call suspends', engine => {
  const getMock = useLoopScenarioAimock();
  // PF-6758 / PF-5349 (PF-6640 sync): suspended-step usage keeps fork behaviour (decided in
  // PF-6758). The fork reports the suspended step's usage out of band
  // (`_recordPendingSuspendedStepUsage` on the output that ran the step), never on a
  // chunk. Upstream #27634/#27882/#28351 make durable, evented and Inngest callers read
  // it from the `tool-call-suspended` / `tool-call-approval` chunk, which the fork does
  // not carry, so a durable caller's output still has no usage for the suspending step
  // (as on fork main before this sync, upstream #27440). Resumed totals are correct.
  // Carrying the out-of-band usage across the durable stream adapter is not adopted (PF-6758).
  // The durable/evented tests below assert that specific gap (`usageGap`) instead of
  // expecting upstream's numbers. The Inngest-only test runs only on an external Inngest
  // engine host, where this gap has not been reproduced, so it stays `it.fails`.
  const durableGap = isDurableEngineVariant(engine);
  const itFailsOnDurable = durableGap ? it.fails : it;
  /** PF-6758 / PF-5349: what a durable/evented caller sees instead of the suspended step's usage. */
  const usageGap = { inputTokens: undefined, outputTokens: undefined, totalTokens: undefined };
  const suspendedUsage = (expected: Usage) => (durableGap ? usageGap : expected);

  async function setup(kind: Kind, { withPriorStep = false } = {}) {
    const llm = getMock();
    const toolName = kind === 'suspend' ? 'ask' : 'approve';
    const steps = [...(withPriorStep ? [{ toolName: 'lookup', usage: PRIOR }] : []), { toolName, usage: SUSPENDED }];
    steps.forEach((step, i) => {
      llm.on(
        { endpoint: 'chat', sequenceIndex: i },
        { toolCalls: [{ id: `call-${i + 1}`, name: step.toolName, arguments: {} }], usage: wire(step.usage) },
      );
    });
    llm.on({ endpoint: 'chat', sequenceIndex: steps.length }, { content: 'done', usage: wire(RESUMED) });
    llm.on({ endpoint: 'chat', sequenceIndex: steps.length + 1 }, { content: 'done', usage: wire(RESUMED) });

    const { agent } = await createSharedAgent(llm, {
      engine,
      instructions: 'use the tool',
      tools: { lookup: lookupTool, ...(kind === 'suspend' ? { ask: askTool } : { approve: approveTool }) },
    });
    return { agent: agent as any, toolCallId: `call-${steps.length}` };
  }

  // Durable streams stay open while suspended, so stop draining at the suspension.
  async function drain(run: RunHandle) {
    const output = run.output ?? run;
    for await (const chunk of run.fullStream) {
      if (chunk.type === 'tool-call-suspended' || chunk.type === 'tool-call-approval') break;
    }
    return output;
  }

  const resume = (agent: any, kind: Kind, runId: string, toolCallId: string, answer = 'blue'): Promise<RunHandle> =>
    kind === 'suspend' ? agent.resumeStream({ answer }, { runId }) : agent.approveToolCall({ runId, toolCallId });

  if (engine === 'inngest') {
    for (const kind of ['suspend', 'approval'] as const) {
      itFailsOnDurable(
        `[${kind}] Inngest forwards the suspending step's tokens through the tool-call boundary`,
        async () => {
          const { agent, toolCallId } = await setup(kind, { withPriorStep: true });
          const first: RunHandle = await agent.stream('go');
          const firstOutput = first.output ?? first;
          const heldUsage = firstOutput.usage;
          const heldTotalUsage = firstOutput.totalUsage;
          let suspension: ChunkType | undefined;
          for await (const chunk of first.fullStream) {
            if (chunk.type === 'tool-call-suspended' || chunk.type === 'tool-call-approval') {
              suspension = chunk;
              break;
            }
          }

          expect(suspension).toBeDefined();
          expect(tokens(suspension?.payload.usage)).toEqual(SUSPENDED);
          expect(tokens(await settled(heldUsage))).toEqual(add(PRIOR, SUSPENDED));
          expect(tokens(await settled(heldTotalUsage))).toEqual(add(PRIOR, SUSPENDED));

          const resumed = await drain(await resume(agent, kind, first.runId, toolCallId));
          expect(await resumed.text).toBe('done');
          expect(tokens(await resumed.totalUsage)).toEqual(add(PRIOR, SUSPENDED, RESUMED));
        },
      );
    }
  }

  for (const kind of ['suspend', 'approval'] as const) {
    it(`[${kind}] stream() reports the suspended step usage`, async () => {
      const { agent } = await setup(kind);
      const run = await agent.stream('go');
      const output = await drain(run);

      expect(tokens(await settled(output.usage))).toEqual(suspendedUsage(SUSPENDED));
      expect(tokens(await settled(output.totalUsage))).toEqual(suspendedUsage(SUSPENDED));
      expect(await output.finishReason).toBe('suspended');
    });

    it(`[${kind}] generate() reports the suspended step usage`, async () => {
      const { agent } = await setup(kind);
      const result = await agent.generate('go');

      expect(result.finishReason).toBe('suspended');
      expect(tokens(result.usage)).toEqual(suspendedUsage(SUSPENDED));
      expect(tokens(result.totalUsage)).toEqual(suspendedUsage(SUSPENDED));
    });

    it(`[${kind}] resumed totalUsage is cumulative and counts the suspended step once`, async () => {
      const { agent, toolCallId } = await setup(kind);
      const first: RunHandle = await agent.stream('go');
      await drain(first);

      const resumed = await drain(await resume(agent, kind, first.runId, toolCallId));
      expect(await resumed.text).toBe('done');
      expect(tokens(await resumed.totalUsage)).toEqual(add(SUSPENDED, RESUMED));
    });

    it(`[${kind}] exposes prior and suspended usage before resume, then counts the step once`, async () => {
      const { agent, toolCallId } = await setup(kind, { withPriorStep: true });
      const first: RunHandle = await agent.stream('go');
      const firstOutput = first.output ?? first;
      const heldUsage = firstOutput.usage;
      const heldTotalUsage = firstOutput.totalUsage;
      const heldFinishReason = firstOutput.finishReason;
      await drain(first);

      // PF-6758 / PF-5349: a durable/evented caller sees only the prior step's usage.
      const heldExpected = durableGap ? PRIOR : add(PRIOR, SUSPENDED);
      expect(tokens(await settled(heldUsage))).toEqual(heldExpected);
      expect(tokens(await settled(heldTotalUsage))).toEqual(heldExpected);
      expect(await settled(heldFinishReason)).toBe('suspended');

      const resumed = await drain(await resume(agent, kind, first.runId, toolCallId));
      expect(await resumed.text).toBe('done');
      expect(tokens(await resumed.totalUsage)).toEqual(add(PRIOR, SUSPENDED, RESUMED));

      if (isDurableEngineVariant(engine)) {
        expect(tokens(await firstOutput.usage)).toEqual(add(PRIOR, SUSPENDED, RESUMED));
        expect(tokens(await firstOutput.totalUsage)).toEqual(add(PRIOR, SUSPENDED, RESUMED));
        expect(await firstOutput.finishReason).toBe('stop');
        // PF-6758 / PF-5349: neither held value ever carries upstream's suspended-step total.
        // The held `usage` read only the prior step at the suspension (above) and, once the run
        // resumes, follows the run's cumulative usage; the held `totalUsage` stays at the prior step.
        expect(tokens(await heldUsage)).toEqual(add(PRIOR, SUSPENDED, RESUMED));
        expect(tokens(await heldTotalUsage)).toEqual(PRIOR);
        expect(await heldFinishReason).toBe('suspended');
      }
    });
  }

  it('keeps the suspended step usage when a resumed tool suspends again', async () => {
    const { agent, toolCallId } = await setup('suspend');
    const first: RunHandle = await agent.stream('go');
    await drain(first);

    const againRun = await resume(agent, 'suspend', first.runId, toolCallId, 'again');
    const again = await drain(againRun);
    expect(tokens(await settled(again.usage))).toEqual(suspendedUsage(SUSPENDED));
    expect(tokens(await settled(again.totalUsage))).toEqual(suspendedUsage(SUSPENDED));
    // PF-1875 (PF-6640 sync): the fork refuses to resume a run whose row is still
    // `running` (it does not poll a live claim), and the re-suspension is persisted
    // only after the suspension chunk. Upstream #27882 now settles usage on that chunk,
    // so finish the resumed stream before resuming again, as a caller must.
    if (!isDurableEngineVariant(engine)) {
      for await (const _chunk of againRun.fullStream) {
        // drain to the end so the suspended snapshot is persisted
      }
    }

    const resumed = await drain(await resume(agent, 'suspend', first.runId, toolCallId));
    expect(await resumed.text).toBe('done');
    expect(tokens(await resumed.totalUsage)).toEqual(add(SUSPENDED, RESUMED));
  });
});
