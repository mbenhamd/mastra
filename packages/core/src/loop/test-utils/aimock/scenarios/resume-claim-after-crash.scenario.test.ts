import { createOpenAI } from '@ai-sdk/openai-v5';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { z } from 'zod/v4';
import { Agent, AGENT_RESUME_CLAIM_STALE_MS } from '../../../../agent/agent';
import { Mastra } from '../../../../mastra';
import { InMemoryStore } from '../../../../storage';
import { createTool } from '../../../../tools';
import { useLoopScenarioAimock } from '../aimock-scenario';
import { SCENARIO_MODEL_ID } from '../types';

type HoldWrite = (method: string, args: any, phase: 'before' | 'after') => Promise<void> | undefined;

const createStorage = async () => new InMemoryStore();
const advance = (ms: number) => vi.advanceTimersByTimeAsync(ms).then(() => {});
const outcome = (resume: Promise<{ text: string }>) =>
  resume.then(
    r => r.text,
    (e: { id?: string }) => e?.id ?? String(e),
  );

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>(r => (resolve = r));
  return { promise, resolve };
}

/**
 * A resume claim held by a process that died must not block the suspension forever (#27366),
 * and a claim held by a live process must still not be taken over (#26133).
 *
 * Mastra instances share one store, like server processes. Worker A resumes and is inside the
 * resumed tool when it "dies": every store call it makes from then on hangs, so it neither renews
 * nor releases its claim. Other workers resume the same run afterwards.
 *
 * The in-memory store supports run fencing. A worker created with `fencing: false` sees a store
 * without it, which is also how a process running the code from before run-fencing leases behaves:
 * it never takes over a `running` claim.
 */
describe('AIMock loop scenario: resume claim after the claimant dies', () => {
  const getMock = useLoopScenarioAimock();

  afterEach(() => {
    vi.useRealTimers();
  });

  function createWorker(
    storage: InMemoryStore,
    name: string,
    runResumedTool: () => Promise<void>,
    options: {
      fencing?: boolean;
      agentClass?: any;
      // Holds one of this process's store calls before it reaches the store or before its result
      // comes back, to model a slow store call.
      holdWrite?: HoldWrite;
    } = {},
  ) {
    let dead = false;
    const executions: string[] = [];
    const calls: string[] = [];
    // The process's view of the shared store; once dead, its calls never complete.
    const processStorage = new Proxy(storage, {
      get(target, prop) {
        if (prop === 'getStore') {
          return async (domain: string) => {
            const store = await target.getStore(domain as any);
            if (domain !== 'workflows' || !store) return store;
            return new Proxy(store, {
              get(storeTarget, storeProp) {
                if (storeProp === 'supportsRunFencing' && options.fencing === false) return () => false;
                const value = Reflect.get(storeTarget, storeProp, storeTarget);
                if (typeof value !== 'function') return value;
                return (...args: any[]) => {
                  const method = String(storeProp);
                  calls.push(method);
                  if (dead) return new Promise(() => {});
                  const hold = options.holdWrite;
                  if (!hold) return value.apply(storeTarget, args);
                  return (async () => {
                    await hold(method, args[0], 'before');
                    const result = await value.apply(storeTarget, args);
                    await hold(method, args[0], 'after');
                    return result;
                  })();
                };
              },
            });
          };
        }
        const value = Reflect.get(target, prop, target);
        return typeof value === 'function' ? value.bind(target) : value;
      },
    });

    const openai = createOpenAI({ apiKey: 'aimock-test-key', baseURL: `${getMock().url.replace(/\/+$/, '')}/v1` });
    const AgentCtor = options.agentClass ?? Agent;
    const agent = new AgentCtor({
      id: 'find-user-agent',
      name: 'Find User Agent',
      instructions: 'You find users.',
      model: openai(SCENARIO_MODEL_ID),
      tools: {
        findUser: createTool({
          id: 'find-user',
          description: 'Finds a user by name',
          inputSchema: z.object({ query: z.string() }),
          suspendSchema: z.object({ message: z.string() }),
          resumeSchema: z.object({ name: z.string() }),
          execute: async (_input, context) => {
            if (!context?.agent?.resumeData) {
              return context?.agent?.suspend({ message: 'Which user?' });
            }
            executions.push(name);
            await runResumedTool();
            return { name: context.agent.resumeData.name };
          },
        }),
      },
    });
    const mastra = new Mastra({ agents: { agent }, logger: false, storage: processStorage });

    return {
      agent: mastra.getAgent('agent'),
      executions,
      calls,
      die: () => {
        dead = true;
      },
    };
  }

  async function setup(options: { fencing?: boolean; agentClass?: any } = {}) {
    getMock().on(
      { endpoint: 'chat', hasToolResult: false },
      {
        toolCalls: [{ id: 'call-1', name: 'find-user', arguments: { query: 'Ada' } }],
      },
    );
    getMock().on({ endpoint: 'chat', hasToolResult: true }, { content: 'Found Ada.' });

    const storage = await createStorage();
    const aEntered = deferred();
    const aFinish = deferred();
    const workerA = createWorker(
      storage,
      'A',
      () => {
        aEntered.resolve();
        return aFinish.promise;
      },
      options,
    );

    const suspended = await workerA.agent.generate('Find the user Ada');
    expect(suspended.finishReason).toBe('suspended');
    const runId = suspended.runId!;

    // Fake only the clock and the claim renewal interval, so the test can jump past the stale
    // window. The clock still advances with real time, so resume's own snapshot polling and
    // aimock's HTTP traffic behave normally.
    vi.useFakeTimers({ toFake: ['Date', 'setInterval', 'clearInterval'], shouldAdvanceTime: true });
    const resumeA = workerA.agent.resumeGenerate({ name: 'Ada' }, { runId });
    resumeA.catch(() => {});
    await aEntered.promise;

    return { storage, runId, workerA, resumeA, finishA: aFinish.resolve };
  }

  // PF-6739 (PF-6640 sync): upstream #27791 takeover deferred; fork refuses a resume of a run
  // whose resume claim is `running` (PF-4389 lineage claim has no `pending` state and no lease
  // takeover), so a stranded claim is recovered only through the fenced restart path (PF-6181).
  // Each upstream case below keeps its setup and asserts the fork refusal instead of a takeover.
  const FORK_REFUSAL = 'AGENT_RESUME_RUN_NOT_SUSPENDED';

  it('refuses a later resume of the claim of a resume whose process died (PF-6739)', async () => {
    const { storage, runId, workerA } = await setup();
    const workerB = createWorker(storage, 'B', async () => {});

    workerA.die();
    await advance(AGENT_RESUME_CLAIM_STALE_MS + 1);

    await expect(outcome(workerB.agent.resumeGenerate({ name: 'Ada' }, { runId }))).resolves.toBe(FORK_REFUSAL);
    expect(workerB.executions).toEqual([]);
  }, 30_000);

  it('still rejects a resume while the first resume is alive past the stale window', async () => {
    const { storage, runId, workerA, resumeA, finishA } = await setup();
    const workerB = createWorker(storage, 'B', async () => {});

    await advance(AGENT_RESUME_CLAIM_STALE_MS * 2);

    await expect(outcome(workerB.agent.resumeGenerate({ name: 'Ada' }, { runId }))).resolves.toBe(FORK_REFUSAL);
    expect(workerB.executions).toEqual([]);

    finishA();
    const resumed = await resumeA;
    expect(resumed.text).toBe('Found Ada.');
    expect(workerA.executions).toEqual(['A']);
  }, 30_000);

  it('refuses every one of many concurrent resumes of a dead claim (PF-6739)', async () => {
    const { storage, runId, workerA } = await setup();
    const takers = Array.from({ length: 6 }, (_, i) => createWorker(storage, `T${i}`, async () => {}));

    workerA.die();
    await advance(AGENT_RESUME_CLAIM_STALE_MS + 1);

    const outcomes = await Promise.all(takers.map(t => outcome(t.agent.resumeGenerate({ name: 'Ada' }, { runId }))));

    expect(outcomes).toEqual(Array(6).fill(FORK_REFUSAL));
    expect(takers.flatMap(t => t.executions)).toHaveLength(0);
  }, 30_000);

  it('never double-runs when processes without the lease run next to ones with it', async () => {
    const { storage, runId, workerA } = await setup();
    const oldC = createWorker(storage, 'oldC', async () => {}, { fencing: false });
    const workerB = createWorker(storage, 'B', async () => {});
    const oldF = createWorker(storage, 'oldF', async () => {}, { fencing: false });

    workerA.die();
    await advance(AGENT_RESUME_CLAIM_STALE_MS + 1);

    const outcomes = [
      await outcome(oldC.agent.resumeGenerate({ name: 'Ada' }, { runId })),
      await outcome(workerB.agent.resumeGenerate({ name: 'Ada' }, { runId })),
      await outcome(oldF.agent.resumeGenerate({ name: 'Ada' }, { runId })),
    ];
    await advance(AGENT_RESUME_CLAIM_STALE_MS * 2);
    outcomes.push(await outcome(oldF.agent.resumeGenerate({ name: 'Ada' }, { runId })));

    expect(outcomes).toEqual(Array(4).fill(FORK_REFUSAL));
    expect([...workerA.executions, ...oldC.executions, ...workerB.executions, ...oldF.executions]).toEqual(['A']);
  }, 60_000);

  it('never takes over a running claim started by a process without the lease', async () => {
    const { storage, runId, resumeA, finishA, workerA } = await setup({ fencing: false });
    const workerB = createWorker(storage, 'B', async () => {});

    // A runs well past the stale window without renewing anything.
    await advance(AGENT_RESUME_CLAIM_STALE_MS * 2);
    await expect(outcome(workerB.agent.resumeGenerate({ name: 'Ada' }, { runId }))).resolves.toBe(FORK_REFUSAL);

    finishA();
    expect((await resumeA).text).toBe('Found Ada.');
    expect([...workerA.executions, ...workerB.executions]).toEqual(['A']);
  }, 30_000);

  it('keeps the behaviour from before leases on a store without run fencing', async () => {
    const { storage, runId, workerA } = await setup({ fencing: false });
    const workerB = createWorker(storage, 'B', async () => {}, { fencing: false });

    // A dead running claim is never taken over without fencing: nothing tells it from a slow one.
    workerA.die();
    await advance(AGENT_RESUME_CLAIM_STALE_MS * 2);
    await expect(outcome(workerB.agent.resumeGenerate({ name: 'Ada' }, { runId }))).resolves.toBe(FORK_REFUSAL);

    expect([...workerA.executions, ...workerB.executions]).toEqual(['A']);
    const leaseCalls = ['claimRunOwnership', 'renewRunOwnership', 'releaseRunOwnership', 'getRunOwnership'];
    expect([...workerA.calls, ...workerB.calls].filter(c => leaseCalls.includes(c))).toEqual([]);
  }, 30_000);
});
