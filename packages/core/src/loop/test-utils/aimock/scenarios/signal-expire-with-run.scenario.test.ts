/**
 * AIMock Scenario: run-scoped signal delivery (PF-7473)
 *
 * A system reminder sent to an active run with `ifActive.expireWithRun` is
 * drained into that same run's next model request (the Harness `subagent`
 * tool's `send` path), and nothing about it starts another run afterwards.
 */

import { stepCountIs } from '@internal/ai-sdk-v5';
import { expect, it } from 'vitest';
import { z } from 'zod/v4';
import { MockMemory } from '../../../../memory/mock';
import { createTool } from '../../../../tools';
import { runLoopScenario, useLoopScenarioAimock, describeForAllEngines, createSharedAgent } from '../aimock-scenario';

const REMINDER = 'Parent says: also add a glossary';

function requestText(request: any): string {
  return JSON.stringify(request?.body?.messages ?? request?.body ?? {});
}

describeForAllEngines('AIMock scenario: run-scoped signal delivery', engine => {
  const getMock = useLoopScenarioAimock();

  it('drains a run-scoped reminder into the active run and starts no follow-up run', async () => {
    const threadId = 'expire-with-run-thread';
    const resourceId = 'expire-with-run-user';
    const accepted: any[] = [];

    let signalAgent: any;
    const doWork = createTool({
      id: 'do_work',
      description: 'Performs work; the parent sends an instruction while it runs',
      inputSchema: z.object({}),
      outputSchema: z.object({ ok: z.boolean() }),
      execute: async () => {
        const result = signalAgent.sendSignal(
          { type: 'system-reminder', contents: REMINDER },
          {
            resourceId,
            threadId,
            ifActive: { behavior: 'deliver', expireWithRun: true },
            ifIdle: { behavior: 'discard' },
          },
        );
        accepted.push(await result.accepted);
        return { ok: true };
      },
    });

    const memory = new MockMemory();
    const shared = await createSharedAgent(getMock(), { tools: { do_work: doWork }, memory, engine });
    signalAgent = shared.agent;

    const { output, requests } = await runLoopScenario({
      engine,
      llm: getMock(),
      prompt: 'Please do the work.',
      sharedAgent: shared,
      memory,
      threadId,
      resourceId,
      stopWhen: stepCountIs(5),
      fixtures: llm => {
        llm.on(
          { endpoint: 'chat', hasToolResult: false },
          { toolCalls: [{ id: 'call_work', name: 'do_work', arguments: {} }] },
        );
        llm.on(
          { endpoint: 'chat', predicate: req => JSON.stringify(req).includes(REMINDER) },
          { content: 'glossary added' },
        );
        llm.on(
          { endpoint: 'chat', hasToolResult: true, predicate: req => !JSON.stringify(req).includes(REMINDER) },
          { content: 'done without reminder' },
        );
      },
    });

    expect(accepted).toEqual([expect.objectContaining({ action: 'deliver' })]);
    expect(await output.text).toContain('glossary added');
    expect(requests.some(request => requestText(request).includes(REMINDER))).toBe(true);

    // The run consumed the reminder: exactly the tool-call request and the
    // drained request were made, and no further model request follows.
    const settledCount = getMock().getRequests().length;
    expect(settledCount).toBe(2);
    await new Promise(resolve => setTimeout(resolve, 150));
    expect(getMock().getRequests().length).toBe(settledCount);
  });
});
