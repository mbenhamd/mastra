import { DurableAgentDefaults } from '@mastra/core/agent/durable';
import { Inngest } from 'inngest';
import { describe, expect, it } from 'vitest';

import {
  createInngestDurableAgenticWorkflow,
  createInngestDurableAgenticWorkflowIds,
  InngestDurableStepIds,
} from './create-inngest-agentic-workflow';

/**
 * Regression coverage for #19317: the Inngest durable engine must honor
 * `toolCallConcurrency` instead of always running tool calls sequentially.
 *
 * The tool-call foreach carries a concurrency *resolver* that derives the
 * effective concurrency at execution time from the serialized iteration state
 * (options + toolsMetadata). This keeps resolution safe across Inngest step
 * memoization/replay and across runs sharing the same workflow instance —
 * unlike a shared mutable options object.
 */

function findForeachEntry(steps: any[]): any {
  for (const entry of steps ?? []) {
    if (entry.type === 'foreach') return entry;
    if (entry.step?.executionGraph) {
      const nested = findForeachEntry(entry.step.executionGraph.steps);
      if (nested) return nested;
    }
    if (entry.steps) {
      const nested = findForeachEntry(entry.steps);
      if (nested) return nested;
    }
  }
  return undefined;
}

describe('createInngestDurableAgenticWorkflow ownership IDs', () => {
  const inngest = new Inngest({ id: 'inngest-agentic-workflow-id-tests' });

  it('retains the historical shared IDs for direct factory callers', () => {
    const workflow = createInngestDurableAgenticWorkflow({ inngest }) as any;
    const functionIds = workflow.getFunctions().map((fn: any) => fn.id());

    expect(workflow.id).toBe(InngestDurableStepIds.AGENTIC_LOOP);
    expect(functionIds).toEqual(
      expect.arrayContaining([
        `workflow.${InngestDurableStepIds.AGENTIC_LOOP}`,
        `workflow.${InngestDurableStepIds.AGENTIC_EXECUTION}`,
      ]),
    );
  });

  it('derives deterministic collision-safe IDs from the public durable-agent ID', () => {
    const first = createInngestDurableAgenticWorkflowIds('owner-agent');
    const repeated = createInngestDurableAgenticWorkflowIds('owner-agent');
    const other = createInngestDurableAgenticWorkflowIds('other-agent');

    expect(first).toEqual(repeated);
    expect(first).not.toEqual(other);
    expect(first.AGENTIC_LOOP).toMatch(/^inngest:durable-agentic-loop:[a-f0-9]{32}$/);
    expect(first.AGENTIC_EXECUTION).toMatch(/^inngest:durable-agentic-execution:[a-f0-9]{32}$/);
    expect(() => createInngestDurableAgenticWorkflowIds('')).toThrow(/non-empty agent ID/);
  });

  it('applies the owner namespace to both parent and nested function IDs', () => {
    const workflowIds = createInngestDurableAgenticWorkflowIds('nested-owner');
    const workflow = createInngestDurableAgenticWorkflow({ inngest, workflowIds }) as any;
    const functionIds = workflow.getFunctions().map((fn: any) => fn.id());

    expect(workflow.id).toBe(workflowIds.AGENTIC_LOOP);
    expect(functionIds).toEqual(
      expect.arrayContaining([`workflow.${workflowIds.AGENTIC_LOOP}`, `workflow.${workflowIds.AGENTIC_EXECUTION}`]),
    );
    expect(functionIds).toHaveLength(2);
  });
});

describe('createInngestDurableAgenticWorkflow tool-call concurrency', () => {
  const inngest = new Inngest({ id: 'inngest-agentic-workflow-concurrency-tests' });
  const workflow = createInngestDurableAgenticWorkflow({ inngest });
  const foreachEntry = findForeachEntry((workflow as any).executionGraph.steps);

  const resolveWith = (state: unknown): number => {
    const resolver = foreachEntry.opts.concurrency;
    expect(typeof resolver).toBe('function');
    return resolver({ inputData: [], getInitData: () => state });
  };

  it('uses a concurrency resolver on the tool-call foreach (not a static value)', () => {
    expect(foreachEntry).toBeDefined();
    expect(typeof foreachEntry.opts.concurrency).toBe('function');
  });

  it('resolves the configured toolCallConcurrency from the iteration state', () => {
    expect(
      resolveWith({
        options: { toolCallConcurrency: 5 },
        toolsMetadata: [{ id: 'plain', name: 'plain', inputSchema: { type: 'object' } }],
      }),
    ).toBe(5);
  });

  it('defaults to the standard tool-call concurrency when unset', () => {
    expect(resolveWith({ options: {}, toolsMetadata: [] })).toBe(DurableAgentDefaults.TOOL_CALL_CONCURRENCY);
    // Missing init data (e.g. unexpected replay shape) must not crash — it
    // falls back to defaults.
    expect(resolveWith(undefined)).toBe(DurableAgentDefaults.TOOL_CALL_CONCURRENCY);
  });

  it('forces sequential execution when requireToolApproval is set globally', () => {
    expect(
      resolveWith({
        options: { requireToolApproval: true, toolCallConcurrency: 10 },
        toolsMetadata: [],
      }),
    ).toBe(1);
  });

  it('forces sequential execution when a tool requires approval', () => {
    expect(
      resolveWith({
        options: { toolCallConcurrency: 10 },
        toolsMetadata: [
          { id: 'plain', name: 'plain', inputSchema: { type: 'object' } },
          { id: 'gated', name: 'gated', inputSchema: { type: 'object' }, requireApproval: true },
        ],
      }),
    ).toBe(1);
  });

  it('forces sequential execution when a tool can suspend', () => {
    expect(
      resolveWith({
        options: { toolCallConcurrency: 10 },
        toolsMetadata: [
          { id: 'suspending', name: 'suspending', inputSchema: { type: 'object' }, hasSuspendSchema: true },
        ],
      }),
    ).toBe(1);
  });
});
