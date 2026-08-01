import { describe, expect, it } from 'vitest';
import { RequestContext } from '../../../request-context';
import { TOOL_PERMISSION_POLICY_KEY, TOOL_PERMISSION_POLICY_STABLE_KEY } from '../../tool-permission-prefilter';
import { DurableAgentDefaults } from '../constants';
import { createDurableAgenticWorkflow } from './create-durable-agentic-workflow';

/**
 * The durable agentic workflow's tool-call foreach must honor the run's
 * `toolCallConcurrency` while forcing sequential execution for approval /
 * suspend-capable tool sets. Because the workflow graph is created once and
 * shared across runs, concurrency is a resolver evaluated per run from the
 * serialized iteration state — never a static value or shared mutable object.
 */

function findForeachEntry(steps: any[]): any {
  for (const entry of steps ?? []) {
    if (entry.type === 'foreach') return entry;

    // Nested workflow as a plain step: { type: 'step', step: Workflow }
    if (entry.type === 'step' && entry.step?.executionGraph) {
      const nested = findForeachEntry(entry.step.executionGraph.steps);
      if (nested) return nested;
    }

    // Loop / foreach containers hold a SingleStepEntry in `.step`.
    // Recurse into that one-element list so nested workflows buried under
    // `.dowhile(subWorkflow, …)` are still found.
    if (entry.step && entry.type !== 'step') {
      const nested = findForeachEntry([entry.step]);
      if (nested) return nested;
    }

    if (entry.steps) {
      const nested = findForeachEntry(entry.steps);
      if (nested) return nested;
    }
  }
  return undefined;
}

describe('createDurableAgenticWorkflow tool-call concurrency', () => {
  const workflow = createDurableAgenticWorkflow();
  const foreachEntry = findForeachEntry((workflow as any).executionGraph.steps);

  const resolveWith = (state: unknown, inputData: unknown[] = [], requestContext?: RequestContext): number => {
    const resolver = foreachEntry.opts.concurrency;
    expect(typeof resolver).toBe('function');
    return resolver({ inputData, getInitData: () => state, requestContext });
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

  it('uses configured concurrency only for a stable live policy that allows every emitted call', () => {
    const requestContext = new RequestContext();
    requestContext.set(TOOL_PERMISSION_POLICY_KEY, () => 'allow');
    requestContext.set(TOOL_PERMISSION_POLICY_STABLE_KEY, true);

    expect(
      resolveWith(
        { options: { permissionPolicyRequired: true, toolCallConcurrency: 5 }, toolsMetadata: [] },
        [
          { toolCallId: 'call-a', toolName: 'read_a', args: {} },
          { toolCallId: 'call-b', toolName: 'read_b', args: {} },
        ],
        requestContext,
      ),
    ).toBe(5);
  });

  it('keeps a stable live policy batch sequential when any emitted call asks', () => {
    const requestContext = new RequestContext();
    requestContext.set(TOOL_PERMISSION_POLICY_KEY, (toolName: string) => (toolName === 'write' ? 'ask' : 'allow'));
    requestContext.set(TOOL_PERMISSION_POLICY_STABLE_KEY, true);

    expect(
      resolveWith(
        { options: { permissionPolicyRequired: true, toolCallConcurrency: 5 }, toolsMetadata: [] },
        [
          { toolCallId: 'call-read', toolName: 'read', args: {} },
          { toolCallId: 'call-write', toolName: 'write', args: {} },
        ],
        requestContext,
      ),
    ).toBe(1);
  });

  it('forces sequential execution when a tool requires approval or can suspend', () => {
    expect(
      resolveWith({
        options: { toolCallConcurrency: 10 },
        toolsMetadata: [{ id: 'gated', name: 'gated', inputSchema: { type: 'object' }, requireApproval: true }],
      }),
    ).toBe(1);
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
