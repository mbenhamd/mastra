import { describe, expect, it } from 'vitest';
import {
  effectiveToolSetRequiresSequentialExecution,
  resolveCalledBatchToolCallConcurrency,
  resolveConfiguredToolCallConcurrency,
  resolveToolCallConcurrency,
} from './tool-call-concurrency';

describe('tool call concurrency resolution', () => {
  const safeTool = {};
  const approvalTool = { requireApproval: true };
  const dynamicApprovalTool = { needsApprovalFn: () => false };
  const rawAiSdkApprovalTool = { needsApproval: () => false };
  const suspendTool = { hasSuspendSchema: true };

  it('requires sequential execution when global approval is enabled', () => {
    expect(
      effectiveToolSetRequiresSequentialExecution({
        requireToolApproval: true,
        tools: {
          safe: safeTool,
        },
        activeTools: ['safe'],
      }),
    ).toBe(true);
  });

  it('requires sequential execution when global approval is a function', () => {
    // A function policy can only be evaluated per call once args are known, so before
    // execution we conservatively force sequential to avoid approval suspensions racing.
    expect(
      effectiveToolSetRequiresSequentialExecution({
        requireToolApproval: () => false,
        tools: {
          safe: safeTool,
        },
        activeTools: ['safe'],
      }),
    ).toBe(true);
  });

  it('scans all current tools when activeTools is undefined', () => {
    expect(
      effectiveToolSetRequiresSequentialExecution({
        tools: {
          safe: safeTool,
          approval: approvalTool,
        },
        activeTools: undefined,
      }),
    ).toBe(true);
  });

  it('scans no tools when activeTools is empty', () => {
    expect(
      effectiveToolSetRequiresSequentialExecution({
        tools: {
          approval: approvalTool,
        },
        activeTools: [],
      }),
    ).toBe(false);
  });

  it('ignores inactive approval and suspension tools', () => {
    expect(
      effectiveToolSetRequiresSequentialExecution({
        tools: {
          safe: safeTool,
          approval: approvalTool,
          suspend: suspendTool,
        },
        activeTools: ['safe'],
      }),
    ).toBe(false);
  });

  it('requires sequential execution when a dynamic approval tool is active', () => {
    expect(
      effectiveToolSetRequiresSequentialExecution({
        tools: {
          safe: safeTool,
          dynamicApproval: dynamicApprovalTool,
        },
        activeTools: ['dynamicApproval'],
      }),
    ).toBe(true);
  });

  it('requires sequential execution when a raw AI SDK approval tool is active', () => {
    expect(
      effectiveToolSetRequiresSequentialExecution({
        tools: {
          safe: safeTool,
          rawApproval: rawAiSdkApprovalTool,
        },
        activeTools: ['rawApproval'],
      }),
    ).toBe(true);
  });

  it('keeps parallel tool calls concurrent when unrelated available tools can suspend', () => {
    expect(
      resolveToolCallConcurrency({
        tools: {
          subagent: safeTool,
          ask_user: suspendTool,
          submit_plan: suspendTool,
        },
        activeTools: ['subagent'],
        configuredConcurrency: 4,
      }),
    ).toBe(4);
  });

  it('ignores unknown active tool names', () => {
    expect(
      effectiveToolSetRequiresSequentialExecution({
        tools: {
          safe: safeTool,
        },
        activeTools: ['missing'],
      }),
    ).toBe(false);
  });

  it('uses the configured concurrency when the effective tool set is safe', () => {
    expect(
      resolveToolCallConcurrency({
        tools: {
          safe: safeTool,
          approval: approvalTool,
        },
        activeTools: ['safe'],
        configuredConcurrency: 4,
      }),
    ).toBe(4);
  });

  it('serializes an effective tool surface containing a permission-policy ask', () => {
    expect(
      resolveToolCallConcurrency({
        tools: {
          read: safeTool,
          write: safeTool,
        },
        activeTools: ['read', 'write'],
        permissionPolicy: toolName => (toolName === 'write' ? 'ask' : 'allow'),
        configuredConcurrency: 4,
      }),
    ).toBe(1);
  });

  it('keeps an all-allow permission-policy surface concurrent', () => {
    expect(
      resolveToolCallConcurrency({
        tools: {
          firstRead: safeTool,
          secondRead: safeTool,
        },
        activeTools: ['firstRead', 'secondRead'],
        permissionPolicy: () => 'allow',
        configuredConcurrency: 4,
      }),
    ).toBe(4);
  });

  it('fails conservatively to sequential execution when the permission policy throws', () => {
    expect(
      resolveToolCallConcurrency({
        tools: { read: safeTool },
        activeTools: ['read'],
        permissionPolicy: () => {
          throw new Error('policy unavailable');
        },
        configuredConcurrency: 4,
      }),
    ).toBe(1);
  });

  it('honors configured concurrency of one for safe tools', () => {
    expect(
      resolveToolCallConcurrency({
        tools: {
          safe: safeTool,
        },
        activeTools: ['safe'],
        configuredConcurrency: 1,
      }),
    ).toBe(1);
  });

  it('normalizes invalid configured concurrency to the default', () => {
    expect(resolveConfiguredToolCallConcurrency(undefined)).toBe(10);
    expect(resolveConfiguredToolCallConcurrency(0)).toBe(10);
    expect(resolveConfiguredToolCallConcurrency(-1)).toBe(10);
    expect(resolveConfiguredToolCallConcurrency(3)).toBe(3);
  });

  describe('called-batch concurrency (the tool-call step wiring)', () => {
    const tools = {
      search: safeTool,
      spawn: safeTool,
      approval: approvalTool,
      suspending: suspendTool,
      dynamic: dynamicApprovalTool,
    };

    it('keeps a safe called batch parallel even when ask/suspend tools are registered', () => {
      // The old wiring scanned the whole active set, so any surface exposing an
      // approval-family tool ran EVERY batch sequentially — including pure
      // search fan-outs and multi-spawn subagent batches that never touched an
      // approval tool.
      expect(
        resolveCalledBatchToolCallConcurrency({
          toolCalls: [{ toolName: 'search' }, { toolName: 'spawn' }, { toolName: 'search' }],
          tools,
          configuredConcurrency: 10,
        }),
      ).toBe(10);
    });

    it('serializes when the batch actually calls an approval tool', () => {
      expect(
        resolveCalledBatchToolCallConcurrency({
          toolCalls: [{ toolName: 'search' }, { toolName: 'approval' }],
          tools,
          configuredConcurrency: 10,
        }),
      ).toBe(1);
    });

    it('serializes when the batch calls a suspend-capable tool', () => {
      expect(
        resolveCalledBatchToolCallConcurrency({
          toolCalls: [{ toolName: 'suspending' }, { toolName: 'search' }],
          tools,
          configuredConcurrency: 10,
        }),
      ).toBe(1);
    });

    it('serializes when the batch calls a dynamically-approving tool', () => {
      expect(
        resolveCalledBatchToolCallConcurrency({
          toolCalls: [{ toolName: 'dynamic' }],
          tools,
          configuredConcurrency: 10,
        }),
      ).toBe(1);
    });

    it('serializes when a permission policy marks a called tool ask', () => {
      expect(
        resolveCalledBatchToolCallConcurrency({
          toolCalls: [{ toolName: 'search' }, { toolName: 'spawn' }],
          tools,
          permissionPolicy: toolName => (toolName === 'spawn' ? 'ask' : 'allow'),
          configuredConcurrency: 10,
        }),
      ).toBe(1);
    });

    it('stays sequential under a global function-valued approval policy', () => {
      expect(
        resolveCalledBatchToolCallConcurrency({
          toolCalls: [{ toolName: 'search' }],
          tools,
          requireToolApproval: () => false,
          configuredConcurrency: 10,
        }),
      ).toBe(1);
    });

    it('ignores hallucinated tool names and non-string entries', () => {
      expect(
        resolveCalledBatchToolCallConcurrency({
          toolCalls: [{ toolName: 'search' }, { toolName: 'no_such_tool' }, { toolName: 42 }, {}],
          tools,
          configuredConcurrency: 10,
        }),
      ).toBe(10);
    });
  });
});
