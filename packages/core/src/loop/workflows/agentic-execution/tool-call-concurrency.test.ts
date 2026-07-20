import { describe, expect, it } from 'vitest';
import {
  effectiveToolSetRequiresSequentialExecution,
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
});
