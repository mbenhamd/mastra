import { describe, expect, it } from 'vitest';
import type { MastraDBMessage } from '../../agent/message-list';
import { resolveFrameworkSuspendedToolIdentity, resolveFrameworkSuspendedToolRunId } from './suspended-tool-run-id';

function assistantMessage({
  suspendedTools,
  pendingToolApprovals,
  parts = [],
}: {
  suspendedTools?: Record<string, unknown>;
  pendingToolApprovals?: Record<string, unknown>;
  parts?: Array<{ type: string; data: Record<string, unknown> }>;
}): MastraDBMessage {
  return {
    id: 'assistant-message',
    role: 'assistant',
    createdAt: new Date(0),
    content: {
      format: 2,
      metadata: { suspendedTools, pendingToolApprovals },
      parts,
    },
  } as unknown as MastraDBMessage;
}

const resolve = (overrides: Partial<Parameters<typeof resolveFrameworkSuspendedToolRunId>[0]> = {}) =>
  resolveFrameworkSuspendedToolRunId({
    toolCallId: 'call-1',
    toolName: 'agent-researcher',
    resumeSource: 'model',
    messages: [],
    ...overrides,
  });

describe('resolveFrameworkSuspendedToolRunId', () => {
  it('prefers the framework suspend payload and preserves custom id formats', () => {
    expect(
      resolve({
        resumeSource: 'framework',
        suspendData: { suspendedToolRunId: ' custom/run:id ' },
        modelSuppliedSuspendedToolRunId: 'foreign-run',
      }),
    ).toBe(' custom/run:id ');
  });

  it.each(['null', ' undefined ', 'NONE', '', 42, null])('rejects an unverified model claim %j', claim => {
    expect(resolve({ modelSuppliedSuspendedToolRunId: claim })).toBeUndefined();
  });

  it('resolves an exact persisted tool call for a framework-driven resume', () => {
    // Single-target routing: only the framework-targeted call resolves. Entries carry the current
    // native producer envelope shape; the router itself stays coordinate-scoped while the caller
    // authenticates the envelope before routing.
    expect(
      resolve({
        resumeSource: 'framework',
        messages: [
          assistantMessage({
            suspendedTools: {
              'call-1': {
                version: 1,
                originRunId: 'outer-run',
                stepId: 'toolCallStep',
                toolCallId: 'call-1',
                toolName: 'agent-researcher',
                runId: 'outer-run',
                delegatedRunId: 'inner-run',
                type: 'suspension',
              },
            },
          }),
        ],
      }),
    ).toBe('inner-run');
  });

  it('never resolves a different same-tool call for a framework-driven resume', () => {
    // Single-target regression: a unique same-tool sibling is not a fallback target. Only the exact
    // framework-targeted call may resolve; any other call yields undefined even when it is unique.
    expect(
      resolve({
        resumeSource: 'framework',
        toolCallId: 'call-1',
        messages: [
          assistantMessage({
            suspendedTools: {
              'other-call': {
                version: 1,
                originRunId: 'outer-run',
                stepId: 'toolCallStep',
                toolCallId: 'other-call',
                toolName: 'agent-researcher',
                runId: 'outer-run',
                delegatedRunId: 'other-inner-run',
                type: 'suspension',
              },
            },
          }),
        ],
      }),
    ).toBeUndefined();
  });

  it('uses a delegated approval id only for a framework-driven resume', () => {
    const messages = [
      assistantMessage({
        pendingToolApprovals: {
          'call-1': {
            version: 1,
            originRunId: 'outer-run',
            stepId: 'toolCallStep',
            toolCallId: 'call-1',
            toolName: 'charge-card',
            parentToolName: 'agent-researcher',
            runId: 'outer-run',
            delegatedRunId: 'inner-run',
            type: 'approval',
          },
        },
      }),
    ];

    expect(resolve({ resumeSource: 'framework', messages })).toBe('inner-run');
    expect(
      resolve({
        messages,
        modelSuppliedSuspendedToolCallId: 'call-1',
        modelSuppliedSuspendedToolRunId: 'inner-run',
      }),
    ).toBeUndefined();
  });

  it('does not treat an outer approval run as delegated identity', () => {
    expect(
      resolve({
        resumeSource: 'framework',
        messages: [
          assistantMessage({
            pendingToolApprovals: {
              'call-1': {
                toolCallId: 'call-1',
                toolName: 'agent-researcher',
                runId: 'outer-run',
                type: 'approval',
              },
            },
          }),
        ],
      }),
    ).toBeUndefined();
  });

  it('does not infer delegated approval identity from runId differing from originRunId', () => {
    // A `runId !== originRunId` inequality is routing inference, not authentication: only the
    // producer-written `delegatedRunId` exposes an inner run for an approval entry. An approval
    // without it yields no candidate even when the runs differ.
    const messages = [
      assistantMessage({
        pendingToolApprovals: {
          'call-1': {
            toolCallId: 'call-1',
            toolName: 'agent-researcher',
            runId: 'some-other-run',
            originRunId: 'outer-run',
            type: 'approval',
          },
        },
      }),
    ];

    expect(resolve({ resumeSource: 'framework', messages })).toBeUndefined();
    expect(
      resolve({
        messages,
        modelSuppliedSuspendedToolCallId: 'call-1',
        modelSuppliedSuspendedToolRunId: 'some-other-run',
      }),
    ).toBeUndefined();
  });

  it('derives the run id from the exact suspended tool call selected by the model', () => {
    const messages = [
      assistantMessage({
        suspendedTools: {
          'old-call': {
            version: 1,
            originRunId: 'outer-run',
            stepId: 'toolCallStep',
            toolCallId: 'old-call',
            toolName: 'agent-researcher',
            runId: 'outer-run',
            delegatedRunId: 'inner-run',
            type: 'suspension',
          },
        },
      }),
    ];

    expect(resolve({ messages, modelSuppliedSuspendedToolCallId: 'old-call' })).toBe('inner-run');
    expect(
      resolve({
        messages,
        modelSuppliedSuspendedToolCallId: 'old-call',
        modelSuppliedSuspendedToolRunId: 'foreign-run',
      }),
    ).toBeUndefined();
    expect(
      resolve({
        toolName: 'agent-writer',
        messages,
        modelSuppliedSuspendedToolCallId: 'old-call',
      }),
    ).toBeUndefined();
  });

  it('falls back to an unresumed suspension part', () => {
    expect(
      resolve({
        modelSuppliedSuspendedToolCallId: 'old-call',
        modelSuppliedSuspendedToolRunId: 'part-run',
        messages: [
          assistantMessage({
            parts: [
              {
                type: 'data-tool-call-suspended',
                data: {
                  version: 1,
                  originRunId: 'outer-run',
                  stepId: 'toolCallStep',
                  type: 'suspension',
                  toolCallId: 'old-call',
                  toolName: 'agent-researcher',
                  runId: 'part-run',
                },
              },
              {
                type: 'data-tool-call-suspended',
                data: {
                  toolCallId: 'resumed-call',
                  toolName: 'agent-researcher',
                  runId: 'resumed-run',
                  resumed: true,
                },
              },
            ],
          }),
        ],
      }),
    ).toBe('part-run');
  });

  it('resolves the exact framework-targeted suspension when the id is known', () => {
    // No envelope-free legacy fallback: a framework resume carries its exact call id. A unique
    // same-tool entry for a different call never resolves; the exact targeted entry does.
    const exactMessages = [
      assistantMessage({
        suspendedTools: {
          'call-1': {
            version: 1,
            originRunId: 'outer-run',
            stepId: 'toolCallStep',
            toolCallId: 'call-1',
            toolName: 'agent-researcher',
            runId: 'outer-run',
            delegatedRunId: 'exact-inner-run',
            type: 'suspension',
          },
        },
      }),
    ];
    expect(resolve({ resumeSource: 'framework', messages: exactMessages })).toBe('exact-inner-run');
  });

  it('rejects a sibling run id that does not belong to the claimed suspended call', () => {
    const messages = [
      assistantMessage({
        suspendedTools: {
          'call-a': {
            version: 1,
            originRunId: 'outer-run',
            stepId: 'toolCallStep',
            toolCallId: 'call-a',
            toolName: 'agent-researcher',
            runId: 'outer-run',
            delegatedRunId: 'inner-a',
            type: 'suspension',
          },
          'call-b': {
            version: 1,
            originRunId: 'outer-run',
            stepId: 'toolCallStep',
            toolCallId: 'call-b',
            toolName: 'agent-researcher',
            runId: 'outer-run',
            delegatedRunId: 'inner-b',
            type: 'suspension',
          },
        },
      }),
    ];

    expect(resolve({ messages })).toBeUndefined();
    expect(
      resolve({
        messages,
        modelSuppliedSuspendedToolCallId: 'call-b',
        modelSuppliedSuspendedToolRunId: 'inner-a',
      }),
    ).toBeUndefined();
    expect(
      resolve({
        messages,
        modelSuppliedSuspendedToolCallId: 'call-b',
        modelSuppliedSuspendedToolRunId: 'inner-b',
      }),
    ).toBe('inner-b');
  });

  it('uses the claimed tool call to disambiguate repeated run ids', () => {
    const messages = [
      assistantMessage({
        suspendedTools: {
          'call-a': {
            version: 1,
            originRunId: 'outer-run',
            stepId: 'toolCallStep',
            toolCallId: 'call-a',
            toolName: 'agent-researcher',
            runId: 'outer-run',
            delegatedRunId: 'repeated-run',
            type: 'suspension',
          },
          'call-b': {
            version: 1,
            originRunId: 'outer-run',
            stepId: 'toolCallStep',
            toolCallId: 'call-b',
            toolName: 'agent-researcher',
            runId: 'outer-run',
            delegatedRunId: 'repeated-run',
            type: 'suspension',
          },
        },
      }),
    ];

    expect(resolve({ messages, modelSuppliedSuspendedToolCallId: 'call-b' })).toBe('repeated-run');
    expect(resolve({ messages, modelSuppliedSuspendedToolCallId: 'unknown-call' })).toBeUndefined();
  });
});

describe('resolveFrameworkSuspendedToolIdentity', () => {
  it('returns the original persisted identity selected by a model resume', () => {
    const identity = resolveFrameworkSuspendedToolIdentity({
      toolCallId: 'new-model-call',
      toolName: 'agent-researcher',
      resumeSource: 'model',
      modelSuppliedSuspendedToolCallId: 'original-call',
      modelSuppliedSuspendedToolRunId: 'inner-run',
      messages: [
        assistantMessage({
          suspendedTools: {
            'original-call': {
              version: 1,
              originRunId: 'outer-run',
              stepId: 'toolCallStep',
              toolCallId: 'original-call',
              toolName: 'agent-researcher',
              runId: 'outer-run',
              delegatedRunId: 'inner-run',
              type: 'suspension',
            },
          },
        }),
      ],
    });

    expect(identity).toEqual({
      toolCallId: 'original-call',
      toolName: 'agent-researcher',
      runId: 'inner-run',
      type: 'suspension',
    });
  });

  it('returns the exact framework-targeted approval identity', () => {
    const identity = resolveFrameworkSuspendedToolIdentity({
      toolCallId: 'approval-call',
      toolName: 'agent-researcher',
      resumeSource: 'framework',
      messages: [
        assistantMessage({
          pendingToolApprovals: {
            'approval-call': {
              version: 1,
              originRunId: 'outer-run',
              stepId: 'toolCallStep',
              toolCallId: 'approval-call',
              parentToolName: 'agent-researcher',
              runId: 'outer-run',
              delegatedRunId: 'inner-run',
              type: 'approval',
            },
          },
        }),
      ],
    });

    expect(identity).toEqual({
      toolCallId: 'approval-call',
      toolName: 'agent-researcher',
      runId: 'inner-run',
      type: 'approval',
    });
  });

  it('returns the framework-targeted identity carried in suspend data', () => {
    expect(
      resolveFrameworkSuspendedToolIdentity({
        toolCallId: 'call-1',
        toolName: 'agent-researcher',
        resumeSource: 'framework',
        suspendData: { suspendedToolRunId: 'inner-run' },
        messages: [],
      }),
    ).toEqual({
      toolCallId: 'call-1',
      toolName: 'agent-researcher',
      runId: 'inner-run',
      type: 'suspension',
    });
  });

  it('infers legacy delegated approval suspend data from the approval marker', () => {
    expect(
      resolveFrameworkSuspendedToolIdentity({
        toolCallId: 'approval-call',
        toolName: 'agent-researcher',
        resumeSource: 'framework',
        suspendData: {
          requireToolApproval: { toolCallId: 'approval-call' },
          suspendedToolRunId: 'inner-run',
        },
        messages: [],
      }),
    ).toEqual({
      toolCallId: 'approval-call',
      toolName: 'agent-researcher',
      runId: 'inner-run',
      type: 'approval',
    });
  });
});
