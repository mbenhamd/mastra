import { ReadableStream } from 'node:stream/web';
import { readUIMessageStream } from '@internal/ai-v6';
import { ChunkFrom } from '@mastra/core/stream';
import type { MastraModelOutput } from '@mastra/core/stream';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { handleChatStream, extractV6NativeApprovals } from '../chat-route';
import { toAISdkStream, toAISdkV5Stream } from '../convert-streams';
import { convertMastraChunkToAISDKv5, convertMastraChunkToAISDKv6, APPROVAL_ID_SEPARATOR } from '../helpers';

async function collectChunks(stream: ReadableStream) {
  const chunks: any[] = [];

  for await (const chunk of stream as any) {
    chunks.push(chunk);
  }

  return chunks;
}

function createApprovalStream() {
  return new ReadableStream({
    start(controller) {
      controller.enqueue({
        type: 'start',
        runId: 'run-123',
        from: ChunkFrom.AGENT,
        payload: { id: 'msg-1' },
      });

      controller.enqueue({
        type: 'step-start',
        runId: 'run-123',
        from: ChunkFrom.AGENT,
        payload: { messageId: 'msg-1' },
      });

      controller.enqueue({
        type: 'tool-call',
        runId: 'run-123',
        from: ChunkFrom.AGENT,
        payload: {
          toolCallId: 'tooluse_abc123',
          toolName: 'myTool',
          args: { param: 'value' },
        },
      });

      controller.enqueue({
        type: 'tool-call-approval',
        runId: 'run-123',
        from: ChunkFrom.AGENT,
        payload: {
          version: 1 as const,
          originRunId: 'run-123',
          stepId: 'toolCallStep',
          type: 'approval' as const,
          identityDigest: 'digest-123',
          resumeIdentityDigest: 'resume-digest-123',
          toolCallId: 'tooluse_abc123',
          toolName: 'myTool',
          args: { param: 'value' },
          resumeSchema: '{"type":"object","properties":{"approved":{"type":"boolean"}}}',
        },
      });

      controller.close();
    },
  });
}

describe('tool-call-approval chunk conversion (issue #12878)', () => {
  describe('convertMastraChunkToAISDKv5', () => {
    it('should include a state field in the data-tool-call-approval chunk', () => {
      const chunk = {
        type: 'tool-call-approval' as const,
        runId: 'run-123',
        from: ChunkFrom.AGENT,
        payload: {
          version: 1 as const,
          originRunId: 'run-123',
          stepId: 'toolCallStep',
          type: 'approval' as const,
          identityDigest: 'digest-123',
          resumeIdentityDigest: 'resume-digest-123',
          toolCallId: 'tooluse_abc123',
          toolName: 'myTool',
          args: { param: 'value' },
          resumeSchema: '{"type":"object","properties":{"approved":{"type":"boolean"}}}',
        },
      };

      const result = convertMastraChunkToAISDKv5({ chunk, mode: 'stream' }) as any;

      expect(result).toBeDefined();
      expect(result.type).toBe('data-tool-call-approval');
      expect(result.id).toBe('tooluse_abc123');
      expect(result.data).toMatchObject({
        identityDigest: 'digest-123',
        resumeIdentityDigest: 'resume-digest-123',
      });

      // Issue #12878: The data-tool-call-approval chunk should include a state
      // field so the frontend can identify the part's state consistently
      // with other tool UI parts (which have states like 'input-available',
      // 'output-available', etc.)
      expect(result.data).toHaveProperty('state', 'data-tool-call-approval');
    });

    it('should include a state field in the data-tool-call-suspended chunk', () => {
      const chunk = {
        type: 'tool-call-suspended' as const,
        runId: 'run-123',
        from: ChunkFrom.AGENT,
        payload: {
          version: 1 as const,
          originRunId: 'run-123',
          stepId: 'toolCallStep',
          type: 'suspension' as const,
          identityDigest: 'digest-123',
          resumeIdentityDigest: 'resume-digest-123',
          approval: { id: 'tooluse_abc123', approved: true as const },
          toolCallId: 'tooluse_abc123',
          toolName: 'myTool',
          args: { param: 'value' },
          suspendPayload: { reason: 'Needs user input' },
          resumeSchema: '{"type":"object"}',
        },
      };

      const result = convertMastraChunkToAISDKv5({ chunk, mode: 'stream' }) as any;

      expect(result).toBeDefined();
      expect(result.type).toBe('data-tool-call-suspended');
      expect(result.id).toBe('tooluse_abc123');

      // Issue #12878: Consistent with tool-call-approval, the suspended chunk
      // should also include a state field
      expect(result.data).toHaveProperty('state', 'data-tool-call-suspended');
      expect(result.data).toMatchObject({
        version: 1,
        originRunId: 'run-123',
        stepId: 'toolCallStep',
        type: 'suspension',
        identityDigest: 'digest-123',
        resumeIdentityDigest: 'resume-digest-123',
        approval: { id: 'tooluse_abc123', approved: true },
        args: { param: 'value' },
      });
    });
  });

  describe('end-to-end: tool-call-approval through agent stream', () => {
    it('should emit data-tool-call-approval with state field when tool requires approval', async () => {
      const aiSdkStream = toAISdkV5Stream(createApprovalStream() as unknown as MastraModelOutput, { from: 'agent' });
      const chunks = await collectChunks(aiSdkStream);

      // Should have the tool-input-available chunk for the tool call
      const toolInputChunk = chunks.find(chunk => chunk.type === 'tool-input-available');
      expect(toolInputChunk).toBeDefined();
      expect(toolInputChunk.toolCallId).toBe('tooluse_abc123');

      // Should have the data-tool-call-approval chunk
      const approvalChunk = chunks.find(chunk => chunk.type === 'data-tool-call-approval');
      expect(approvalChunk).toBeDefined();
      expect(approvalChunk.type).toBe('data-tool-call-approval');
      expect(approvalChunk.id).toBe('tooluse_abc123');

      // Issue #12878: The data field should include a state property
      expect(approvalChunk.data.state).toBe('data-tool-call-approval');

      // The rest of the data should still be present
      expect(approvalChunk.data.runId).toBe('run-123');
      expect(approvalChunk.data.toolCallId).toBe('tooluse_abc123');
      expect(approvalChunk.data.toolName).toBe('myTool');
      expect(approvalChunk.data.args).toEqual({ param: 'value' });
      expect(approvalChunk.data.resumeSchema).toBe('{"type":"object","properties":{"approved":{"type":"boolean"}}}');
    });
  });
});

describe('extractV6NativeApprovals', () => {
  it('returns runId and approved:true from an approval-responded part', () => {
    const approvalId = `run-123${APPROVAL_ID_SEPARATOR}tooluse_abc123`;
    const messages = [
      {
        role: 'assistant' as const,
        id: 'msg-1',
        parts: [
          {
            type: 'tool-myTool',
            toolCallId: 'tooluse_abc123',
            state: 'approval-responded' as const,
            input: { param: 'value' },
            approval: { id: approvalId, approved: true },
          },
        ],
      },
    ];

    const result = extractV6NativeApprovals(messages as any);

    expect(result).toEqual([
      {
        resumeData: { approved: true },
        runId: 'run-123',
        toolCallId: 'tooluse_abc123',
      },
    ]);
  });

  it('preserves separator-bearing run and tool-call IDs', () => {
    const toolCallId = 'provider::opaque-call';
    const messages = [
      {
        role: 'assistant' as const,
        id: 'msg-delimited',
        parts: [
          {
            type: 'tool-myTool',
            toolCallId,
            state: 'approval-responded' as const,
            input: {},
            approval: { id: `run::opaque${APPROVAL_ID_SEPARATOR}${toolCallId}`, approved: true },
          },
        ],
      },
    ];

    expect(extractV6NativeApprovals(messages as any)).toEqual([
      {
        resumeData: { approved: true },
        runId: 'run::opaque',
        toolCallId,
      },
    ]);
  });

  it('rejects a composite approval ID that conflicts with the visible tool-call ID', () => {
    const messages = [
      {
        role: 'assistant' as const,
        id: 'msg-conflict',
        parts: [
          {
            type: 'tool-myTool',
            toolCallId: 'visible-call',
            state: 'approval-responded' as const,
            input: {},
            approval: { id: `run-1${APPROVAL_ID_SEPARATOR}different-call`, approved: true },
          },
        ],
      },
    ];

    expect(extractV6NativeApprovals(messages as any)).toEqual([]);
  });

  it('does not fall back to an older valid response when the latest response is malformed', () => {
    const messages = [
      {
        role: 'assistant' as const,
        id: 'msg-no-fallback',
        parts: [
          {
            type: 'tool-myTool',
            toolCallId: 'older-call',
            state: 'approval-responded' as const,
            input: {},
            approval: { id: `run-1${APPROVAL_ID_SEPARATOR}older-call`, approved: true },
          },
          {
            type: 'tool-myTool',
            toolCallId: 'latest-call',
            state: 'approval-responded' as const,
            input: {},
            approval: { id: `run-1${APPROVAL_ID_SEPARATOR}different-call`, approved: true },
          },
        ],
      },
    ];

    expect(extractV6NativeApprovals(messages as any)).toEqual([]);
  });

  it('rejects a malformed latest approval response without throwing', () => {
    const messages = [
      {
        role: 'assistant' as const,
        id: 'msg-malformed-latest',
        parts: [
          {
            type: 'tool-myTool',
            toolCallId: 'latest-call',
            state: 'approval-responded' as const,
            input: {},
          },
        ],
      },
    ];

    expect(() => extractV6NativeApprovals(messages as any)).not.toThrow();
    expect(extractV6NativeApprovals(messages as any)).toEqual([]);
  });

  it('rejects a latest approval response without a boolean decision', () => {
    const messages = [
      {
        role: 'assistant' as const,
        id: 'msg-malformed-decision',
        parts: [
          {
            type: 'tool-myTool',
            toolCallId: 'latest-call',
            state: 'approval-responded' as const,
            input: {},
            approval: { id: `run-1${APPROVAL_ID_SEPARATOR}latest-call` },
          },
        ],
      },
    ];

    expect(extractV6NativeApprovals(messages as any)).toEqual([]);
  });

  it('rejects a malformed earlier approval response without throwing', () => {
    const messages = [
      {
        role: 'assistant' as const,
        id: 'msg-malformed-earlier',
        parts: [
          {
            type: 'tool-myTool',
            toolCallId: 'earlier-call',
            state: 'approval-responded' as const,
            input: {},
          },
          {
            type: 'tool-myTool',
            toolCallId: 'latest-call',
            state: 'approval-responded' as const,
            input: {},
            approval: { id: `run-1${APPROVAL_ID_SEPARATOR}latest-call`, approved: true },
          },
        ],
      },
    ];

    expect(() => extractV6NativeApprovals(messages as any)).not.toThrow();
    expect(extractV6NativeApprovals(messages as any)).toEqual([]);
  });

  it('rejects duplicate responded identities in the trailing assistant message', () => {
    const approvalId = `run-1${APPROVAL_ID_SEPARATOR}duplicate-call`;
    const messages = [
      {
        role: 'assistant' as const,
        id: 'msg-duplicate',
        parts: [
          {
            type: 'tool-myTool',
            toolCallId: 'duplicate-call',
            state: 'approval-responded' as const,
            input: { attempt: 1 },
            approval: { id: approvalId, approved: false },
          },
          {
            type: 'tool-myTool',
            toolCallId: 'duplicate-call',
            state: 'approval-responded' as const,
            input: { attempt: 2 },
            approval: { id: approvalId, approved: true },
          },
        ],
      },
    ];

    expect(extractV6NativeApprovals(messages as any)).toEqual([]);
  });

  it('targets the answered call when another tool call is still awaiting approval', () => {
    const messages = [
      {
        role: 'assistant' as const,
        id: 'msg-parallel-approvals',
        parts: [
          {
            type: 'tool-firstTool',
            toolCallId: 'tooluse_first',
            state: 'approval-requested' as const,
            input: {},
            approval: { id: `run-shared${APPROVAL_ID_SEPARATOR}tooluse_first` },
          },
          {
            type: 'tool-secondTool',
            toolCallId: 'tooluse_second',
            state: 'approval-responded' as const,
            input: {},
            approval: {
              id: `run-shared${APPROVAL_ID_SEPARATOR}tooluse_second`,
              approved: true,
            },
          },
        ],
      },
    ];

    expect(extractV6NativeApprovals(messages as any)).toEqual([
      {
        resumeData: { approved: true },
        runId: 'run-shared',
        toolCallId: 'tooluse_second',
      },
    ]);
  });

  it('includes reason when the user denied with a reason', () => {
    const approvalId = `run-456${APPROVAL_ID_SEPARATOR}tooluse_xyz`;
    const messages = [
      {
        role: 'assistant' as const,
        id: 'msg-1',
        parts: [
          {
            type: 'tool-myTool',
            toolCallId: 'tooluse_xyz',
            state: 'approval-responded' as const,
            input: {},
            approval: { id: approvalId, approved: false, reason: 'Not safe' },
          },
        ],
      },
    ];

    const result = extractV6NativeApprovals(messages as any);

    expect(result).toEqual([
      {
        resumeData: { approved: false, reason: 'Not safe' },
        runId: 'run-456',
        toolCallId: 'tooluse_xyz',
      },
    ]);
  });

  it('omits reason when not provided', () => {
    const approvalId = `run-789${APPROVAL_ID_SEPARATOR}tooluse_abc`;
    const messages = [
      {
        role: 'assistant' as const,
        id: 'msg-1',
        parts: [
          {
            type: 'tool-myTool',
            toolCallId: 'tooluse_abc',
            state: 'approval-responded' as const,
            input: {},
            approval: { id: approvalId, approved: true },
          },
        ],
      },
    ];

    const result = extractV6NativeApprovals(messages as any);

    expect(result[0]?.resumeData).not.toHaveProperty('reason');
  });

  it('returns an empty list when no approval-responded part exists', () => {
    const messages = [
      { role: 'user' as const, id: 'msg-1', parts: [{ type: 'text', text: 'hello' }] },
      {
        role: 'assistant' as const,
        id: 'msg-2',
        parts: [
          {
            type: 'tool-myTool',
            toolCallId: 'tooluse_abc123',
            state: 'approval-requested' as const,
            input: {},
            approval: { id: 'run-123::tooluse_abc123' },
          },
        ],
      },
    ];

    expect(extractV6NativeApprovals(messages as any)).toEqual([]);
  });

  it('skips a part whose composite approval id embeds a different toolCallId', () => {
    const approvalId = `run-123${APPROVAL_ID_SEPARATOR}tooluse_other`;
    const messages = [
      {
        role: 'assistant' as const,
        id: 'msg-1',
        parts: [
          {
            type: 'tool-myTool',
            toolCallId: 'tooluse_abc123',
            state: 'approval-responded' as const,
            input: {},
            approval: { id: approvalId, approved: true },
          },
        ],
      },
    ];

    expect(extractV6NativeApprovals(messages as any)).toEqual([]);
  });

  it('returns an empty list when the approval id has no separator', () => {
    const messages = [
      {
        role: 'assistant' as const,
        id: 'msg-1',
        parts: [
          {
            type: 'tool-myTool',
            toolCallId: 'tooluse_abc123',
            state: 'approval-responded' as const,
            input: {},
            approval: { id: 'no-separator-here', approved: true },
          },
        ],
      },
    ];

    expect(extractV6NativeApprovals(messages as any)).toEqual([]);
  });

  it('collects every approval response when one assistant message has several (issue #17899)', () => {
    const messages = [
      {
        role: 'assistant' as const,
        id: 'msg-1',
        parts: [
          {
            type: 'tool-myTool',
            toolCallId: 'old-call',
            state: 'approval-responded' as const,
            input: {},
            approval: { id: `old-run${APPROVAL_ID_SEPARATOR}old-call`, approved: true },
          },
          {
            type: 'tool-myTool',
            toolCallId: 'new-call',
            state: 'approval-responded' as const,
            input: {},
            approval: { id: `new-run${APPROVAL_ID_SEPARATOR}new-call`, approved: false, reason: 'changed mind' },
          },
        ],
      },
    ];

    const result = extractV6NativeApprovals(messages as any);

    expect(result).toEqual([
      { resumeData: { approved: true }, runId: 'old-run', toolCallId: 'old-call' },
      { resumeData: { approved: false, reason: 'changed mind' }, runId: 'new-run', toolCallId: 'new-call' },
    ]);
  });

  it('does not replay an older approval when the newest responded part is structurally malformed', () => {
    const messages = [
      {
        role: 'assistant' as const,
        id: 'msg-1',
        parts: [
          {
            type: 'tool-myTool',
            toolCallId: 'old-call',
            state: 'approval-responded' as const,
            input: {},
            approval: { id: `old-run${APPROVAL_ID_SEPARATOR}old-call`, approved: true },
          },
          {
            type: 'text',
            state: 'approval-responded' as const,
            text: 'malformed approval response',
          },
        ],
      },
    ];

    expect(extractV6NativeApprovals(messages as any)).toEqual([]);
  });

  it('extracts only responses from the trailing assistant message', () => {
    const messages = [
      {
        role: 'assistant' as const,
        id: 'msg-1',
        parts: [
          {
            type: 'tool-myTool',
            toolCallId: 'old-call',
            state: 'approval-responded' as const,
            input: {},
            approval: { id: `old-run${APPROVAL_ID_SEPARATOR}old-call`, approved: true },
          },
        ],
      },
      {
        role: 'assistant' as const,
        id: 'msg-2',
        parts: [
          {
            type: 'tool-myTool',
            toolCallId: 'new-call',
            state: 'approval-responded' as const,
            input: {},
            approval: { id: `new-run${APPROVAL_ID_SEPARATOR}new-call`, approved: false },
          },
        ],
      },
    ];

    const result = extractV6NativeApprovals(messages as any);

    expect(result).toEqual([{ resumeData: { approved: false }, runId: 'new-run', toolCallId: 'new-call' }]);
  });

  it('does not consume historical responses when a user message is trailing', () => {
    const messages = [
      {
        role: 'assistant' as const,
        id: 'msg-1',
        parts: [
          {
            type: 'tool-myTool',
            toolCallId: 'old-call',
            state: 'approval-responded' as const,
            input: {},
            approval: { id: `old-run${APPROVAL_ID_SEPARATOR}old-call`, approved: true },
          },
        ],
      },
      {
        role: 'user' as const,
        id: 'msg-2',
        parts: [{ type: 'text', text: 'What happened?' }],
      },
    ];

    expect(extractV6NativeApprovals(messages as any)).toEqual([]);
  });

  it('uses only the trailing response when history repeats a toolCallId for another run', () => {
    const part = (runId: string, approved: boolean) => ({
      type: 'tool-myTool',
      toolCallId: 'shared-call',
      state: 'approval-responded' as const,
      input: {},
      approval: { id: `${runId}${APPROVAL_ID_SEPARATOR}shared-call`, approved },
    });
    const messages = [
      { role: 'assistant', id: 'msg-1', parts: [part('run-1', true)] },
      { role: 'assistant', id: 'msg-2', parts: [part('run-2', false)] },
    ];

    expect(extractV6NativeApprovals(messages as any)).toEqual([
      { resumeData: { approved: false }, runId: 'run-2', toolCallId: 'shared-call' },
    ]);
  });

  it('uses the trailing response when an exact target is repeated across messages', () => {
    const part = (approved: boolean) => ({
      type: 'tool-myTool',
      toolCallId: 'same-call',
      state: 'approval-responded' as const,
      input: {},
      approval: { id: `same-run${APPROVAL_ID_SEPARATOR}same-call`, approved },
    });
    const messages = [
      { role: 'assistant', id: 'msg-1', parts: [part(false)] },
      { role: 'assistant', id: 'msg-2', parts: [part(true)] },
    ];

    expect(extractV6NativeApprovals(messages as any)).toEqual([
      { resumeData: { approved: true }, runId: 'same-run', toolCallId: 'same-call' },
    ]);
  });
});

describe('handleChatStream v6 native approve() resume flow', () => {
  const emptyResult = () => ({
    fullStream: new ReadableStream({
      start(controller) {
        controller.close();
      },
    }),
  });

  const approvalPart = (runId: string, toolCallId: string, approved = true) => ({
    type: 'tool-myTool',
    toolCallId,
    state: 'approval-responded',
    input: {},
    approval: { id: `${runId}${APPROVAL_ID_SEPARATOR}${toolCallId}`, approved },
  });

  const requestedPart = (runId: string, toolCallId: string) => ({
    type: 'tool-myTool',
    toolCallId,
    state: 'approval-requested',
    input: {},
    approval: { id: `${runId}${APPROVAL_ID_SEPARATOR}${toolCallId}` },
  });

  const assistantMessage = (id: string, parts: any[]) => ({
    id,
    role: 'assistant',
    parts,
  });

  const resumeResult = (runId: string, toolCallId: string, output: Record<string, unknown>, finishReason = 'stop') => ({
    fullStream: new ReadableStream({
      start(controller) {
        controller.enqueue({ type: 'start', runId, from: ChunkFrom.AGENT, payload: {} });
        controller.enqueue({
          type: 'tool-result',
          runId,
          from: ChunkFrom.AGENT,
          payload: {
            toolCallId,
            toolName: 'myTool',
            args: {},
            result: output,
          },
        });
        controller.enqueue({
          type: 'finish',
          runId,
          from: ChunkFrom.AGENT,
          payload: {
            stepResult: { reason: finishReason },
            output: { usage: {} },
          },
        });
        controller.close();
      },
    }),
  });

  const terminalResult = (runId: string, type: 'error' | 'abort') => ({
    fullStream: new ReadableStream({
      start(controller) {
        controller.enqueue(
          type === 'error'
            ? {
                type,
                runId,
                from: ChunkFrom.AGENT,
                payload: { error: new Error('resume failed') },
              }
            : { type, runId, from: ChunkFrom.AGENT, payload: {} },
        );
        controller.close();
      },
    }),
  });

  const mockAgent = {
    stream: vi.fn(),
    resumeStream: vi.fn(),
  };

  const mockMastra = {
    getAgentById: vi.fn().mockReturnValue(mockAgent),
  };

  beforeEach(() => {
    mockAgent.stream.mockReset().mockImplementation(async () => emptyResult());
    mockAgent.resumeStream.mockReset().mockImplementation(async () => emptyResult());
    mockMastra.getAgentById.mockReset().mockReturnValue(mockAgent);
  });

  it('calls resumeStream with correct runId and resumeData when messages contain approval-responded', async () => {
    const messages = [
      assistantMessage('msg-1', [
        {
          ...approvalPart('run-123', 'tooluse_abc123'),
          input: { param: 'value' },
        },
      ]),
    ];

    await handleChatStream({
      mastra: mockMastra as any,
      agentId: 'test-agent',
      version: 'v6',
      params: { messages } as any,
    });

    expect(mockAgent.resumeStream).toHaveBeenCalledTimes(1);
    expect(mockAgent.resumeStream).toHaveBeenCalledWith(
      { approved: true },
      expect.objectContaining({ runId: 'run-123', toolCallId: 'tooluse_abc123' }),
    );
    expect(mockAgent.stream).not.toHaveBeenCalled();
  });

  it('calls stream() for a normal (non-approval) message', async () => {
    const messages = [{ id: 'msg-1', role: 'user', parts: [{ type: 'text', text: 'hello' }] }];

    await handleChatStream({
      mastra: mockMastra as any,
      agentId: 'test-agent',
      version: 'v6',
      params: { messages } as any,
    });

    expect(mockAgent.stream).toHaveBeenCalledTimes(1);
    expect(mockAgent.resumeStream).not.toHaveBeenCalled();
  });

  it('explicit resumeData/runId takes precedence over message scanning', async () => {
    const messages = [assistantMessage('msg-1', [approvalPart('run-from-msg', 'tooluse_abc123')])];

    await handleChatStream({
      mastra: mockMastra as any,
      agentId: 'test-agent',
      version: 'v6',
      params: { messages, resumeData: { approved: true }, runId: 'explicit-run' } as any,
    });

    expect(mockAgent.resumeStream).toHaveBeenCalledWith(
      { approved: true },
      expect.objectContaining({ runId: 'explicit-run' }),
    );
  });

  it('rejects an earlier-only approval response when a pending card is trailing', async () => {
    const messages = [
      assistantMessage('msg-1', [approvalPart('old-run', 'old-call')]),
      assistantMessage('msg-2', [requestedPart('new-run', 'new-call')]),
    ];

    await expect(
      handleChatStream({
        mastra: mockMastra as any,
        agentId: 'test-agent',
        version: 'v6',
        params: { messages } as any,
      }),
    ).rejects.toThrow(/cannot safely resume an approval response from an earlier assistant message/);

    expect(mockAgent.resumeStream).not.toHaveBeenCalled();
    expect(mockAgent.stream).not.toHaveBeenCalled();
  });

  it('does not consume a later user message as an approval resume', async () => {
    const messages = [
      assistantMessage('msg-1', [approvalPart('old-run', 'old-call')]),
      { id: 'msg-2', role: 'user', parts: [{ type: 'text', text: 'What happened?' }] },
    ];

    const stream = await handleChatStream({
      mastra: mockMastra as any,
      agentId: 'test-agent',
      version: 'v6',
      params: { messages } as any,
    });
    await collectChunks(stream);

    expect(mockAgent.stream).toHaveBeenCalledTimes(1);
    expect(mockAgent.resumeStream).not.toHaveBeenCalled();
  });

  it('allows a normal assistant continuation when only history contains an approval response', async () => {
    const messages = [
      assistantMessage('msg-old', [approvalPart('old-run', 'old-call')]),
      assistantMessage('msg-current', [
        {
          type: 'tool-myTool',
          toolCallId: 'client-call',
          state: 'output-available',
          input: {},
          output: { ok: true },
        },
      ]),
    ];

    const stream = await handleChatStream({
      mastra: mockMastra as any,
      agentId: 'test-agent',
      version: 'v6',
      params: { messages } as any,
    });
    await collectChunks(stream);

    expect(mockAgent.stream).toHaveBeenCalledTimes(1);
    expect(mockAgent.resumeStream).not.toHaveBeenCalled();
  });

  it('isolates a trailing approval from history that repeats the same toolCallId', async () => {
    const trailingMessage = assistantMessage('msg-current', [approvalPart('new-run', 'shared-call', false)]);
    const messages = [assistantMessage('msg-old', [approvalPart('old-run', 'shared-call')]), trailingMessage];
    mockAgent.resumeStream.mockResolvedValueOnce(resumeResult('new-run', 'shared-call', { resumed: 'new-run' }));

    const stream = await handleChatStream({
      mastra: mockMastra as any,
      agentId: 'test-agent',
      version: 'v6',
      params: { messages } as any,
    });
    const states: any[] = [];
    for await (const state of readUIMessageStream({
      message: trailingMessage as any,
      stream: stream as any,
      terminateOnError: true,
    })) {
      states.push(state);
    }

    expect(mockAgent.resumeStream).toHaveBeenCalledTimes(1);
    expect(mockAgent.resumeStream).toHaveBeenCalledWith(
      { approved: false },
      expect.objectContaining({ runId: 'new-run', toolCallId: 'shared-call' }),
    );
    expect(states.at(-1)?.parts).toContainEqual(
      expect.objectContaining({
        toolCallId: 'shared-call',
        state: 'output-available',
        output: { resumed: 'new-run' },
      }),
    );
  });

  it('rejects a malformed trailing approval response without falling through', async () => {
    const messages = [
      assistantMessage('msg-1', [
        {
          type: 'text',
          state: 'approval-responded',
          text: 'not a tool response',
        },
      ]),
    ];

    await expect(
      handleChatStream({
        mastra: mockMastra as any,
        agentId: 'test-agent',
        version: 'v6',
        params: { messages } as any,
      }),
    ).rejects.toThrow(/malformed or ambiguous/);

    expect(mockAgent.stream).not.toHaveBeenCalled();
    expect(mockAgent.resumeStream).not.toHaveBeenCalled();
  });

  it('rejects duplicate decisions for one trailing tool card without executing either', async () => {
    const messages = [
      assistantMessage('msg-1', [approvalPart('run-1', 'same-call', true), approvalPart('run-1', 'same-call', false)]),
    ];

    await expect(
      handleChatStream({
        mastra: mockMastra as any,
        agentId: 'test-agent',
        version: 'v6',
        params: { messages } as any,
      }),
    ).rejects.toThrow(/malformed or ambiguous/);

    expect(mockAgent.stream).not.toHaveBeenCalled();
    expect(mockAgent.resumeStream).not.toHaveBeenCalled();
  });

  it('surfaces the core error when no approval target can be resumed', async () => {
    mockAgent.resumeStream.mockRejectedValueOnce(
      Object.assign(new Error('target is not suspended'), { id: 'AGENT_RESUME_TOOL_CALL_NOT_SUSPENDED' }),
    );
    const messages = [assistantMessage('msg-1', [approvalPart('run-1', 'call-B')])];

    const stream = await handleChatStream({
      mastra: mockMastra as any,
      agentId: 'test-agent',
      version: 'v6',
      params: { messages } as any,
    });
    const chunks = await collectChunks(stream);

    expect(chunks).toContainEqual(expect.objectContaining({ type: 'error' }));
    expect(mockAgent.stream).not.toHaveBeenCalled();
  });

  it('resumes multiple exact targets sequentially and keeps one framed response', async () => {
    mockAgent.resumeStream
      .mockResolvedValueOnce(resumeResult('run-1', 'call-A', { resumed: 'A' }, 'length'))
      .mockResolvedValueOnce(resumeResult('run-2', 'call-B', { resumed: 'B' }));
    const trailingMessage = assistantMessage('msg-batch', [
      approvalPart('run-1', 'call-A'),
      approvalPart('run-2', 'call-B'),
    ]);

    const stream = await handleChatStream({
      mastra: mockMastra as any,
      agentId: 'test-agent',
      version: 'v6',
      params: { messages: [trailingMessage] } as any,
      messageMetadata: ({ part }: any) =>
        part?.type === 'finish' ? { finishReason: part.rawFinishReason } : undefined,
    });
    const [readerStream, rawStream] = stream.tee();
    const states: any[] = [];
    for await (const state of readUIMessageStream({
      message: trailingMessage as any,
      stream: readerStream as any,
      terminateOnError: true,
    })) {
      states.push(state);
    }
    const chunks = await collectChunks(rawStream);

    expect(mockAgent.resumeStream).toHaveBeenCalledTimes(2);
    expect(mockAgent.resumeStream).toHaveBeenNthCalledWith(
      1,
      { approved: true },
      expect.objectContaining({ runId: 'run-1', toolCallId: 'call-A' }),
    );
    expect(mockAgent.resumeStream).toHaveBeenNthCalledWith(
      2,
      { approved: true },
      expect.objectContaining({ runId: 'run-2', toolCallId: 'call-B' }),
    );
    expect(chunks[0]?.type).toBe('start');
    expect(chunks.filter(chunk => chunk.type === 'start')).toHaveLength(1);
    expect(chunks.filter(chunk => chunk.type === 'tool-output-available')).toEqual([
      expect.objectContaining({ toolCallId: 'call-A', output: { resumed: 'A' } }),
      expect.objectContaining({ toolCallId: 'call-B', output: { resumed: 'B' } }),
    ]);
    expect(chunks.filter(chunk => chunk.type === 'finish')).toEqual([
      expect.objectContaining({ messageMetadata: { finishReason: 'stop' } }),
    ]);
    expect(states.at(-1)?.parts).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          toolCallId: 'call-A',
          state: 'output-available',
          output: { resumed: 'A' },
        }),
        expect.objectContaining({
          toolCallId: 'call-B',
          state: 'output-available',
          output: { resumed: 'B' },
        }),
      ]),
    );
  });

  it('does not reuse finish metadata when the final successful resume leg is empty', async () => {
    const finishStream = {
      fullStream: new ReadableStream({
        start(controller) {
          controller.enqueue({ type: 'start', runId: 'run-1', from: ChunkFrom.AGENT, payload: {} });
          controller.enqueue({
            type: 'finish',
            runId: 'run-1',
            from: ChunkFrom.AGENT,
            payload: { stepResult: { reason: 'length' }, output: { usage: {} } },
          });
          controller.close();
        },
      }),
    };
    mockAgent.resumeStream.mockResolvedValueOnce(finishStream).mockResolvedValueOnce(emptyResult());
    const trailingMessage = assistantMessage('msg-batch', [
      approvalPart('run-1', 'call-A'),
      approvalPart('run-2', 'call-B'),
    ]);

    const stream = await handleChatStream({
      mastra: mockMastra as any,
      agentId: 'test-agent',
      version: 'v6',
      params: { messages: [trailingMessage] } as any,
      messageMetadata: ({ part }: any) =>
        part?.type === 'finish' ? { finishReason: part.rawFinishReason } : undefined,
    });
    const chunks = await collectChunks(stream);
    const finishChunks = chunks.filter(chunk => chunk.type === 'finish');

    expect(mockAgent.resumeStream).toHaveBeenCalledTimes(2);
    expect(finishChunks).toHaveLength(1);
    expect(finishChunks[0]).not.toHaveProperty('messageMetadata');
  });

  it.each(['AGENT_RESUME_TOOL_CALL_NOT_SUSPENDED', 'AGENT_RESUME_NO_SNAPSHOT_FOUND'])(
    'skips the exact stale target error %s and continues the trailing batch',
    async errorId => {
      mockAgent.resumeStream
        .mockRejectedValueOnce(Object.assign(new Error('already resolved'), { id: errorId }))
        .mockResolvedValueOnce(resumeResult('run-2', 'call-B', { resumed: 'B' }));
      const trailingMessage = assistantMessage('msg-batch', [
        approvalPart('run-1', 'call-A'),
        approvalPart('run-2', 'call-B'),
      ]);

      const stream = await handleChatStream({
        mastra: mockMastra as any,
        agentId: 'test-agent',
        version: 'v6',
        params: { messages: [trailingMessage] } as any,
      });
      const chunks = await collectChunks(stream);

      expect(mockAgent.resumeStream).toHaveBeenCalledTimes(2);
      expect(chunks).toContainEqual(
        expect.objectContaining({
          type: 'tool-output-available',
          toolCallId: 'call-B',
          output: { resumed: 'B' },
        }),
      );
    },
  );

  it('stops the trailing batch on a fatal resume error', async () => {
    mockAgent.resumeStream.mockRejectedValueOnce(
      Object.assign(new Error('wrong owner'), { id: 'AGENT_RESUME_OWNER_MISMATCH' }),
    );
    const trailingMessage = assistantMessage('msg-batch', [
      approvalPart('run-1', 'call-A'),
      approvalPart('run-2', 'call-B'),
    ]);

    const stream = await handleChatStream({
      mastra: mockMastra as any,
      agentId: 'test-agent',
      version: 'v6',
      params: { messages: [trailingMessage] } as any,
    });
    const chunks = await collectChunks(stream);

    expect(mockAgent.resumeStream).toHaveBeenCalledTimes(1);
    expect(chunks).toContainEqual(expect.objectContaining({ type: 'error' }));
  });

  it.each(['error', 'abort'] as const)('stops the trailing batch after an in-band %s chunk', async terminalType => {
    mockAgent.resumeStream.mockResolvedValueOnce(terminalResult('run-1', terminalType));
    const trailingMessage = assistantMessage('msg-batch', [
      approvalPart('run-1', 'call-A'),
      approvalPart('run-2', 'call-B'),
    ]);

    const stream = await handleChatStream({
      mastra: mockMastra as any,
      agentId: 'test-agent',
      version: 'v6',
      params: { messages: [trailingMessage] } as any,
    });
    const chunks = await collectChunks(stream);

    expect(mockAgent.resumeStream).toHaveBeenCalledTimes(1);
    expect(chunks).toContainEqual(expect.objectContaining({ type: terminalType }));
  });

  it('honors disabled start and finish framing for a trailing approval batch', async () => {
    mockAgent.resumeStream
      .mockResolvedValueOnce(resumeResult('run-1', 'call-A', { resumed: 'A' }))
      .mockResolvedValueOnce(resumeResult('run-2', 'call-B', { resumed: 'B' }));
    const trailingMessage = assistantMessage('msg-batch', [
      approvalPart('run-1', 'call-A'),
      approvalPart('run-2', 'call-B'),
    ]);

    const stream = await handleChatStream({
      mastra: mockMastra as any,
      agentId: 'test-agent',
      version: 'v6',
      params: { messages: [trailingMessage] } as any,
      sendStart: false,
      sendFinish: false,
    });
    const chunks = await collectChunks(stream);

    expect(chunks.filter(chunk => chunk.type === 'start')).toHaveLength(0);
    expect(chunks.filter(chunk => chunk.type === 'finish')).toHaveLength(0);
    expect(chunks.filter(chunk => chunk.type === 'tool-output-available')).toHaveLength(2);
  });
});

describe('tool-call-approval conversion', () => {
  it('keeps the v5 data-tool-call-approval shape', () => {
    const chunk = {
      type: 'tool-call-approval' as const,
      runId: 'run-123',
      from: ChunkFrom.AGENT,
      payload: {
        toolCallId: 'tooluse_abc123',
        toolName: 'myTool',
        args: { param: 'value' },
        resumeSchema: '{"type":"object","properties":{"approved":{"type":"boolean"}}}',
      },
    };

    const result = convertMastraChunkToAISDKv5({ chunk, mode: 'stream' }) as any;

    expect(result.type).toBe('data-tool-call-approval');
    expect(result.data.runId).toBe('run-123');
  });

  it('maps v6 approvals to both tool-approval-request and data-tool-call-approval', () => {
    const chunk = {
      type: 'tool-call-approval' as const,
      runId: 'run-123',
      from: ChunkFrom.AGENT,
      payload: {
        version: 1 as const,
        originRunId: 'run-origin',
        stepId: 'toolCallStep',
        type: 'approval' as const,
        approvalSource: 'tool-gate' as const,
        identityDigest: 'digest-123',
        resumeIdentityDigest: 'resume-digest-123',
        toolCallId: 'tooluse_abc123',
        toolName: 'myTool',
        args: { param: 'value' },
        resumeSchema: '{"type":"object","properties":{"approved":{"type":"boolean"}}}',
      },
      metadata: {
        mastra: {
          toolPayloadTransform: {
            display: {
              approval: { transformed: { param: 'redacted' } },
            },
          },
        },
      },
    };

    const result = convertMastraChunkToAISDKv6({ chunk, mode: 'stream' }) as any[];
    const expectedDataChunk = convertMastraChunkToAISDKv5({ chunk, mode: 'stream' });

    expect(Array.isArray(result)).toBe(true);
    expect(result[0]).toEqual({
      type: 'tool-approval-request',
      approvalId: 'run-123::tooluse_abc123',
      toolCallId: 'tooluse_abc123',
    });
    expect(result[1]).toMatchObject({
      type: 'data-tool-call-approval',
      id: 'tooluse_abc123',
      data: expect.objectContaining({
        state: 'data-tool-call-approval',
        runId: 'run-123',
        version: 1,
        originRunId: 'run-origin',
        stepId: 'toolCallStep',
        type: 'approval',
        approvalSource: 'tool-gate',
        identityDigest: 'digest-123',
        resumeIdentityDigest: 'resume-digest-123',
        toolCallId: 'tooluse_abc123',
      }),
    });
    expect(result[1]).toEqual(expectedDataChunk);
    expect(result[1].data.args).toEqual({ param: 'redacted' });
  });

  it('keeps v5 streaming behavior unchanged', async () => {
    const aiSdkStream = toAISdkV5Stream(createApprovalStream() as unknown as MastraModelOutput, { from: 'agent' });
    const chunks = await collectChunks(aiSdkStream);

    expect(chunks.find(chunk => chunk.type === 'data-tool-call-approval')).toBeDefined();
    expect(chunks.find(chunk => chunk.type === 'tool-approval-request')).toBeUndefined();
  });

  it('emits both tool-approval-request and data-tool-call-approval on the v6 stream', async () => {
    const aiSdkStream = toAISdkStream(createApprovalStream() as unknown as MastraModelOutput, {
      from: 'agent',
      version: 'v6',
    });
    const chunks = await collectChunks(aiSdkStream);

    expect(chunks.find(chunk => chunk.type === 'tool-input-available')).toBeDefined();
    expect(chunks.find(chunk => chunk.type === 'tool-approval-request')).toBeDefined();
    expect(chunks.find(chunk => chunk.type === 'data-tool-call-approval')).toBeDefined();
  });

  it('is interpreted by the v6 UI message reader as approval-requested', async () => {
    const aiSdkStream = toAISdkStream(createApprovalStream() as unknown as MastraModelOutput, {
      from: 'agent',
      version: 'v6',
    });

    const messages = [] as any[];
    for await (const message of readUIMessageStream({ stream: aiSdkStream as any })) {
      messages.push(message);
    }

    const lastMessage = messages.at(-1);
    expect(lastMessage?.role).toBe('assistant');

    const approvalPart = lastMessage?.parts.find(
      (part: any) => part.type === 'tool-myTool' && part.state === 'approval-requested',
    );

    expect(approvalPart).toMatchObject({
      type: 'tool-myTool',
      toolCallId: 'tooluse_abc123',
      state: 'approval-requested',
      approval: { id: 'run-123::tooluse_abc123' },
    });
  });
});
