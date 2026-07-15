import { Agent } from '@mastra/core/agent';
import { Harness } from '@mastra/core/harness/v1';
import { MastraLanguageModelV2Mock } from '@mastra/core/test-utils/llm-mock';
import { createTool } from '@mastra/core/tools';
import { LibSQLStore } from '@mastra/libsql';
import { describe, it, expect, vi } from 'vitest';
import z from 'zod';

vi.setConfig({ testTimeout: 30_000 });

function createToolCallStream() {
  return new ReadableStream({
    start(controller) {
      controller.enqueue({ type: 'stream-start', warnings: [] });
      controller.enqueue({
        type: 'response-metadata',
        id: 'id-0',
        modelId: 'mock',
        timestamp: new Date(0),
      });
      controller.enqueue({
        type: 'tool-call',
        toolCallId: 'call-1',
        toolName: 'readFile',
        input: '{"path":"test.txt"}',
        providerExecuted: false,
      });
      controller.enqueue({
        type: 'finish',
        finishReason: 'tool-calls',
        usage: { inputTokens: 10, outputTokens: 20, totalTokens: 30 },
      });
      controller.close();
    },
  });
}

function createTextStream() {
  return new ReadableStream({
    start(controller) {
      controller.enqueue({ type: 'stream-start', warnings: [] });
      controller.enqueue({
        type: 'response-metadata',
        id: 'id-1',
        modelId: 'mock',
        timestamp: new Date(0),
      });
      controller.enqueue({ type: 'text-start', id: 'text-1' });
      controller.enqueue({ type: 'text-delta', id: 'text-1', delta: 'File contents here' });
      controller.enqueue({ type: 'text-end', id: 'text-1' });
      controller.enqueue({
        type: 'finish',
        finishReason: 'stop',
        usage: { inputTokens: 10, outputTokens: 20, totalTokens: 30 },
      });
      controller.close();
    },
  });
}

describe('tool approval with LibSQLStore via Harness', () => {
  it('should surface a tool approval request with LibSQLStore-backed Harness state', async () => {
    const mockExecute = vi.fn().mockResolvedValue({ content: 'file contents' });

    const readFileTool = createTool({
      id: 'readFile',
      description: 'Read a file',
      inputSchema: z.object({ path: z.string() }),
      requireApproval: true,
      execute: async input => mockExecute(input),
    });

    const storage = new LibSQLStore({
      id: 'test-store',
      url: 'file::memory:?cache=shared',
    });
    await storage.init();

    const agent = new Agent({
      id: 'test-agent',
      name: 'Test Agent',
      instructions: 'You read files.',
      model: new MastraLanguageModelV2Mock({
        doStream: (() => {
          let callCount = 0;
          return async () => {
            callCount++;
            return { stream: callCount === 1 ? createToolCallStream() : createTextStream() };
          };
        })(),
      }) as any,
      tools: { readFile: readFileTool },
    });

    const harness = new Harness({
      agents: { 'test-agent': agent },
      storage,
      modes: [
        {
          id: 'default',
          agentId: 'test-agent',
        },
      ],
      defaultModeId: 'default',
    });
    try {
      const session = await harness.session({
        resourceId: 'libsql-tool-approval',
        threadId: { fresh: true },
      });
      const events: any[] = [];
      session.subscribe(event => {
        events.push(event);
      });

      const suspended = await session.message({ content: 'Read test.txt' });

      expect(suspended.finishReason).toBe('suspended');
      expect(events.some(event => event.type === 'tool_approval_required')).toBe(true);

      await session.respondToToolApproval({ approved: true });
      expect(mockExecute).toHaveBeenCalledOnce();
    } finally {
      await harness.shutdown();
    }
  });
});
