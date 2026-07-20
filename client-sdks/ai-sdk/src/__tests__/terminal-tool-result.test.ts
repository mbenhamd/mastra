import { ReadableStream } from 'node:stream/web';
import type { MastraModelOutput } from '@mastra/core/stream';
import { describe, expect, it } from 'vitest';
import { toAISdkStream, toAISdkV5Stream } from '../convert-streams';

const terminalResult = {
  status: 'success' as const,
  items: [
    {
      toolName: 'repairLatex',
      toolCallId: 'call-1',
      status: 'success' as const,
      value: { compiled: true, changedFiles: ['main.tex'] },
    },
  ],
};

function createTerminalResultStream() {
  return new ReadableStream({
    start(controller) {
      controller.enqueue({
        type: 'start',
        runId: 'run-1',
        payload: { id: 'message-1' },
      });
      controller.enqueue({
        type: 'data-terminal-tool-result',
        id: 'run-1:terminal-tool-result:1',
        data: terminalResult,
      });
      controller.enqueue({
        type: 'finish',
        runId: 'run-1',
        payload: {
          stepResult: { reason: 'tool-calls', warnings: [] },
          output: { usage: { inputTokens: 10, outputTokens: 5, totalTokens: 15 } },
        },
      });
      controller.close();
    },
  });
}

async function collect(stream: ReadableStream) {
  const chunks: any[] = [];
  for await (const chunk of stream) chunks.push(chunk);
  return chunks;
}

describe('terminal tool result transport', () => {
  it('preserves the bounded terminal result in the AI SDK v5 stream', async () => {
    const chunks = await collect(
      toAISdkV5Stream(createTerminalResultStream() as unknown as MastraModelOutput, { from: 'agent' }),
    );

    expect(chunks).toContainEqual({
      type: 'data-terminal-tool-result',
      id: 'run-1:terminal-tool-result:1',
      data: terminalResult,
    });
    expect(chunks.at(-1)?.type).toBe('finish');
  });

  it('preserves the bounded terminal result in the AI SDK v6 stream', async () => {
    const chunks = await collect(
      toAISdkStream(createTerminalResultStream() as unknown as MastraModelOutput, {
        from: 'agent',
        version: 'v6',
      }),
    );

    expect(chunks).toContainEqual({
      type: 'data-terminal-tool-result',
      id: 'run-1:terminal-tool-result:1',
      data: terminalResult,
    });
    expect(chunks.at(-1)?.type).toBe('finish');
  });
});
