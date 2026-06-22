import type { UIMessage as V5UIMessage } from '@internal/ai-sdk-v5';
import type { UIMessage as V6UIMessage } from '@internal/ai-v6';
import type { MastraModelOutput } from '@mastra/core/stream';
import { describe, expect, it, vi } from 'vitest';

import { HarnessChatStreamValidationError, handleHarnessChatStream } from '../chat-route';

async function collectChunks(stream: ReadableStream) {
  const chunks: any[] = [];
  for await (const chunk of stream as any) {
    chunks.push(chunk);
  }
  return chunks;
}

function createHarnessOutput(text = 'Harness says hello'): MastraModelOutput {
  return {
    fullStream: new ReadableStream({
      start(controller) {
        controller.enqueue({
          type: 'start',
          runId: 'harness-run-1',
          payload: { id: 'harness-message-1' },
        });
        controller.enqueue({
          type: 'text-start',
          runId: 'harness-run-1',
          payload: { id: 'text-1', providerMetadata: undefined },
        });
        controller.enqueue({
          type: 'text-delta',
          runId: 'harness-run-1',
          payload: { id: 'text-1', text, providerMetadata: undefined },
        });
        controller.enqueue({
          type: 'text-end',
          runId: 'harness-run-1',
          payload: { id: 'text-1', providerMetadata: undefined },
        });
        controller.enqueue({
          type: 'finish',
          runId: 'harness-run-1',
          payload: {
            stepResult: { reason: 'stop' },
            output: { usage: { inputTokens: 2, outputTokens: 3, totalTokens: 5 } },
          },
        });
        controller.close();
      },
    }),
  } as MastraModelOutput;
}

function createSession(output: MastraModelOutput = createHarnessOutput()) {
  return {
    abort: vi.fn(),
    message: vi.fn(async () => output),
  };
}

describe('handleHarnessChatStream', () => {
  it('admits the trailing user message through a Harness session with the UI message id as admissionId', async () => {
    const session = createSession();
    const messages: V5UIMessage[] = [{ id: 'user-1', role: 'user', parts: [{ type: 'text', text: 'Hello Harness' }] }];

    const stream = await handleHarnessChatStream({
      session,
      params: {
        messages,
        mode: 'research',
        requestContext: { app: { tenant: 'papersflow', nested: { ok: true } } },
      },
    });
    const chunks = await collectChunks(stream);

    expect(session.message).toHaveBeenCalledTimes(1);
    expect(session.message).toHaveBeenCalledWith({
      admissionId: 'user-1',
      content: 'Hello Harness',
      mode: 'research',
      requestContext: { app: { tenant: 'papersflow', nested: { ok: true } } },
      stream: true,
    });
    expect(chunks.some(chunk => JSON.stringify(chunk).includes('Harness says hello'))).toBe(true);
  });

  it('returns AI SDK v6 UI stream chunks when version v6 is requested', async () => {
    const session = createSession(createHarnessOutput('v6 hello'));
    const messages: V6UIMessage[] = [{ id: 'user-v6', role: 'user', parts: [{ type: 'text', text: 'Hello v6' }] }];

    const stream = await handleHarnessChatStream({
      session,
      params: { messages },
      version: 'v6',
    });
    const chunks = await collectChunks(stream);

    expect(session.message.mock.calls[0]?.[0]).toMatchObject({
      admissionId: 'user-v6',
      content: 'Hello v6',
      stream: true,
    });
    expect(chunks.some(chunk => JSON.stringify(chunk).includes('v6 hello'))).toBe(true);
  });

  it('rejects infrastructure-owned requestContext keys before Harness admission', async () => {
    const session = createSession();
    const messages: V5UIMessage[] = [{ id: 'user-1', role: 'user', parts: [{ type: 'text', text: 'Hello' }] }];

    await expect(
      handleHarnessChatStream({
        session,
        params: {
          messages,
          requestContext: { app: {}, harness: { sessionId: 'forged' } },
        },
      }),
    ).rejects.toBeInstanceOf(HarnessChatStreamValidationError);
    expect(session.message).not.toHaveBeenCalled();
  });

  it('normalizes requestContext.app with Harness canonical JSON rules before admission', async () => {
    const session = createSession();
    const messages: V5UIMessage[] = [{ id: 'user-1', role: 'user', parts: [{ type: 'text', text: 'Hello' }] }];

    const stream = await handleHarnessChatStream({
      session,
      params: {
        messages,
        requestContext: { app: { a: 1, b: undefined, c: null } },
      },
    });
    await collectChunks(stream);

    expect(session.message.mock.calls[0]?.[0].requestContext).toEqual({ app: { a: 1, c: null } });
  });

  it('rejects non-canonical requestContext.app values before Harness admission', async () => {
    const session = createSession();
    const messages: V5UIMessage[] = [{ id: 'user-1', role: 'user', parts: [{ type: 'text', text: 'Hello' }] }];

    await expect(
      handleHarnessChatStream({
        session,
        params: {
          messages,
          requestContext: { app: { z: -0 } },
        },
      }),
    ).rejects.toBeInstanceOf(HarnessChatStreamValidationError);
    expect(session.message).not.toHaveBeenCalled();
  });

  it('omits admissionId for regenerate requests so Harness creates a fresh turn', async () => {
    const session = createSession();
    const messages: V6UIMessage[] = [
      { id: 'user-1', role: 'user', parts: [{ type: 'text', text: 'Try again' }] },
      { id: 'assistant-1', role: 'assistant', parts: [{ type: 'text', text: 'Old answer' }] },
    ];

    const stream = await handleHarnessChatStream({
      session,
      params: {
        messages,
        trigger: 'regenerate-message',
      },
      version: 'v6',
    });
    await collectChunks(stream);

    expect(session.message).toHaveBeenCalledWith({
      content: 'Try again',
      stream: true,
    });
  });

  it('rejects per-turn additionalTools on normal submits before defaulting an admissionId', async () => {
    const session = createSession();
    const messages: V5UIMessage[] = [{ id: 'user-1', role: 'user', parts: [{ type: 'text', text: 'Hello' }] }];

    await expect(
      handleHarnessChatStream({
        session,
        params: {
          additionalTools: { extra: {} },
          messages,
        },
      }),
    ).rejects.toBeInstanceOf(HarnessChatStreamValidationError);
    expect(session.message).not.toHaveBeenCalled();
  });

  it('rejects file parts instead of silently dropping them before Harness admission', async () => {
    const session = createSession();
    const messages = [
      {
        id: 'user-1',
        role: 'user',
        parts: [
          { type: 'text', text: 'Summarize this' },
          { type: 'file', mediaType: 'text/plain', url: 'data:text/plain;base64,SGk=' },
        ],
      },
    ] as V5UIMessage[];

    await expect(
      handleHarnessChatStream({
        session,
        params: { messages },
      }),
    ).rejects.toMatchObject({
      path: 'handleHarnessChatStream.params.messages.parts[1]',
    });
    expect(session.message).not.toHaveBeenCalled();
  });

  it('does not turn consumer stream cancellation into a Harness abort', async () => {
    const session = createSession({
      fullStream: new ReadableStream({
        start(controller) {
          controller.enqueue({
            type: 'start',
            runId: 'harness-run-1',
            payload: { id: 'harness-message-1' },
          });
        },
      }),
    } as MastraModelOutput);
    const messages: V5UIMessage[] = [{ id: 'user-1', role: 'user', parts: [{ type: 'text', text: 'Hello' }] }];

    const stream = await handleHarnessChatStream({
      session,
      params: { messages },
    });
    const reader = stream.getReader();
    await reader.read();
    await reader.cancel('client disconnected');

    expect(session.message.mock.calls[0]?.[0].abortSignal).toBeUndefined();
    expect(session.abort).not.toHaveBeenCalled();
  });

  it('forwards an explicit abortSignal as a caller-owned Harness turn abort', async () => {
    const session = createSession();
    const controller = new AbortController();
    const messages: V5UIMessage[] = [{ id: 'user-1', role: 'user', parts: [{ type: 'text', text: 'Hello' }] }];

    const stream = await handleHarnessChatStream({
      session,
      params: { messages, abortSignal: controller.signal },
    });
    await collectChunks(stream);

    expect(session.message.mock.calls[0]?.[0].abortSignal).toBe(controller.signal);
  });
});
