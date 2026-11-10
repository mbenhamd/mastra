import { afterEach, describe, expect, it, vi } from 'vitest';

import { AgentChannels } from '../agent-channels';
import { getChatModule } from '../chat-lazy';
import { ChatChannelOutputProcessor, CHAT_CHANNEL_RENDER_CONTEXT_KEY } from '../output-processor';

const toolResult = (runId: string, toolCallId: string, result: unknown = 'no') =>
  ({
    type: 'tool-result',
    runId,
    from: 'AGENT',
    payload: { toolCallId, toolName: 'send', args: {}, result },
  }) as any;

function setup() {
  const channels = new AgentChannels({ adapters: {} } as any);
  const processor = new ChatChannelOutputProcessor(channels);
  const process = (part: any) => processor.processOutputStream({ part, state: {} } as any);
  const records = () => (channels as any).deniedToolCalls.size as number;
  const mark = (runId: string, toolCallId: string) => (channels as any).markToolCallDenied(runId, toolCallId);
  return { channels, process, records, mark };
}

describe('denied tool-call records', () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it('removes the record when the denial result arrives and keeps the result on the stream', async () => {
    const { process, records, mark } = setup();
    mark('run-1', 'call-1');
    const part = toolResult('run-1', 'call-1');

    await expect(process(part)).resolves.toBe(part);

    expect(records()).toBe(0);

    // An earlier output processor may have translated the denial text.
    mark('run-1', 'call-2');
    await process(toolResult('run-1', 'call-2', 'El usuario no aprobó la herramienta'));
    expect(records()).toBe(0);
  });

  it('keeps the record for a tool result from another run that reuses the call id', async () => {
    const { channels, process, records, mark } = setup();
    mark('run-1', 'call-1');

    await process(toolResult('run-2', 'call-1'));

    expect(records()).toBe(1);
    channels.close();
  });

  it.each([
    ['abort', { type: 'abort', runId: 'run-1', payload: {} }],
    ['error', { type: 'error', runId: 'run-1', payload: { error: 'boom' } }],
    ['terminal step', { type: 'step-finish', runId: 'run-1', payload: { stepResult: { isContinued: false } } }],
  ])('removes the record when the run ends with %s before any result', async (_name, part) => {
    const { process, records, mark } = setup();
    mark('run-1', 'call-1');

    await process(part);

    expect(records()).toBe(0);
  });

  it('removes the record of a run the output processor never sees', () => {
    vi.useFakeTimers();
    const { records, mark } = setup();
    mark('run-1', 'call-1');

    vi.advanceTimersByTime(24 * 60 * 60 * 1000);

    expect(records()).toBe(0);
    expect(vi.getTimerCount()).toBe(0);
  });

  /** Drives a run through a channel thread and returns the messages the bot edited. */
  async function renderRun(mark: (channels: AgentChannels) => void, chunks: any[]) {
    await getChatModule();
    const edits: unknown[] = [];
    let nextId = 1;
    const adapter: any = {
      name: 'test',
      editMessage: vi.fn(async (_thread: string, _id: string, content: unknown) => edits.push(content)),
      renderFormatted: (t: string) => t,
      startTyping: vi.fn().mockResolvedValue(undefined),
    };
    const chatThread: any = {
      id: 'test:c1:t1',
      channelId: 'test:c1',
      isDM: false,
      post: vi.fn(async () => ({ id: `m${nextId++}`, text: '' })),
      startTyping: vi.fn().mockResolvedValue(undefined),
    };
    const channels = new AgentChannels({ adapters: { test: { adapter, streaming: false } as any } });
    mark(channels);
    const processor = new ChatChannelOutputProcessor(channels);
    const render = (channels as any)._buildRenderContext(chatThread, 'test', {});
    const requestContext = { get: (key: string) => (key === CHAT_CHANNEL_RENDER_CONTEXT_KEY ? render : undefined) };
    const state = {};
    for (const part of chunks) await processor.processOutputStream({ part, state, requestContext } as any);
    channels.close();
    return edits;
  }

  const toolCall = (toolCallId: string, toolName: string) =>
    ({ type: 'tool-call', runId: 'run-1', from: 'AGENT', payload: { toolCallId, toolName, args: {} } }) as any;
  const runEnd = { type: 'step-finish', runId: 'run-1', payload: { stepResult: { isContinued: false } } } as any;

  it('still suppresses a denial result that arrives after a long tool-call batch', async () => {
    const denial = 'The user declined this call.';
    const edits = await renderRun(
      channels => {
        vi.useFakeTimers();
        (channels as any).markToolCallDenied('run-1', 'call-1');
        vi.advanceTimersByTime(10 * 60 * 1000 + 1);
        vi.useRealTimers();
      },
      [toolCall('call-1', 'send'), toolResult('run-1', 'call-1', denial), runEnd],
    );

    expect(JSON.stringify(edits)).not.toContain(denial);
  });
});
