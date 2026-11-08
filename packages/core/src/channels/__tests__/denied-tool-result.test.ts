import { afterEach, describe, expect, it, vi } from 'vitest';

import { AgentChannels } from '../agent-channels';
import { ChatChannelOutputProcessor } from '../output-processor';

const toolResult = (runId: string, toolCallId: string) =>
  ({
    type: 'tool-result',
    runId,
    from: 'AGENT',
    payload: { toolCallId, toolName: 'send', args: {}, result: 'no' },
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

    vi.advanceTimersByTime(10 * 60 * 1000);

    expect(records()).toBe(0);
    expect(vi.getTimerCount()).toBe(0);
  });
});
