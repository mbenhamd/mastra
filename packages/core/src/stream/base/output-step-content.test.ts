/**
 * PF-5399: each entry of the public `out.steps` holds only the content its own step wrote.
 *
 * `MastraModelOutput` used to rebuild a step's content at `step-finish` by counting
 * `step-start` markers back from the end of the response. That reads another step's parts
 * whenever the step has no marker of its own: the answer after a resumed tool approval read
 * as the approved tool call and its result, and a rolled-back retry attempt read as the tool
 * step before it. A feedback note added after a step was also read as that step's text. The
 * loop's StepResult already holds the right content; these cases pin that `out.steps` agrees,
 * except that a rolled-back attempt reports no content, as its parts left the response.
 */
import { ReadableStream } from 'node:stream/web';
import { describe, expect, it, vi } from 'vitest';
import { z } from 'zod/v4';
import { MockLanguageModelV2, convertArrayToReadableStream } from '../../agent/__tests__/mock-model';
import { Agent } from '../../agent/agent';
import { MessageList } from '../../agent/message-list';
import { Mastra } from '../../mastra';
import { InMemoryStore } from '../../storage';
import { createTool } from '../../tools';
import { ChunkFrom } from '../types';
import type { ChunkType } from '../types';
import { MastraModelOutput } from './output';

const usage = { inputTokens: 1, outputTokens: 1, totalTokens: 2 };
const finish = (finishReason: string) => ({ type: 'finish', finishReason, usage });
const text = (delta: string) => [
  { type: 'text-start', id: 't' },
  { type: 'text-delta', id: 't', delta },
  { type: 'text-end', id: 't' },
];
const toolCall = (toolCallId: string, toolName = 'echo') => ({ type: 'tool-call', toolCallId, toolName, input: '{}' });

/** A model that answers each call with the next scripted chunk list. */
function scriptedModel(script: Array<Array<Record<string, unknown>>>) {
  let calls = 0;
  return new MockLanguageModelV2({
    doStream: async () => {
      const chunks = script[calls] ?? [...text('unexpected extra call'), finish('stop')];
      calls += 1;
      return {
        rawCall: { rawPrompt: null, rawSettings: {} },
        warnings: [],
        stream: convertArrayToReadableStream([{ type: 'stream-start', warnings: [] }, ...chunks] as any),
      };
    },
  });
}

const tools = {
  echo: createTool({ id: 'echo', description: 'echo', inputSchema: z.object({}), execute: async () => 'echoed' }),
  apply: createTool({
    id: 'apply',
    description: 'apply',
    inputSchema: z.object({}),
    requireApproval: true,
    execute: async () => 'applied',
  }),
};

/** A loop step-finish chunk that carries the given loop StepResults. */
function stepFinishChunk(steps: Array<{ content: Array<Record<string, unknown>> }>, reason: string): ChunkType {
  return {
    type: 'step-finish',
    runId: 'r',
    from: ChunkFrom.AGENT,
    payload: {
      id: 'step',
      output: { steps, usage },
      stepResult: { reason, warnings: [], isContinued: reason === 'retry' },
      metadata: {},
      messages: { nonUser: [], all: [] },
    },
  } as unknown as ChunkType;
}

function finishChunk(): ChunkType {
  return {
    type: 'finish',
    runId: 'r',
    from: ChunkFrom.AGENT,
    payload: {
      id: 'finish',
      output: { steps: [], usage },
      stepResult: { reason: 'stop', warnings: [], isContinued: false },
      metadata: {},
      messages: { nonUser: [], all: [] },
    },
  } as unknown as ChunkType;
}

function modelOutput(chunks: ChunkType[], messageList: MessageList, initialState?: unknown) {
  return new MastraModelOutput({
    model: { modelId: 'test-model', provider: 'test', version: 'v2' },
    stream: new ReadableStream<ChunkType>({
      start(controller) {
        for (const chunk of chunks) controller.enqueue(chunk);
        controller.close();
      },
    }),
    messageList,
    messageId: 'msg-1',
    options: { runId: 'r' },
    initialState,
  });
}

/** Each step's content as part type plus the tool call id or text it carries. */
async function stepContents(out: { steps: Promise<Array<{ content: Array<any> }>> }) {
  return (await out.steps).map(step => step.content.map(part => `${part.type}:${part.toolCallId ?? part.text ?? ''}`));
}

describe('PF-5399 public step content', () => {
  it('keeps an approved tool call out of the answer step that follows it', async () => {
    const agent = new Agent({
      id: 'a',
      name: 'a',
      instructions: 'test',
      model: scriptedModel([
        [toolCall('c1', 'apply'), finish('tool-calls')],
        [...text('answer'), finish('stop')],
      ]),
      tools,
    });
    const mastra = new Mastra({ agents: { a: agent }, logger: false, storage: new InMemoryStore() });
    const registered = mastra.getAgent('a');

    const first = await registered.stream('go');
    await first.consumeStream();
    const resumed = await registered.approveToolCall({ runId: first.runId, toolCallId: 'c1' });
    await resumed.consumeStream();

    expect(await stepContents(resumed)).toEqual([['tool-call:c1', 'tool-result:c1'], ['text:answer']]);
  });

  it('gives a rolled-back retry attempt no content, not the tool step before it', async () => {
    const agent = new Agent({
      id: 'a',
      name: 'a',
      instructions: 'test',
      model: scriptedModel([
        [toolCall('c1'), finish('tool-calls')],
        [...text('REJECTED'), finish('stop')],
        [...text('answer'), finish('stop')],
      ]),
      tools,
      outputProcessors: [
        {
          id: 'reject-once',
          processOutputStep: ({ text: stepText, abort, retryCount, messageList }: any) => {
            if (retryCount === 0 && (stepText ?? '').includes('REJECTED')) abort('Answer again.', { retry: true });
            return messageList;
          },
        },
      ] as any,
      maxProcessorRetries: 2,
    });

    const out = await agent.stream('go');
    await out.consumeStream();
    const steps = await out.steps;

    // The rejected attempt was rolled out of the response, so like its text it reports nothing.
    expect(await stepContents(out)).toEqual([['tool-call:c1', 'tool-result:c1'], [], ['text:answer']]);
    expect(steps.map(step => step.text)).toEqual(['', '', 'answer']);
  });

  it('keeps the tool step out of an attempt retried after an API error', async () => {
    // The retried attempt adds no StepResult to the loop, so its step-finish carries the tool
    // step's last. Upstream #26654 drops a failed attempt's partial text when an API error is
    // retried, so the attempt reports no content (and never the tool step's).
    const agent = new Agent({
      id: 'a',
      name: 'a',
      instructions: 'test',
      model: scriptedModel([
        [toolCall('c1'), finish('tool-calls')],
        [...text('FAILED PARTIAL'), { type: 'error', error: new Error('retry me') }, finish('error')],
        [...text('recovered'), finish('stop')],
      ]),
      tools,
      maxProcessorRetries: 1,
      errorProcessors: [{ id: 'retry', processAPIError: () => ({ retry: true }) }] as any,
    });

    const out = await agent.stream('go');
    await out.consumeStream();

    expect(await stepContents(out)).toEqual([['tool-call:c1', 'tool-result:c1'], [], ['text:recovered']]);
  });

  it.each([
    ['carries the loop step count', (state: Record<string, unknown>) => state],
    ['predates the loop step count', ({ loopStepCount: _count, ...state }: Record<string, unknown>) => state],
  ])('keeps the previous step out of an API-error retry after restoring a state that %s', async (_label, saved) => {
    const toolStep = { content: [{ type: 'text', text: 'loop step one' }] };
    const first = modelOutput([stepFinishChunk([toolStep], 'tool-calls'), finishChunk()], new MessageList());
    await first.consumeStream();
    expect((await first.steps).map(step => step.content)).toEqual([toolStep.content]);
    const state = saved(JSON.parse(JSON.stringify(first.serializeState())));

    // The attempt retried after an API error adds no loop StepResult, so its chunk carries the
    // same steps as before the state was saved.
    const messageList = new MessageList();
    const restored = modelOutput([stepFinishChunk([toolStep], 'retry'), finishChunk()], messageList, state);
    messageList.add({ role: 'assistant', content: [{ type: 'text', text: 'FAILED PARTIAL' }] }, 'response');
    await restored.consumeStream();

    const retried = (await restored.steps).at(-1)!;
    expect(retried.content).not.toContainEqual(toolStep.content[0]);
    // Upstream #26654: a retried attempt contributes no content.
    expect(retried.content).toEqual([]);
  });

  it('reads the answer after resuming a state saved before the loop step count, past API-error retries', async () => {
    // A release before the count saved no `loopStepCount`. An attempt retried after an API error
    // has a public step but no loop StepResult, so the count derived on restore must leave it out.
    const original = MastraModelOutput.prototype.serializeState;
    const serializeState = vi
      .spyOn(MastraModelOutput.prototype, 'serializeState')
      .mockImplementation(function (this: MastraModelOutput<any>) {
        const { loopStepCount: _count, ...state } = original.call(this);
        return state as ReturnType<typeof original>;
      });
    try {
      const agent = new Agent({
        id: 'a',
        name: 'a',
        instructions: 'test',
        model: scriptedModel([
          [toolCall('c0'), finish('tool-calls')],
          [...text('FAILED PARTIAL'), { type: 'error', error: new Error('retry me') }, finish('error')],
          [...text('FAILED AGAIN'), { type: 'error', error: new Error('retry me') }, finish('error')],
          [toolCall('c1', 'apply'), finish('tool-calls')],
          [...text('answer'), finish('stop')],
        ]),
        tools,
        maxProcessorRetries: 2,
        errorProcessors: [{ id: 'retry', processAPIError: () => ({ retry: true }) }] as any,
      });
      const mastra = new Mastra({ agents: { a: agent }, logger: false, storage: new InMemoryStore() });
      const registered = mastra.getAgent('a');

      const first = await registered.stream('go');
      await first.consumeStream();
      const resumed = await registered.approveToolCall({ runId: first.runId, toolCallId: 'c1' });
      await resumed.consumeStream();

      expect(serializeState).toHaveBeenCalled();
      expect((await stepContents(resumed)).slice(-2)).toEqual([['tool-call:c1', 'tool-result:c1'], ['text:answer']]);
    } finally {
      serializeState.mockRestore();
    }
  });

  it('leaves a feedback note added after a step out of that step', async () => {
    const agent = new Agent({
      id: 'a',
      name: 'a',
      instructions: 'test',
      model: scriptedModel([
        [toolCall('c1'), finish('tool-calls')],
        [...text('first'), finish('stop')],
        [...text('answer'), finish('stop')],
      ]),
      tools,
    });
    let iterations = 0;

    const out = await agent.stream('go', {
      onIterationComplete: () => (++iterations === 2 ? { continue: true, feedback: 'Say more.' } : undefined),
    });
    await out.consumeStream();

    expect(await stepContents(out)).toEqual([['tool-call:c1', 'tool-result:c1'], ['text:first'], ['text:answer']]);
  });
});
