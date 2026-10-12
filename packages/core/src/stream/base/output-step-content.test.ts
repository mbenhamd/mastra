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
import { describe, expect, it } from 'vitest';
import { z } from 'zod/v4';
import { MockLanguageModelV2, convertArrayToReadableStream } from '../../agent/__tests__/mock-model';
import { Agent } from '../../agent/agent';
import { Mastra } from '../../mastra';
import { InMemoryStore } from '../../storage';
import { createTool } from '../../tools';

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
    // step's last; the attempt keeps the partial text it wrote.
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

    expect(await stepContents(out)).toEqual([
      ['tool-call:c1', 'tool-result:c1'],
      ['text:FAILED PARTIAL'],
      ['text:recovered'],
    ]);
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
