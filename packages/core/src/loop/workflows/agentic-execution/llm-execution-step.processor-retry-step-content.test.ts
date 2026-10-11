/**
 * PF-5364: a step's content after a processor retry.
 *
 * A granted retry rolls the rejected attempt's parts back, including the `step-start`
 * marker that opened them, but keeps its StepResult in `steps`; the retry then streams
 * into a fresh response message after the feedback signal. Step content is found by
 * counting markers, so both sides of that have to line up, or the repaired step reads
 * as empty (Harness then treats the turn as silent and answers twice) or as another
 * step's parts. These cases also pin the paths an earlier, location-based attempt at the
 * fix broke (failed partial attempts, denied retries, cloned messages, tool results
 * stored in a fresh message).
 */
import { describe, expect, it } from 'vitest';
import { z } from 'zod/v4';
import { MockLanguageModelV2, convertArrayToReadableStream } from '../../../agent/__tests__/mock-model';
import { Agent } from '../../../agent/agent';
import { createTool } from '../../../tools';

const usage = { inputTokens: 1, outputTokens: 1, totalTokens: 2 };
const finish = (finishReason: string) => ({ type: 'finish', finishReason, usage });
const text = (delta: string) => [
  { type: 'text-start', id: 't' },
  { type: 'text-delta', id: 't', delta },
  { type: 'text-end', id: 't' },
];
const toolCall = (toolCallId: string) => ({ type: 'tool-call', toolCallId, toolName: 'echo', input: '{}' });

/** A model that answers each call with the next scripted chunk list. */
function scriptedModel(script: Array<Array<Record<string, unknown>>>) {
  let calls = 0;
  const model = new MockLanguageModelV2({
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
  return { model, calls: () => calls };
}

function echoTool(execute: () => Promise<unknown> = async () => 'done') {
  return createTool({ id: 'echo', description: 'echo', inputSchema: z.object({}), execute });
}

/** Rejects, once, any answer that contains REJECTED. */
const rejectOnce = {
  id: 'reject-once',
  processOutputStep: ({ text: stepText, abort, retryCount, messageList }: any) => {
    if (retryCount === 0 && (stepText ?? '').includes('REJECTED')) abort('Answer again.', { retry: true });
    return messageList;
  },
};

describe('PF-5364 step content after a processor retry', () => {
  it('reads the repaired answer of a retry on the first iteration', async () => {
    const { model } = scriptedModel([
      [...text('REJECTED'), finish('stop')],
      [...text('ACCEPTED'), finish('stop')],
    ]);
    const iterationTexts: string[] = [];
    const agent = new Agent({
      id: 'a',
      name: 'a',
      instructions: 'test',
      model,
      outputProcessors: [rejectOnce] as any,
      maxProcessorRetries: 2,
    });
    const stream = await agent.stream('go', {
      onIterationComplete: ({ text: iterationText }) => {
        iterationTexts.push(iterationText);
        return undefined;
      },
    });
    await stream.consumeStream();

    expect(iterationTexts).toEqual(['', 'ACCEPTED']);
  });

  it('shows the next step the results of a tool step that follows a retry', async () => {
    const { model, calls } = scriptedModel([
      [...text('REJECTED'), finish('stop')],
      [toolCall('c1'), finish('tool-calls')],
      [...text('done'), finish('stop')],
    ]);
    const toolResultsSeen: number[] = [];
    const agent = new Agent({
      id: 'a',
      name: 'a',
      instructions: 'test',
      model,
      tools: { echo: echoTool() },
      outputProcessors: [rejectOnce] as any,
      maxProcessorRetries: 2,
    });
    const stream = await agent.stream('go', {
      prepareStep: ({ stepNumber, steps }) => {
        if (stepNumber === 2) toolResultsSeen.push(steps.at(-1)?.toolResults.length ?? -1);
        return undefined;
      },
    });
    await stream.consumeStream();

    expect(calls()).toBe(3);
    expect(toolResultsSeen).toEqual([1]);
  });

  it('keeps the results of a tool step whose retry was requested but not granted', async () => {
    const { model } = scriptedModel([[toolCall('c1'), finish('tool-calls')]]);
    let executions = 0;
    const agent = new Agent({
      id: 'a',
      name: 'a',
      instructions: 'test',
      model,
      tools: {
        echo: echoTool(async () => {
          executions += 1;
          return 'done';
        }),
      },
      maxProcessorRetries: 0,
      outputProcessors: [{ id: 'reject', processOutputStep: ({ abort }: any) => abort('no', { retry: true }) }] as any,
    });
    const stream = await agent.stream('go');
    let finishedSteps: any[] = [];
    for await (const chunk of stream.fullStream as AsyncIterable<any>) {
      if (chunk.type === 'step-finish') finishedSteps = chunk.payload.output.steps;
    }

    expect(executions).toBe(1);
    expect(finishedSteps[0]?.tripwire).toMatchObject({ retry: true });
    expect(finishedSteps[0]?.tripwire?.rolledBack).toBeUndefined();
    expect(finishedSteps[0]?.toolResults).toHaveLength(1);
  });

  it('does not credit a failed partial attempt to the accepted tool step before it', async () => {
    const { model, calls } = scriptedModel([
      [toolCall('c1'), finish('tool-calls')],
      [...text('FAILED PARTIAL'), { type: 'error', error: new Error('retry me') }, finish('error')],
      [...text('recovered'), finish('stop')],
    ]);
    const toolStepTexts: string[] = [];
    const agent = new Agent({
      id: 'a',
      name: 'a',
      instructions: 'test',
      model,
      tools: { echo: echoTool() },
      maxProcessorRetries: 1,
      errorProcessors: [{ id: 'retry', processAPIError: () => ({ retry: true }) }] as any,
      inputProcessors: [
        {
          id: 'spy',
          processInputStep: ({ steps }: any) => {
            if (steps.length > 0) toolStepTexts.push(steps[0].text);
            return {};
          },
        },
      ] as any,
    });
    const stream = await agent.stream('go');
    await stream.consumeStream();

    expect(calls()).toBe(3);
    expect(toolStepTexts).toEqual(['', '']);
  });

  it('reads the final answer after a retry when a processor clones the conversation and drops tool parts', async () => {
    const { model, calls } = scriptedModel([
      [toolCall('c1'), finish('tool-calls')],
      [...text('REJECTED'), finish('stop')],
      [toolCall('c2'), finish('tool-calls')],
      [...text('FINAL ANSWER'), finish('stop')],
    ]);
    const iterationTexts: string[] = [];
    const agent = new Agent({
      id: 'a',
      name: 'a',
      instructions: 'test',
      model,
      tools: { echo: echoTool() },
      maxProcessorRetries: 2,
      outputProcessors: [
        {
          id: 'reject-then-trim',
          processOutputStep: ({ text: stepText, abort, retryCount, messages }: any) => {
            if (retryCount === 0 && (stepText ?? '').includes('REJECTED')) abort('again', { retry: true });
            if (!(stepText ?? '').includes('FINAL')) return messages;
            return structuredClone(messages).map((message: any) =>
              message.role === 'assistant'
                ? {
                    ...message,
                    content: {
                      ...message.content,
                      parts: message.content.parts.filter((part: any) => part.type !== 'tool-invocation'),
                    },
                  }
                : message,
            );
          },
        },
      ] as any,
    });
    const stream = await agent.stream('go', {
      onIterationComplete: ({ text: iterationText }) => {
        iterationTexts.push(iterationText);
        return undefined;
      },
    });
    await stream.consumeStream();

    expect(calls()).toBe(4);
    expect(iterationTexts.at(-1)).toBe('FINAL ANSWER');
  });

  it('counts a tool result stored in a fresh message toward the step that called the tool', async () => {
    // Dropping the invocation part makes the result miss `updateToolInvocation`, so the
    // mapping step stores it in a fresh assistant message after the step finished.
    const { model, calls } = scriptedModel([
      [toolCall('c1'), finish('tool-calls')],
      [...text('done'), finish('stop')],
    ]);
    const toolResultsSeen: number[] = [];
    const agent = new Agent({
      id: 'a',
      name: 'a',
      instructions: 'test',
      model,
      tools: { echo: echoTool() },
      inputProcessors: [
        {
          id: 'spy',
          processInputStep: ({ steps }: any) => {
            if (steps.length > 0) toolResultsSeen.push(steps[0].toolResults.length);
            return {};
          },
        },
      ] as any,
      outputProcessors: [
        {
          id: 'drop-invocations',
          processOutputStep: ({ toolCalls, messages }: any) => {
            if (!toolCalls?.length) return messages;
            return structuredClone(messages).map((message: any) =>
              message.role === 'assistant'
                ? {
                    ...message,
                    content: {
                      ...message.content,
                      parts: message.content.parts.filter((part: any) => part.type !== 'tool-invocation'),
                      toolInvocations: [],
                    },
                  }
                : message,
            );
          },
        },
      ] as any,
    });
    const stream = await agent.stream('go');
    await stream.consumeStream();

    expect(calls()).toBe(2);
    expect(toolResultsSeen).toEqual([1]);
  });

  it('reads the repaired answer after a processor sealed the response message', async () => {
    // Observational memory seals the response message between steps; the retry after it
    // must still find its own answer.
    const { model, calls } = scriptedModel([
      [toolCall('c1'), finish('tool-calls')],
      [...text('REJECTED'), finish('stop')],
      [...text('ACCEPTED'), finish('stop')],
    ]);
    const iterationTexts: string[] = [];
    const agent = new Agent({
      id: 'a',
      name: 'a',
      instructions: 'test',
      model,
      tools: { echo: echoTool() },
      maxProcessorRetries: 2,
      inputProcessors: [
        {
          id: 'seal',
          processInputStep: ({ stepNumber, rotateResponseMessageId }: any) => {
            if (stepNumber === 1) rotateResponseMessageId?.();
            return {};
          },
        },
      ] as any,
      outputProcessors: [rejectOnce] as any,
    });
    const stream = await agent.stream('go', {
      onIterationComplete: ({ text: iterationText }) => {
        iterationTexts.push(iterationText);
        return undefined;
      },
    });
    await stream.consumeStream();

    expect(calls()).toBe(3);
    expect(iterationTexts.at(-1)).toBe('ACCEPTED');
  });

  it('reads text that follows a provider-executed tool result in the same response', async () => {
    // The response holds an intra-response step-start between the tool result and the text.
    const { model, calls } = scriptedModel([
      [
        { type: 'tool-call', toolCallId: 'p1', toolName: 'web_search', input: '{}', providerExecuted: true },
        {
          type: 'tool-result',
          toolCallId: 'p1',
          toolName: 'web_search',
          result: { found: 'data' },
          providerExecuted: true,
        },
        ...text('ANSWER'),
        finish('stop'),
      ],
    ]);
    const iterationTexts: string[] = [];
    const agent = new Agent({
      id: 'a',
      name: 'a',
      instructions: 'test',
      model,
      outputProcessors: [{ id: 'noop', processOutputStep: ({ messageList }: any) => messageList }] as any,
    });
    const stream = await agent.stream('go', {
      onIterationComplete: ({ text: iterationText }) => {
        iterationTexts.push(iterationText);
        return undefined;
      },
    });
    await stream.consumeStream();

    expect(calls()).toBe(1);
    expect(iterationTexts).toEqual(['ANSWER']);
  });

  it('reads the answer after a processor pruned the earlier response', async () => {
    const { model, calls } = scriptedModel([
      [toolCall('c1'), finish('tool-calls')],
      [...text('DONE'), finish('stop')],
    ]);
    const iterationTexts: string[] = [];
    const agent = new Agent({
      id: 'a',
      name: 'a',
      instructions: 'test',
      model,
      tools: { echo: echoTool() },
      outputProcessors: [{ id: 'noop', processOutputStep: ({ messageList }: any) => messageList }] as any,
      inputProcessors: [
        {
          id: 'prune',
          processInputStep: ({ stepNumber, messageList, rotateResponseMessageId }: any) => {
            if (stepNumber === 1) {
              rotateResponseMessageId();
              messageList.removeByIds(messageList.get.response.db().map((message: any) => message.id));
            }
            return messageList;
          },
        },
      ] as any,
    });
    const stream = await agent.stream('go', {
      onIterationComplete: ({ text: iterationText }) => {
        iterationTexts.push(iterationText);
        return undefined;
      },
    });
    await stream.consumeStream();

    expect(calls()).toBe(2);
    expect(iterationTexts.at(-1)).toBe('DONE');
  });

  it('does not restore narration a processor removed when the tool result arrived', async () => {
    const { model } = scriptedModel([
      [...text('REMOVED NARRATION'), toolCall('c1'), finish('tool-calls')],
      [...text('DONE'), finish('stop')],
    ]);
    const iterationTexts: string[] = [];
    const agent = new Agent({
      id: 'a',
      name: 'a',
      instructions: 'test',
      model,
      tools: { echo: echoTool() },
      outputProcessors: [
        {
          id: 'redact',
          processOutputStream: ({ part, messageList }: any) => {
            if (part.type === 'tool-result') {
              messageList.removeByIds(messageList.get.response.db().map((message: any) => message.id));
            }
            return part;
          },
        },
      ] as any,
    });
    const stream = await agent.stream('go', {
      onIterationComplete: ({ text: iterationText }) => {
        iterationTexts.push(iterationText);
        return undefined;
      },
    });
    await stream.consumeStream();

    expect(iterationTexts).not.toContain('REMOVED NARRATION');
    expect(iterationTexts.at(-1)).toBe('DONE');
  });

  it('returns the repaired structured output after a retry', async () => {
    const { model, calls } = scriptedModel([
      [...text('{"answer":"REJECTED"}'), finish('stop')],
      [...text('{"answer":"ACCEPTED"}'), finish('stop')],
    ]);
    const agent = new Agent({
      id: 'a',
      name: 'a',
      instructions: 'test',
      model,
      outputProcessors: [rejectOnce] as any,
      maxProcessorRetries: 2,
    });
    const stream = await agent.stream('go', { structuredOutput: { schema: z.object({ answer: z.string() }) } });
    await stream.consumeStream();

    expect(calls()).toBe(2);
    expect(await stream.object).toEqual({ answer: 'ACCEPTED' });
  });

  it('stops without another model call when the run is aborted while a retry is granted', async () => {
    const { model, calls } = scriptedModel([
      [...text('REJECTED'), finish('stop')],
      [...text('ACCEPTED'), finish('stop')],
    ]);
    const abortController = new AbortController();
    let abortedSteps: number | undefined;
    const agent = new Agent({
      id: 'a',
      name: 'a',
      instructions: 'test',
      model,
      outputProcessors: [
        {
          id: 'reject-and-abort',
          processOutputStep: ({ abort }: any) => {
            abortController.abort();
            abort('Answer again.', { retry: true });
          },
        },
      ] as any,
      maxProcessorRetries: 2,
    });
    const stream = await agent.stream('go', {
      abortSignal: abortController.signal,
      onAbort: ({ steps }) => {
        abortedSteps = steps.length;
      },
    });
    await stream.consumeStream();

    expect(calls()).toBe(1);
    expect(abortedSteps).toBe(1);
  });
});
