import type { LanguageModelV2Prompt } from '@ai-sdk/provider-v5';
import { describe, expect, it } from 'vitest';

import { MessageList } from '../../agent/message-list';
import type { ToolResultSupersedeCall, ToolResultSupersedeOptions } from './tool-result-supersede';
import { ToolResultSupersedeProcessor } from './tool-result-supersede';

type Call = {
  id: string;
  tool: string;
  input: unknown;
  output?: any;
  result?: unknown;
  approval?: unknown;
  /** Recorded providerMetadata.mastra.resultStatus */
  resultStatus?: 'error';
  /** Prompt output's providerOptions.mastra.resultStatus */
  promptStatus?: 'error';
};

/**
 * Build a provider prompt and the matching recorded message list. Each inner
 * array is one model step (consecutive assistant messages merge into one
 * recorded message, separated by step-start parts). `runSteps`
 * marks how many trailing steps belong to the current run.
 */
function build(stepsOfCalls: Call[][], options: { runSteps?: number; separateToolMessages?: boolean } = {}) {
  const messageList = new MessageList();
  const prompt: LanguageModelV2Prompt = [];
  stepsOfCalls.forEach((calls, stepIndex) => {
    // A user message between steps keeps MessageList from merging the recorded
    // assistant messages (a merge drops the repeated step-start boundary that a
    // live run records).
    if (stepIndex > 0) messageList.add({ role: 'user', content: `step ${stepIndex}` } as any, 'input');
    messageList.add(
      {
        id: `assistant-${stepIndex}`,
        role: 'assistant',
        createdAt: new Date(stepIndex),
        content: {
          format: 2,
          // Recorded runs separate model steps with step-start parts.
          parts: [
            { type: 'step-start' },
            ...calls.map(call => ({
              type: 'tool-invocation',
              toolInvocation: {
                state: 'result',
                toolCallId: call.id,
                toolName: call.tool,
                args: call.input,
                result: call.result ?? call.output?.value,
                ...(call.approval ? { approval: call.approval } : {}),
              },
              ...(call.resultStatus ? { providerMetadata: { mastra: { resultStatus: call.resultStatus } } } : {}),
            })),
          ],
        },
      } as any,
      'response',
    );
    prompt.push({
      role: 'assistant',
      content: calls.map(call => ({ type: 'tool-call', toolCallId: call.id, toolName: call.tool, input: call.input })),
    });
    const results = calls.map(call => ({
      type: 'tool-result' as const,
      toolCallId: call.id,
      toolName: call.tool,
      output: call.output ?? { type: 'text', value: `body-${call.id}` },
      ...(call.promptStatus ? { providerOptions: { mastra: { resultStatus: call.promptStatus } } } : {}),
    }));
    if (options.separateToolMessages) {
      for (const result of results) prompt.push({ role: 'tool', content: [result] });
    } else {
      prompt.push({ role: 'tool', content: results });
    }
  });
  const runSteps = options.runSteps ?? 0;
  const steps = stepsOfCalls
    .slice(stepsOfCalls.length - runSteps)
    .map(calls => ({ toolCalls: calls.map(call => ({ toolCallId: call.id })) }));
  return { prompt, messageList, steps };
}

function run(options: ToolResultSupersedeOptions, built: ReturnType<typeof build>) {
  const result = new ToolResultSupersedeProcessor(options).processLLMRequest({
    prompt: built.prompt,
    messageList: built.messageList,
    steps: built.steps as any,
    model: 'test-model' as any,
    stepNumber: built.steps.length,
    state: {},
    abort: (() => {
      throw new Error('aborted');
    }) as any,
  } as any) as { prompt?: LanguageModelV2Prompt } | undefined;
  return result?.prompt;
}

/** Tool-call ids whose result was replaced by the pointer, in prompt order. */
function superseded(prompt: LanguageModelV2Prompt | undefined): string[] {
  const ids: string[] = [];
  for (const message of prompt ?? []) {
    if (message.role !== 'tool') continue;
    for (const part of message.content) {
      if (String((part.output as { value?: unknown }).value).startsWith('[superseded]')) ids.push(part.toolCallId);
    }
  }
  return ids;
}

const byPath = { tools: { read_file: { key: ({ input }: ToolResultSupersedeCall) => (input as any)?.path } } };
const read = (id: string, path = 'a.tex', extra: Partial<Call> = {}): Call => ({
  id,
  tool: 'read_file',
  input: { path },
  ...extra,
});

describe('ToolResultSupersedeProcessor', () => {
  it('replaces an earlier-step result for the same key with a fixed pointer and keeps the newest verbatim', () => {
    const built = build([[read('c1')], [read('c2')]]);
    const rewritten = run(byPath, built);

    const outputs = rewritten!.flatMap(message =>
      message.role === 'tool' ? message.content.map(part => [part.toolCallId, part.output]) : [],
    );
    expect(outputs).toEqual([
      [
        'c1',
        {
          type: 'text',
          value:
            '[superseded] A later read_file result for the same resource replaced this one; use the latest result.',
        },
      ],
      ['c2', { type: 'text', value: 'body-c2' }],
    ]);
    // The call stays, and the input prompt is not mutated (transient rewrite).
    expect(rewritten![0]).toBe(built.prompt[0]);
    expect(superseded(built.prompt)).toEqual([]);
  });

  it('keeps different keys and tools that did not opt in', () => {
    const built = build([
      [read('c1', 'a.tex'), { id: 's1', tool: 'search', input: { path: 'a.tex' } }],
      [read('c2', 'b.tex'), { id: 's2', tool: 'search', input: { path: 'a.tex' } }],
    ]);
    expect(run(byPath, built)).toBeUndefined();
  });

  it('never replaces results of the same step, even when they arrive as separate tool messages', () => {
    const built = build([[read('p1'), read('p2')]], { separateToolMessages: true });
    expect(run(byPath, built)).toBeUndefined();
  });

  it('orders by model step: history results are earlier than this run, and run steps follow their order', () => {
    const built = build([[read('h1')], [read('h2')], [read('r1')], [read('r2')]], { runSteps: 2 });
    expect(superseded(run(byPath, built))).toEqual(['h1', 'h2', 'r1']);
  });

  it('keeps the pointer byte-identical across three generations', () => {
    const two = run(byPath, build([[read('c1')], [read('c2')]]));
    const three = run(byPath, build([[read('c1')], [read('c2')], [read('c3')]]));
    expect(JSON.stringify(three!.slice(0, 2))).toBe(JSON.stringify(two!.slice(0, 2)));
    expect(superseded(three)).toEqual(['c1', 'c2']);
  });

  it('pairs reused tool-call ids by occurrence, so an older turn keeps its own input', () => {
    // A provider reused `call_0` in two turns for different files; a third read
    // of a.tex supersedes only the first turn's result.
    const rewritten = run(byPath, build([[read('call_0', 'a.tex')], [read('call_0', 'b.tex')], [read('x', 'a.tex')]]));
    const toolMessages = rewritten!.filter(message => message.role === 'tool');
    expect(String((toolMessages[0]!.content[0]!.output as any).value)).toContain('[superseded]');
    expect(toolMessages[1]!.content[0]!.output).toEqual({ type: 'text', value: 'body-call_0' });
    expect(toolMessages[2]!.content[0]!.output).toEqual({ type: 'text', value: 'body-x' });
  });

  it('keeps every result without a recorded occurrence', () => {
    const built = build([[read('c1')], [read('c2')]]);
    expect(run(byPath, { ...built, messageList: new MessageList() })).toBeUndefined();
  });

  it('never keys errors, classified failures, approvals, media or a broken key', () => {
    const options = {
      tools: {
        read_file: byPath.tools.read_file,
        read_paper: {
          key: ({ input }: ToolResultSupersedeCall) => {
            if ((input as any).id === 'boom') throw new Error('bad key');
            return (input as any).id;
          },
        },
      },
    };
    const image = { type: 'content', value: [{ type: 'media', data: 'xx', mediaType: 'image/png' }] };
    const cases: Array<[string, Call, Call]> = [
      ['error output', read('a1'), read('a2', 'a.tex', { output: { type: 'error-text', value: 'nope' } })],
      ['classified failure', read('b1'), read('b2', 'a.tex', { promptStatus: 'error', resultStatus: 'error' })],
      ['approval', read('c1', 'a.tex', { approval: { id: 'c1', approved: true } }), read('c2')],
      ['media', read('d1', 'a.tex', { output: image }), read('d2', 'a.tex', { output: image })],
      [
        'broken key',
        { id: 'e1', tool: 'read_paper', input: { id: 'boom' } },
        { id: 'e2', tool: 'read_paper', input: { id: 'boom' } },
      ],
    ];
    for (const [label, first, second] of cases) {
      expect([label, superseded(run(options, build([[first], [second]])))]).toEqual([label, []]);
    }
    expect(
      superseded(
        run(
          { tools: { read_file: { ...byPath.tools.read_file, includeMedia: true } } },
          build([[read('d1', 'a.tex', { output: image })], [read('d2', 'a.tex', { output: image })]]),
        ),
      ),
    ).toEqual(['d1']);
  });

  it('shares a key space across tools in one group and reads the recorded result', () => {
    const seen: unknown[] = [];
    const rawKey = (call: ToolResultSupersedeCall) => {
      seen.push(call.result);
      const raw = call.result as { path?: string; version?: string } | undefined;
      return raw?.path && raw.version ? `${raw.path}@${raw.version}` : undefined;
    };
    const built = build([
      [{ id: 'c1', tool: 'read_full', input: { path: 'a.tex' }, result: { path: 'a.tex', version: '1' } }],
      [{ id: 'c2', tool: 'read_full_again', input: { path: 'a.tex' }, result: { path: 'a.tex', version: '1' } }],
    ]);
    const rewritten = run(
      { tools: { read_full: { key: rawKey, group: 'file' }, read_full_again: { key: rawKey, group: 'file' } } },
      built,
    );
    expect(seen).toEqual([
      { path: 'a.tex', version: '1' },
      { path: 'a.tex', version: '1' },
    ]);
    expect(superseded(rewritten)).toEqual(['c1']);
  });

  it('rejects a configuration without a key function', () => {
    expect(() => new ToolResultSupersedeProcessor({ tools: { read_file: {} as any } })).toThrow(
      /key must be a function/,
    );
    expect(() => new ToolResultSupersedeProcessor(undefined as any)).toThrow(/options.tools/);
  });
});
