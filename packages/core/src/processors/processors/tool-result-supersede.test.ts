import type { LanguageModelV2Prompt } from '@ai-sdk/provider-v5';
import { describe, expect, it } from 'vitest';

import { MessageList } from '../../agent/message-list';
import type { ToolResultSupersedeCall } from './tool-result-supersede';
import { ToolResultSupersedeProcessor } from './tool-result-supersede';

function run(processor: ToolResultSupersedeProcessor, prompt: LanguageModelV2Prompt, messageList?: MessageList) {
  const result = processor.processLLMRequest({
    prompt,
    messageList,
    model: 'test-model' as any,
    stepNumber: 0,
    steps: [],
    state: {},
    abort: (() => {
      throw new Error('aborted');
    }) as any,
  } as any) as { prompt?: LanguageModelV2Prompt } | undefined;
  return result?.prompt;
}

function call(toolCallId: string, toolName: string, input: unknown): LanguageModelV2Prompt[number] {
  return { role: 'assistant', content: [{ type: 'tool-call', toolCallId, toolName, input }] };
}

function result(toolCallId: string, toolName: string, output: any): LanguageModelV2Prompt[number] {
  return { role: 'tool', content: [{ type: 'tool-result', toolCallId, toolName, output }] };
}

function outputs(prompt: LanguageModelV2Prompt | undefined): Record<string, unknown> {
  const byId: Record<string, unknown> = {};
  for (const message of prompt ?? []) {
    if (message.role !== 'tool') continue;
    for (const part of message.content) byId[part.toolCallId] = part.output;
  }
  return byId;
}

const readKey = ({ input, output }: ToolResultSupersedeCall) => {
  const path = (input as { path?: string }).path;
  const hash = (output as { value?: { hash?: string } }).value?.hash;
  return path && hash ? `${path}@${hash}` : undefined;
};

describe('ToolResultSupersedeProcessor', () => {
  it('replaces an earlier result for the same key with a pointer and keeps the newest verbatim', () => {
    const processor = new ToolResultSupersedeProcessor({ tools: { read_file: { key: readKey } } });
    const first = { type: 'json', value: { path: 'main.tex', hash: 'h1', text: 'A'.repeat(500) } };
    const second = { type: 'json', value: { path: 'main.tex', hash: 'h1', text: 'A'.repeat(500) } };
    const prompt: LanguageModelV2Prompt = [
      { role: 'user', content: [{ type: 'text', text: 'Edit main.tex' }] },
      call('c1', 'read_file', { path: 'main.tex' }),
      result('c1', 'read_file', first),
      call('c2', 'read_file', { path: 'main.tex' }),
      result('c2', 'read_file', second),
    ];

    const rewritten = run(processor, prompt);

    expect(outputs(rewritten)).toEqual({
      c1: {
        type: 'text',
        value: '[superseded] A later read_file result for the same resource replaced this one; use the latest result.',
      },
      c2: second,
    });
    // The call and its input stay, so pairing and provenance survive.
    expect(rewritten?.[1]).toBe(prompt[1]);
    // The rewrite is transient: the input prompt is not mutated.
    expect(outputs(prompt).c1).toBe(first);
  });

  it('keeps results whose keys differ, and tools that did not opt in', () => {
    const processor = new ToolResultSupersedeProcessor({ tools: { read_file: { key: readKey } } });
    const prompt: LanguageModelV2Prompt = [
      call('c1', 'read_file', { path: 'main.tex' }),
      result('c1', 'read_file', { type: 'json', value: { hash: 'h1' } }),
      call('c2', 'read_file', { path: 'main.tex' }),
      result('c2', 'read_file', { type: 'json', value: { hash: 'h2' } }),
      call('c3', 'search', { path: 'main.tex' }),
      result('c3', 'search', { type: 'json', value: { hash: 'h2' } }),
      call('c4', 'search', { path: 'main.tex' }),
      result('c4', 'search', { type: 'json', value: { hash: 'h2' } }),
    ];
    expect(run(processor, prompt)).toBeUndefined();
  });

  it('never lets an error supersede evidence, and fails closed on a broken key', () => {
    const processor = new ToolResultSupersedeProcessor({
      tools: {
        read_file: { key: readKey },
        read_paper: {
          key: ({ input }) => {
            if ((input as { id: string }).id === 'boom') throw new Error('bad key');
            return (input as { id: string }).id;
          },
        },
      },
    });
    const prompt: LanguageModelV2Prompt = [
      call('c1', 'read_file', { path: 'main.tex' }),
      result('c1', 'read_file', { type: 'json', value: { hash: 'h1' } }),
      call('c2', 'read_file', { path: 'main.tex' }),
      result('c2', 'read_file', { type: 'error-json', value: { hash: 'h1' } }),
      call('c3', 'read_paper', { id: 'boom' }),
      result('c3', 'read_paper', { type: 'text', value: 'paper' }),
      call('c4', 'read_paper', { id: 'boom' }),
      result('c4', 'read_paper', { type: 'text', value: 'paper' }),
    ];
    expect(run(processor, prompt)).toBeUndefined();
  });

  it('shares a key space across tools in one group and reads the raw result from the message list', () => {
    const seen: unknown[] = [];
    const rawKey = (callInfo: ToolResultSupersedeCall) => {
      seen.push(callInfo.result);
      const raw = callInfo.result as { path?: string; contentHash?: string } | undefined;
      return raw?.path && raw.contentHash ? `${raw.path}@${raw.contentHash}` : undefined;
    };
    const processor = new ToolResultSupersedeProcessor({
      tools: {
        latex_read_file: { key: rawKey, group: 'latex-file' },
        latex_read_file_slice: { key: rawKey, group: 'latex-file' },
      },
    });
    const messageList = new MessageList();
    messageList.add(
      {
        id: 'assistant-1',
        role: 'assistant',
        content: {
          format: 2,
          parts: [
            {
              type: 'tool-invocation',
              toolInvocation: {
                state: 'result',
                toolCallId: 'c1',
                toolName: 'latex_read_file',
                args: { path: 'a.tex' },
                result: { path: 'a.tex', contentHash: 'x' },
              },
            },
            {
              type: 'tool-invocation',
              toolInvocation: {
                state: 'result',
                toolCallId: 'c2',
                toolName: 'latex_read_file_slice',
                args: { path: 'a.tex' },
                result: { path: 'a.tex', contentHash: 'x' },
              },
            },
          ],
        },
        createdAt: new Date(0),
      } as any,
      'response',
    );
    const prompt: LanguageModelV2Prompt = [
      call('c1', 'latex_read_file', { path: 'a.tex' }),
      result('c1', 'latex_read_file', { type: 'text', value: 'numbered text' }),
      call('c2', 'latex_read_file_slice', { path: 'a.tex' }),
      result('c2', 'latex_read_file_slice', { type: 'text', value: 'numbered slice' }),
    ];

    const rewritten = run(processor, prompt, messageList);

    expect(seen).toEqual([
      { path: 'a.tex', contentHash: 'x' },
      { path: 'a.tex', contentHash: 'x' },
    ]);
    expect((outputs(rewritten).c1 as { value: string }).value).toContain('[superseded]');
    expect(outputs(rewritten).c2).toEqual({ type: 'text', value: 'numbered slice' });
  });

  it('is stable across steps: a rewritten prompt plus a new unrelated result keeps the same bytes', () => {
    const processor = new ToolResultSupersedeProcessor({
      tools: { read_file: { key: ({ input }) => (input as any).path } },
    });
    const base: LanguageModelV2Prompt = [
      call('c1', 'read_file', { path: 'a.tex' }),
      result('c1', 'read_file', { type: 'text', value: 'v1' }),
      call('c2', 'read_file', { path: 'a.tex' }),
      result('c2', 'read_file', { type: 'text', value: 'v1' }),
    ];
    const stepOne = run(processor, base)!;
    const stepTwo = run(processor, [
      ...base,
      call('c3', 'read_file', { path: 'b.tex' }),
      result('c3', 'read_file', { type: 'text', value: 'other' }),
    ])!;
    expect(JSON.stringify(stepTwo.slice(0, stepOne.length))).toBe(JSON.stringify(stepOne));
  });

  it('keeps the pointer byte-identical across three generations and never replaces same-step duplicates', () => {
    const processor = new ToolResultSupersedeProcessor({
      tools: { read_file: { key: ({ input }) => (input as any).path } },
    });
    const twoReads: LanguageModelV2Prompt = [
      call('c1', 'read_file', { path: 'a.tex' }),
      result('c1', 'read_file', { type: 'text', value: 'v1' }),
      call('c2', 'read_file', { path: 'a.tex' }),
      result('c2', 'read_file', { type: 'text', value: 'v2' }),
    ];
    const threeReads: LanguageModelV2Prompt = [
      ...twoReads,
      call('c3', 'read_file', { path: 'a.tex' }),
      result('c3', 'read_file', { type: 'text', value: 'v3' }),
    ];
    const afterTwo = outputs(run(processor, twoReads));
    const afterThree = outputs(run(processor, threeReads));
    expect(afterThree.c1).toEqual(afterTwo.c1);
    expect(afterThree.c3).toEqual({ type: 'text', value: 'v3' });

    // Two parallel reads in one step: both stay.
    const parallel: LanguageModelV2Prompt = [
      {
        role: 'assistant',
        content: [
          { type: 'tool-call', toolCallId: 'p1', toolName: 'read_file', input: { path: 'a.tex' } },
          { type: 'tool-call', toolCallId: 'p2', toolName: 'read_file', input: { path: 'a.tex' } },
        ],
      },
      {
        role: 'tool',
        content: [
          { type: 'tool-result', toolCallId: 'p1', toolName: 'read_file', output: { type: 'text', value: 'x' } },
          { type: 'tool-result', toolCallId: 'p2', toolName: 'read_file', output: { type: 'text', value: 'x' } },
        ],
      },
    ];
    expect(run(processor, parallel)).toBeUndefined();
  });

  it('preserves classified failures and results that passed through an approval', () => {
    const processor = new ToolResultSupersedeProcessor({
      tools: { read_file: { key: ({ input }) => (input as any).path } },
    });
    const messageList = new MessageList();
    messageList.add(
      {
        id: 'assistant-approved',
        role: 'assistant',
        content: {
          format: 2,
          parts: [
            {
              type: 'tool-invocation',
              toolInvocation: {
                state: 'result',
                toolCallId: 'c1',
                toolName: 'read_file',
                args: { path: 'a.tex' },
                result: 'v1',
                approval: { id: 'c1', approved: true },
              },
            },
          ],
        },
        createdAt: new Date(0),
      } as any,
      'response',
    );
    const prompt: LanguageModelV2Prompt = [
      call('c1', 'read_file', { path: 'a.tex' }),
      result('c1', 'read_file', { type: 'text', value: 'v1' }),
      call('c2', 'read_file', { path: 'a.tex' }),
      {
        role: 'tool',
        content: [
          {
            type: 'tool-result',
            toolCallId: 'c2',
            toolName: 'read_file',
            output: { type: 'text', value: 'not found' },
            providerOptions: { mastra: { resultStatus: 'error' } },
          },
        ],
      },
      call('c3', 'read_file', { path: 'a.tex' }),
      result('c3', 'read_file', { type: 'text', value: 'v3' }),
    ];
    expect(run(processor, prompt, messageList)).toBeUndefined();
  });

  it('rejects a configuration without a key function', () => {
    expect(() => new ToolResultSupersedeProcessor({ tools: { read_file: {} as any } })).toThrow(
      /key must be a function/,
    );
    expect(() => new ToolResultSupersedeProcessor(undefined as any)).toThrow(/options.tools/);
  });
});
