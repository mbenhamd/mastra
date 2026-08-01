import { safeStringify } from '@mastra/core/utils';
import { describe, expect, it } from 'vitest';
import type { SerializedStreamChunk } from './serialize';
import { serializeStreamChunk } from './serialize';

function expectJson(result: ReturnType<typeof serializeStreamChunk>): string {
  expect(result.ok).toBe(true);
  return (result as { ok: true; json: string }).json;
}

describe('serializeStreamChunk', () => {
  it('serializes plain JSON values', () => {
    const result = serializeStreamChunk({ type: 'workflow-finish', payload: { workflowStatus: 'success' } });
    expect(expectJson(result)).toBe(
      JSON.stringify({ type: 'workflow-finish', payload: { workflowStatus: 'success' } }),
    );
  });

  it('serializes BigInt values as strings', () => {
    const chunk = { type: 'workflow-step-result', payload: { output: { count: 42n } } };
    const json = expectJson(serializeStreamChunk(chunk));
    expect(JSON.parse(json)).toEqual({ type: 'workflow-step-result', payload: { output: { count: '42' } } });
  });

  it('serializes circular references as "[Circular]"', () => {
    const payload: Record<string, unknown> = { id: 'step' };
    payload.self = payload;
    const json = expectJson(serializeStreamChunk({ type: 'workflow-step-result', payload }));
    expect(JSON.parse(json)).toEqual({ type: 'workflow-step-result', payload: { id: 'step', self: '[Circular]' } });
  });

  it('returns the error when the chunk cannot be serialized at all', () => {
    const chunk = {
      type: 'workflow-step-result',
      toJSON() {
        throw new Error('boom from toJSON');
      },
    };
    const result = serializeStreamChunk(chunk);
    expect(result.ok).toBe(false);
    expect((result as { ok: false; error: Error }).error.message).toBe('boom from toJSON');
  });
});

describe('serializeStreamChunk fast-path equivalence with the replacer-based safe path', () => {
  /**
   * Verbatim copy of the previous `serializeStreamChunk` implementation
   * (always `safeStringify`, replacer mode). The fast-path implementation must
   * produce byte-identical results — same `ok` flag, same `json` string, same
   * error message — for every input class below.
   */
  function referenceSerialize(chunk: unknown): SerializedStreamChunk {
    try {
      return { ok: true, json: safeStringify(chunk) };
    } catch (error) {
      return { ok: false, error: error instanceof Error ? error : new Error(String(error)) };
    }
  }

  const circular: Record<string, unknown> = { type: 'workflow-step-result', id: 'c1' };
  circular.self = circular;

  const circularThroughArray: { type: string; items: unknown[] } = { type: 'workflow-step-result', items: [] };
  circularThroughArray.items.push(circularThroughArray);

  const sharedRef = { reused: true };

  const DEEP_LEVELS = 2000;
  const veryDeep: Record<string, unknown> = { depth: 0 };
  let cursor = veryDeep;
  for (let i = 1; i <= DEEP_LEVELS; i++) {
    const child: Record<string, unknown> = { depth: i };
    cursor.child = child;
    cursor = child;
  }

  const throwingGetter: Record<string, unknown> = { type: 'workflow-step-result' };
  Object.defineProperty(throwingGetter, 'bad', {
    enumerable: true,
    get() {
      throw new Error('boom from getter');
    },
  });

  // eslint-disable-next-line no-sparse-arrays
  const sparseArray = [1, , 3];

  const cases: Array<[name: string, chunk: unknown]> = [
    ['plain flat object', { type: 'text-delta', payload: { id: 'msg_1', text: 'hello world' } }],
    [
      'nested object with arrays',
      {
        type: 'step-finish',
        payload: {
          output: { steps: [{ n: 1 }, { n: 2, nested: { deep: [true, null] } }], usage: { inputTokens: 10 } },
        },
      },
    ],
    ['unicode, separators, emoji, control chars', { text: 'héllo    “quotes” \\ " \n\t 🚀 \u0000 end' }],
    ['lone surrogate', { text: '\uD800' }],
    ['Date object (built-in toJSON)', { at: new Date('2026-08-01T12:34:56.789Z') }],
    ['undefined root', undefined],
    ['null root', null],
    ['string root', 'chunk'],
    ['number root', 42],
    ['boolean root', false],
    ['function root', () => 1],
    ['symbol root', Symbol('root')],
    ['undefined properties', { a: undefined, b: 1, c: undefined }],
    ['function and symbol properties', { fn: () => 1, sym: Symbol('s'), keep: true }],
    ['NaN, Infinity, -Infinity, -0', { nan: NaN, inf: Infinity, ninf: -Infinity, nzero: -0 }],
    ['sparse array', sparseArray],
    ['custom toJSON object', { custom: { toJSON: () => ({ replaced: true }) } }],
    ['toJSON returning undefined', { custom: { toJSON: () => undefined } }],
    ['toJSON at root', { toJSON: () => 'flattened' }],
    ['toJSON returning BigInt (fast path throws, fallback converts)', { v: { toJSON: () => 10n } }],
    ['Map and Set (serialize as empty objects)', { map: new Map([['k', 'v']]), set: new Set([1, 2]) }],
    ['shared non-circular reference (not [Circular])', { first: sharedRef, second: sharedRef }],
    [`very deep nesting (${DEEP_LEVELS} levels)`, veryDeep],
    ['circular reference', circular],
    ['circular reference through array', circularThroughArray],
    ['BigInt property', { count: 42n }],
    ['BigInt root', 42n],
    ['BigInt values nested in array', { values: [1n, 2n, 3n] }],
  ];

  it.each(cases)('%s: byte-identical to the safe path', (_name, chunk) => {
    const actual = serializeStreamChunk(chunk);
    const expected = referenceSerialize(chunk);

    expect(actual.ok).toBe(expected.ok);
    if (expected.ok) {
      expect((actual as { ok: true; json: string }).json).toBe(expected.json);
    }
  });

  it('reports the same error as the safe path for a throwing toJSON', () => {
    const chunk = {
      type: 'workflow-step-result',
      toJSON() {
        throw new Error('boom from toJSON');
      },
    };

    const actual = serializeStreamChunk(chunk);
    const expected = referenceSerialize(chunk);

    expect(actual.ok).toBe(false);
    expect(expected.ok).toBe(false);
    expect((actual as { ok: false; error: Error }).error.message).toBe(
      (expected as { ok: false; error: Error }).error.message,
    );
  });

  it('reports the same error as the safe path for a throwing getter', () => {
    const actual = serializeStreamChunk(throwingGetter);
    const expected = referenceSerialize(throwingGetter);

    expect(actual.ok).toBe(false);
    expect(expected.ok).toBe(false);
    expect((actual as { ok: false; error: Error }).error.message).toBe('boom from getter');
    expect((expected as { ok: false; error: Error }).error.message).toBe('boom from getter');
  });
});
