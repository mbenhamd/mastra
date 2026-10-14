/**
 * PF-5620 — incremental `summary` decoding of a streaming
 * `report_subagent_outcome` input. Expectations come from `JSON.parse` of the
 * complete input, never from the decoder's own shape.
 */
import { describe, expect, it } from 'vitest';

import { SubagentReportSummaryDecoder } from './subagent-answer-stream';

const NO_LIMIT = Number.MAX_SAFE_INTEGER;

function decode(chunks: readonly string[], maxBytes = NO_LIMIT): string[] {
  const decoder = new SubagentReportSummaryDecoder(maxBytes);
  return chunks.map(chunk => decoder.push(chunk)).filter(delta => delta.length > 0);
}

function isHighSurrogate(unit: number): boolean {
  return unit >= 0xd800 && unit <= 0xdbff;
}

// Escapes written by hand so `\uXXXX`, surrogate-pair escapes and raw astral
// characters all occur in the provider text.
const ESCAPED_REPORT =
  '{"kind":"subagent-outcome-report","outcome":"completed",' +
  '"summary":"Line 1\\nShe said \\"hi\\" \\\\ path C:\\\\x \\u00e9t\\u00e9 \\ud83d\\ude00 raw 😀 tab\\tend\\/",' +
  '"evidence":[{"kind":"analysis","description":"checked"}]}';

describe('SubagentReportSummaryDecoder', () => {
  it('decodes the summary exactly at every single chunk boundary, including inside escapes', () => {
    const expected = (JSON.parse(ESCAPED_REPORT) as { summary: string }).summary;
    for (let split = 0; split <= ESCAPED_REPORT.length; split += 1) {
      const deltas = decode([ESCAPED_REPORT.slice(0, split), ESCAPED_REPORT.slice(split)]);
      expect(deltas.join('')).toBe(expected);
    }
  });

  it('streams one character at a time without ever ending a delta in half a surrogate pair', () => {
    const expected = (JSON.parse(ESCAPED_REPORT) as { summary: string }).summary;
    const deltas = decode([...ESCAPED_REPORT].flatMap(ch => ch.split('')));
    expect(deltas.join('')).toBe(expected);
    expect(deltas.length).toBeGreaterThan(10);
    for (const delta of deltas) {
      expect(isHighSurrogate(delta.charCodeAt(delta.length - 1))).toBe(false);
    }
  });

  it('streams the summary in order while it arrives, not only at the end', () => {
    const deltas = decode(['{"summary":"Hel', 'lo, wor', 'ld","outcome":"completed"}']);
    expect(deltas).toEqual(['Hel', 'lo, wor', 'ld']);
  });

  it('streams only the top-level summary when it is not the first key and nested values mention it', () => {
    const report = JSON.stringify({
      kind: 'subagent-outcome-report',
      outcome: 'completed',
      evidence: [{ kind: 'analysis', description: 'the "summary": below', summary: 'nested key' }],
      note: { summary: 'deeper', list: ['summary', { summary: 'deepest' }] },
      label: 'summary',
      summary: 'The real answer.',
    });
    for (let size = 1; size <= 7; size += 1) {
      const chunks: string[] = [];
      for (let i = 0; i < report.length; i += size) chunks.push(report.slice(i, i + size));
      expect(decode(chunks).join('')).toBe('The real answer.');
    }
  });

  it('emits nothing when the report has no summary, or the input is not an object', () => {
    expect(decode(['{"kind":"subagent-outcome-report","outcome":"failed","issue":{"summary":"x"}}'])).toEqual([]);
    expect(decode(['["summary","x"]'])).toEqual([]);
    expect(decode(['{"summary":42}'])).toEqual([]);
  });

  it('stops for good once the summary exceeds the byte limit of a direct answer', () => {
    // 'é' is 2 UTF-8 bytes: 3 fit within 6 bytes, the 4th does not.
    const deltas = decode(['{"summary":"éé', 'é', 'é', 'éé"}'], 6);
    expect(deltas).toEqual(['éé', 'é']);
  });
});
