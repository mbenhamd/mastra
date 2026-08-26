import { describe, expect, it } from 'vitest';
import { sanitizeJsonForPg } from './index';

describe('sanitizeJsonForPg', () => {
  it('removes bare null character escapes', () => {
    expect(sanitizeJsonForPg('"prefix\\u0000suffix"')).toBe('"prefixsuffix"');
  });

  it('removes bare unpaired surrogate escapes (high and low, mixed case)', () => {
    expect(sanitizeJsonForPg('"a\\uD800b"')).toBe('"ab"');
    expect(sanitizeJsonForPg('"a\\udfffb"')).toBe('"ab"');
    expect(sanitizeJsonForPg('"a\\uDABCb"')).toBe('"ab"');
  });

  it('escapes invalid JSON escape sequences (\\v, \\k)', () => {
    expect(sanitizeJsonForPg('"Omschr\\vijving"')).toBe('"Omschr\\\\vijving"');
    expect(sanitizeJsonForPg('"Toepassel\\k"')).toBe('"Toepassel\\\\k"');
  });

  it('preserves valid JSON escapes (\\n, \\t, \\", \\\\)', () => {
    expect(sanitizeJsonForPg('"line1\\nline2"')).toBe('"line1\\nline2"');
    expect(sanitizeJsonForPg('"a\\tb"')).toBe('"a\\tb"');
    expect(sanitizeJsonForPg('"quote\\""')).toBe('"quote\\""');
    expect(sanitizeJsonForPg('"back\\\\slash"')).toBe('"back\\\\slash"');
  });

  describe('backslash parity and surrogate pairs', () => {
    it.each(['u0000', 'uD800', 'uDC00'])('sanitizes only odd backslash runs before %s', token => {
      for (const slashCount of [1, 2, 3, 5]) {
        const slashes = '\\'.repeat(slashCount);
        const input = `"prefix${slashes}${token}suffix"`;
        const expected = slashCount % 2 === 0 ? input : `"prefix${'\\'.repeat(slashCount - 1)}suffix"`;
        const sanitized = sanitizeJsonForPg(input);

        expect(sanitized).toBe(expected);
        expect(() => JSON.parse(sanitized)).not.toThrow();
      }
    });

    it.each(['uD83D\\uDE00', 'ud83d\\uDe00'])('preserves valid surrogate pair %s', pair => {
      const input = `"prefix\\${pair}suffix"`;
      expect(sanitizeJsonForPg(input)).toBe(input);
      expect(JSON.parse(sanitizeJsonForPg(input))).toBe('prefix😀suffix');
    });

    it('preserves a valid surrogate pair after an odd run with escaped-backslash pairs', () => {
      const input = `"prefix${'\\'.repeat(3)}uD83D\\uDE00suffix"`;
      expect(sanitizeJsonForPg(input)).toBe(input);
      expect(JSON.parse(sanitizeJsonForPg(input))).toBe('prefix\\😀suffix');
    });

    it('preserves JSON-encoded regex literals from regression #15920 exactly', () => {
      const input = JSON.stringify('a = "[^\\ud800-\\udfff]"');
      const sanitized = sanitizeJsonForPg(input);

      expect(sanitized).toBe(input);
      expect(JSON.parse(sanitized)).toBe('a = "[^\\ud800-\\udfff]"');
    });

    it('sanitizes a mixed object without rewriting escaped literal sequences', () => {
      const input = JSON.stringify({
        invalidEscape: 'Omschr\\vijving',
        regex: '[^\\ud800-\\udfff]',
        nullChar: 'a\u0000b',
        surrogate: 'x\uD800y',
        emoji: '😀',
      });
      const sanitized = sanitizeJsonForPg(input);

      expect(JSON.parse(sanitized)).toEqual({
        invalidEscape: 'Omschr\\vijving',
        regex: '[^\\ud800-\\udfff]',
        nullChar: 'ab',
        surrogate: 'xy',
        emoji: '😀',
      });
    });
  });
});
