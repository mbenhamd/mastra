import { describe, expect, it } from 'vitest';
import { sanitizeJsonForPg, toPgJson } from '../../db/sanitize-json';

describe('PostgreSQL JSON serialization', () => {
  it('removes NUL characters from values and keys', () => {
    expect(JSON.parse(toPgJson({ 'a\0b': 'before\0after' }))).toEqual({ ab: 'beforeafter' });
  });

  it('replaces unpaired surrogates without changing valid emoji', () => {
    expect(JSON.parse(toPgJson({ text: 'a\uD800b\uDFFF', emoji: '😀' }))).toEqual({ text: 'a�b�', emoji: '😀' });
  });

  it('preserves literal Unicode escape text and real backslashes preceding invalid characters', () => {
    const value = {
      literalNull: String.raw`literal\u0000`,
      literalSurrogate: String.raw`literal\uD800`,
      path: 'C:\\path\\\uD800-end',
      nullPath: 'C:\\path\\\0-end',
      regex: String.raw`[^\ud800-\udfff]`,
    };
    expect(JSON.parse(toPgJson(value))).toEqual({
      ...value,
      path: 'C:\\path\\�-end',
      nullPath: 'C:\\path\\-end',
    });
  });

  it('normalizes nested arrays and keys without changing JSON.stringify handling of Dates', () => {
    const value = { nested: [{ 'x\uD800': 'y\0z' }], date: new Date('2020-01-01T00:00:00.000Z') };
    expect(JSON.parse(toPgJson(value))).toEqual({ nested: [{ 'x�': 'yz' }], date: value.date.toISOString() });
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

  it('preserves JSON.stringify behavior for shared references and circular objects', () => {
    const shared = { 'a\0b': 'value' };
    expect(toPgJson({ first: shared, second: shared })).toBe('{"first":{"ab":"value"},"second":{"ab":"value"}}');

    const circular: Record<string, unknown> = { 'a\0b': 'value' };
    circular.self = circular;
    expect(() => toPgJson(circular)).toThrow(TypeError);
  });

  it('preserves native JSON serialization for boxed values, getters, and custom toJSON', () => {
    const value = {
      number: new Number(4),
      boolean: new Boolean(true),
      string: new String('hello'),
      nested: { toJSON: () => ({ text: 'normal' }) },
      get computed() {
        return this.string.toString();
      },
    };
    expect(toPgJson(value)).toBe(JSON.stringify(value));
    expect(toPgJson(JSON.rawJSON('3'))).toBe(JSON.stringify(JSON.rawJSON('3')));
  });

  it('rejects keys that collide after repair rather than silently overwriting values', () => {
    expect(() => toPgJson({ ab: 1, 'a\0b': 2 })).toThrow('JSON keys collide');
  });

  it('preserves native behavior for top-level values without JSON output', () => {
    expect(toPgJson(undefined)).toBe(JSON.stringify(undefined));
  });
});
