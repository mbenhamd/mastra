/**
 * Rehydrating stored workflows must not silently drop schema constraints.
 * Guard the MVP subset by asserting we hard-crash on keywords the converter
 * does not understand — otherwise unsupported schemas would degrade to
 * `z.any()` and let malformed data flow through execution.
 */
import { describe, it, expect } from 'vitest';
import { z } from 'zod';

import { standardSchemaToJSONSchema, toStandardSchema } from '../../schema';
import { jsonSchemaToZod, validateStorableJsonSchema } from '../stored';

describe('jsonSchemaToZod', () => {
  it('round-trips supported primitive + object shapes', () => {
    const zod = jsonSchemaToZod({
      type: 'object',
      properties: {
        name: { type: 'string' },
        age: { type: 'integer' },
        tags: { type: 'array', items: { type: 'string' } },
      },
      required: ['name'],
    });
    const parsed = (zod as z.ZodObject<any>).parse({ name: 'Tony', tags: ['a', 'b'] });
    expect(parsed).toEqual({ name: 'Tony', tags: ['a', 'b'] });
  });

  it('supports enum', () => {
    const zod = jsonSchemaToZod({ enum: ['red', 'blue'] });
    expect(zod.parse('red')).toBe('red');
    expect(() => zod.parse('green')).toThrow();
  });

  it('supports primitive const via z.literal', () => {
    expect(jsonSchemaToZod({ const: 'fixed' }).parse('fixed')).toBe('fixed');
    expect(() => jsonSchemaToZod({ const: 'fixed' }).parse('other')).toThrow();
    expect(jsonSchemaToZod({ const: 42 }).parse(42)).toBe(42);
    expect(jsonSchemaToZod({ const: false }).parse(false)).toBe(false);
    expect(jsonSchemaToZod({ const: null }).parse(null)).toBe(null);
  });

  it('rejects non-primitive const values instead of dropping the constraint', () => {
    expect(() => jsonSchemaToZod({ const: { nested: true } })).toThrow(/non-primitive "const"/);
    const messages: string[] = [];
    const zod = jsonSchemaToZod(
      { const: [1, 2] },
      { onUnsupportedSchema: 'warn', onUnsupported: m => messages.push(m) },
    );
    expect(zod.parse('anything')).toBe('anything'); // degraded to z.any()
    expect(messages[0]).toMatch(/non-primitive "const"/);
  });

  it('preserves non-string enum member types instead of coercing to string', () => {
    const numeric = jsonSchemaToZod({ enum: [1, 2, 3] });
    expect(numeric.parse(2)).toBe(2);
    expect(() => numeric.parse('2')).toThrow();

    const mixed = jsonSchemaToZod({ enum: ['a', 1, true, null] });
    expect(mixed.parse('a')).toBe('a');
    expect(mixed.parse(1)).toBe(1);
    expect(mixed.parse(true)).toBe(true);
    expect(mixed.parse(null)).toBe(null);
    expect(() => mixed.parse('1')).toThrow();
  });

  it('rejects enums with non-primitive members', () => {
    expect(() => jsonSchemaToZod({ enum: [{ bad: true }] })).toThrow(/non-primitive members/);
  });

  it('rejects tuple-form items instead of widening to z.array(z.any())', () => {
    expect(() => jsonSchemaToZod({ type: 'array', items: [{ type: 'string' }, { type: 'number' }] })).toThrow(
      /tuple-form "items"/,
    );
    const messages: string[] = [];
    const zod = jsonSchemaToZod(
      { type: 'array', items: [{ type: 'string' }] },
      { onUnsupportedSchema: 'warn', onUnsupported: m => messages.push(m) },
    );
    expect(zod.parse({ not: 'an array' })).toEqual({ not: 'an array' }); // degraded to z.any()
    expect(messages[0]).toMatch(/tuple-form "items"/);
  });

  it.each(['oneOf', 'anyOf', 'allOf', 'not', '$ref', 'patternProperties', 'discriminator'])(
    'throws on unsupported keyword %s (default mode)',
    keyword => {
      expect(() => jsonSchemaToZod({ [keyword]: [{ type: 'string' }] } as any)).toThrow(
        new RegExp(`unsupported JSON Schema keyword "${keyword.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}"`),
      );
    },
  );

  it('in warn mode, degrades unsupported keyword to z.any() and calls onUnsupported', () => {
    const messages: string[] = [];
    const zod = jsonSchemaToZod({ oneOf: [{ type: 'string' }, { type: 'number' }] } as any, {
      onUnsupportedSchema: 'warn',
      onUnsupported: m => messages.push(m),
    });
    // z.any() accepts anything.
    expect(zod.parse('hello')).toBe('hello');
    expect(zod.parse(42)).toBe(42);
    expect(zod.parse({ arbitrary: true })).toEqual({ arbitrary: true });
    expect(messages).toHaveLength(1);
    expect(messages[0]).toMatch(/oneOf/);
  });

  it('in warn mode, degrades a nested unsupported keyword under properties and keeps other fields typed', () => {
    const messages: string[] = [];
    const zod = jsonSchemaToZod(
      {
        type: 'object',
        properties: {
          name: { type: 'string' },
          payload: { anyOf: [{ type: 'string' }, { type: 'number' }] },
        },
        required: ['name'],
      } as any,
      { onUnsupportedSchema: 'warn', onUnsupported: m => messages.push(m) },
    );
    // `name` is still a required string; `payload` is z.any().
    const parsed = (zod as any).parse({ name: 'Tony', payload: { anything: true } });
    expect(parsed.name).toBe('Tony');
    // Required-string constraint is preserved.
    expect(() => (zod as any).parse({ name: 42, payload: 'ok' })).toThrow();
    expect(messages).toHaveLength(1);
    expect(messages[0]).toMatch(/anyOf/);
  });

  it('throws on unsupported type keyword', () => {
    expect(() => jsonSchemaToZod({ type: 'never' } as any)).toThrow(/unsupported JSON Schema type "never"/);
  });

  it('tolerates a bare schema with no type (annotation-only)', () => {
    const zod = jsonSchemaToZod({ description: 'freeform' } as any);
    expect(zod.parse(42)).toBe(42);
    expect(zod.parse('anything')).toBe('anything');
  });
});

describe('validateStorableJsonSchema', () => {
  it('returns ok for a schema with only supported keywords', () => {
    const result = validateStorableJsonSchema({
      type: 'object',
      properties: { name: { type: 'string' }, tags: { type: 'array', items: { type: 'string' } } },
      required: ['name'],
    });
    expect(result).toEqual({ ok: true });
  });

  it('returns ok for undefined / empty schema', () => {
    expect(validateStorableJsonSchema(undefined)).toEqual({ ok: true });
    expect(validateStorableJsonSchema({})).toEqual({ ok: true });
  });

  it('flags top-level oneOf without throwing', () => {
    const result = validateStorableJsonSchema({ oneOf: [{ type: 'string' }, { type: 'number' }] });
    expect(result).toEqual({ ok: false, unsupported: ['#: oneOf'] });
  });

  it('flags unsupported keywords nested inside properties with a JSON pointer', () => {
    const result = validateStorableJsonSchema({
      type: 'object',
      properties: {
        payload: { anyOf: [{ type: 'string' }, { type: 'number' }] },
      },
    });
    expect(result).toEqual({ ok: false, unsupported: ['/properties/payload: anyOf'] });
  });

  it('flags unsupported keywords nested inside array items', () => {
    const result = validateStorableJsonSchema({
      type: 'array',
      items: { allOf: [{ type: 'string' }] },
    });
    expect(result).toEqual({ ok: false, unsupported: ['/items: allOf'] });
  });

  it('collects multiple offenses in one walk', () => {
    const result = validateStorableJsonSchema({
      oneOf: [{ type: 'string' }],
      properties: { x: { $ref: '#/definitions/foo' } },
    });
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.unsupported).toEqual(expect.arrayContaining(['#: oneOf', '/properties/x: $ref']));
    }
  });
});

describe('primitive constraint keywords', () => {
  it('round-trips string/number/array constraints instead of silently widening', () => {
    const original = z.object({
      name: z.string().min(3).max(5).regex(/^a/),
      count: z.number().int().gt(0).lte(10).multipleOf(2),
      ratio: z.number().gte(0.5).lt(2),
      tags: z.array(z.string()).min(1).max(2),
    });
    const json = standardSchemaToJSONSchema(toStandardSchema(original)) as Record<string, any>;
    const rebuilt = jsonSchemaToZod(json);

    const good = { name: 'abcd', count: 4, ratio: 0.5, tags: ['x'] };
    expect(original.safeParse(good).success).toBe(true);
    expect(rebuilt.safeParse(good).success).toBe(true);

    const bads: Array<[string, Record<string, unknown>]> = [
      ['minLength', { ...good, name: 'ab' }],
      ['maxLength', { ...good, name: 'abcdef' }],
      ['pattern', { ...good, name: 'bcde' }],
      ['exclusiveMinimum', { ...good, count: 0 }],
      ['maximum', { ...good, count: 12 }],
      ['multipleOf', { ...good, count: 3 }],
      ['integer', { ...good, count: 4.5 }],
      ['minimum', { ...good, ratio: 0.4 }],
      ['exclusiveMaximum', { ...good, ratio: 2 }],
      ['minItems', { ...good, tags: [] }],
      ['maxItems', { ...good, tags: ['x', 'y', 'z'] }],
    ];
    for (const [keyword, bad] of bads) {
      expect(original.safeParse(bad).success, `original should reject ${keyword} violation`).toBe(false);
      expect(rebuilt.safeParse(bad).success, `rebuilt should reject ${keyword} violation`).toBe(false);
    }
  });

  it('applies draft-4 boolean exclusive bounds against minimum/maximum', () => {
    const rebuilt = jsonSchemaToZod({
      type: 'number',
      minimum: 1,
      exclusiveMinimum: true,
      maximum: 5,
      exclusiveMaximum: true,
    });
    expect(rebuilt.safeParse(1).success).toBe(false);
    expect(rebuilt.safeParse(5).success).toBe(false);
    expect(rebuilt.safeParse(3).success).toBe(true);
  });

  it('applies constraints per branch of a multi-type schema', () => {
    const rebuilt = jsonSchemaToZod({ type: ['string', 'null'], minLength: 2 });
    expect(rebuilt.safeParse('ok').success).toBe(true);
    expect(rebuilt.safeParse(null).success).toBe(true);
    expect(rebuilt.safeParse('x').success).toBe(false);
  });

  it('rejects an uncompilable string pattern instead of dropping it', () => {
    expect(() => jsonSchemaToZod({ type: 'string', pattern: '(' })).toThrow(/invalid "pattern"/);

    const messages: string[] = [];
    const degraded = jsonSchemaToZod(
      { type: 'string', pattern: '(' },
      { onUnsupportedSchema: 'warn', onUnsupported: m => messages.push(m) },
    );
    expect(degraded.safeParse(123).success).toBe(true); // degraded to z.any()
    expect(messages[0]).toMatch(/invalid "pattern"/);
  });
});
