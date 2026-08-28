/**
 * Fail-closed admission + conversion for persisted dynamic-workflow schemas.
 * Unsupported keywords must not rehydrate as a weaker Zod validator.
 */
import { describe, expect, it } from 'vitest';
import { z } from 'zod';

import {
  ADMITTED_JSON_SCHEMA_BOUNDS,
  ADMITTED_JSON_SCHEMA_DIALECT,
  jsonSchemaToZod,
  UnsupportedJsonSchemaError,
  validateStorableJsonSchema,
} from './json-schema-to-zod';
import type { JsonSchema } from './json-schema-to-zod';

function expectRejected(schema: unknown, keyword: string, pointer?: string): void {
  const result = validateStorableJsonSchema(schema as JsonSchema);
  expect(result.ok).toBe(false);
  if (result.ok) return;
  expect(result.issues.some(issue => issue.keyword === keyword && (pointer ? issue.pointer === pointer : true))).toBe(
    true,
  );
  expect(() => jsonSchemaToZod(schema as JsonSchema)).toThrow(UnsupportedJsonSchemaError);
}

describe('jsonSchemaToZod admitted dialect', () => {
  it('declares the canonical 2020-12 dialect', () => {
    expect(ADMITTED_JSON_SCHEMA_DIALECT).toBe('https://json-schema.org/draft/2020-12/schema');
  });

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
    const parsed = zod.parse({ name: 'Tony', tags: ['a', 'b'] });
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
    expectRejected({ const: { nested: true } }, 'const', '#/const');
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
    expectRejected({ enum: [{ bad: true }] }, 'enum');
  });

  it('rejects draft-07 tuple-form items instead of widening', () => {
    expectRejected({ type: 'array', items: [{ type: 'string' }, { type: 'number' }] }, 'items', '#/items');
  });

  it.each(['oneOf', 'anyOf', 'allOf', 'not', '$ref', 'patternProperties', 'discriminator'])(
    'rejects unsupported keyword %s at write and convert',
    keyword => {
      expectRejected({ type: 'string', [keyword]: [{ type: 'string' }] }, keyword);
    },
  );

  it('rejects nested unsupported keywords with a JSON pointer', () => {
    expectRejected(
      {
        type: 'object',
        properties: { payload: { anyOf: [{ type: 'string' }, { type: 'number' }] } },
      },
      'anyOf',
      '#/properties/payload/anyOf',
    );
  });

  it('throws UnsupportedJsonSchemaError listing every offense, never z.any()', () => {
    try {
      jsonSchemaToZod({ oneOf: [{ type: 'string' }] } as JsonSchema);
      expect.unreachable('should throw');
    } catch (error) {
      expect(error).toBeInstanceOf(UnsupportedJsonSchemaError);
      const issues = (error as UnsupportedJsonSchemaError).issues;
      expect(issues.some(issue => issue.keyword === 'oneOf')).toBe(true);
      expect(issues.some(issue => issue.keyword === 'type')).toBe(true);
    }
  });

  it('throws on unsupported type keyword', () => {
    expectRejected({ type: 'never' }, 'type');
  });

  it('rejects a bare schema with no type (would rehydrate unconstrained)', () => {
    expectRejected({ description: 'freeform' }, 'type', '#');
  });
});

describe('validateStorableJsonSchema', () => {
  it('returns ok for a schema with only admitted keywords', () => {
    const result = validateStorableJsonSchema({
      $schema: ADMITTED_JSON_SCHEMA_DIALECT,
      type: 'object',
      properties: { name: { type: 'string' }, tags: { type: 'array', items: { type: 'string' } } },
      required: ['name'],
    });
    expect(result).toEqual({ ok: true });
  });

  it('returns ok for undefined / null (schema absent) but not empty object', () => {
    expect(validateStorableJsonSchema(undefined)).toEqual({ ok: true });
    expect(validateStorableJsonSchema(null as unknown as JsonSchema)).toEqual({ ok: true });
    expectRejected({}, 'type', '#');
  });

  it('flags top-level oneOf with a JSON pointer', () => {
    const result = validateStorableJsonSchema({ oneOf: [{ type: 'string' }, { type: 'number' }] });
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.issues.some(issue => issue.pointer === '#/oneOf' && issue.keyword === 'oneOf')).toBe(true);
    }
  });

  it('flags unsupported keywords nested inside array items', () => {
    const result = validateStorableJsonSchema({
      type: 'array',
      items: { allOf: [{ type: 'string' }] },
    });
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.issues.some(issue => issue.pointer === '#/items/allOf' && issue.keyword === 'allOf')).toBe(true);
    }
  });

  it('collects multiple offenses in one walk', () => {
    const result = validateStorableJsonSchema({
      oneOf: [{ type: 'string' }],
      properties: { x: { $ref: '#/definitions/foo' } },
    });
    expect(result.ok).toBe(false);
    if (!result.ok) {
      const keys = result.issues.map(issue => `${issue.pointer}:${issue.keyword}`);
      expect(keys.some(key => key.includes('oneOf'))).toBe(true);
      expect(keys.some(key => key.includes('$ref'))).toBe(true);
    }
  });
});

describe('additionalProperties', () => {
  it('omitted additionalProperties passthroughs extras (2020-12 default true)', () => {
    const zod = jsonSchemaToZod({
      type: 'object',
      properties: { name: { type: 'string' } },
      required: ['name'],
    });
    expect(zod.parse({ name: 'Ada', extra: 1 })).toEqual({ name: 'Ada', extra: 1 });
  });

  it('additionalProperties: true keeps extras', () => {
    const zod = jsonSchemaToZod({
      type: 'object',
      properties: { name: { type: 'string' } },
      additionalProperties: true,
    });
    expect(zod.parse({ name: 'Ada', extra: 1 })).toEqual({ name: 'Ada', extra: 1 });
  });

  it('additionalProperties: false rejects extras', () => {
    const zod = jsonSchemaToZod({
      type: 'object',
      properties: { name: { type: 'string' } },
      additionalProperties: false,
    });
    expect(zod.parse({ name: 'Ada' })).toEqual({ name: 'Ada' });
    expect(zod.safeParse({ name: 'Ada', extra: 1 }).success).toBe(false);
  });

  it('schema-valued additionalProperties validates extras', () => {
    const zod = jsonSchemaToZod({
      type: 'object',
      properties: { name: { type: 'string' } },
      additionalProperties: { type: 'number' },
    });
    expect(zod.parse({ name: 'Ada', n: 2 })).toEqual({ name: 'Ada', n: 2 });
    expect(zod.safeParse({ name: 'Ada', n: 'no' }).success).toBe(false);
  });
});

describe('arrays, tuples, uniqueItems, contains', () => {
  it('homogeneous arrays keep item + length constraints', () => {
    const zod = jsonSchemaToZod({ type: 'array', items: { type: 'string' }, minItems: 1, maxItems: 2 });
    expect(zod.parse(['a'])).toEqual(['a']);
    expect(zod.safeParse([]).success).toBe(false);
    expect(zod.safeParse(['a', 'b', 'c']).success).toBe(false);
    expect(zod.safeParse([1]).success).toBe(false);
  });

  it('closed prefixItems tuples rehydrate as fixed-length tuples', () => {
    const zod = jsonSchemaToZod({
      type: 'array',
      prefixItems: [{ type: 'string' }, { type: 'number' }],
      items: false,
    });
    expect(zod.parse(['a', 1])).toEqual(['a', 1]);
    expect(zod.safeParse(['a']).success).toBe(false);
    expect(zod.safeParse(['a', 1, true]).success).toBe(false);
    expect(zod.safeParse([1, 'a']).success).toBe(false);
  });

  it('rejects open tuples (prefixItems without items: false)', () => {
    expectRejected({ type: 'array', prefixItems: [{ type: 'string' }] }, 'items');
  });

  it('uniqueItems rejects duplicates including object-key-order variants', () => {
    const zod = jsonSchemaToZod({
      type: 'array',
      items: { type: 'object', properties: { a: { type: 'number' }, b: { type: 'number' } } },
      uniqueItems: true,
    });
    expect(zod.parse([{ a: 1, b: 2 }])).toEqual([{ a: 1, b: 2 }]);
    expect(
      zod.safeParse([
        { a: 1, b: 2 },
        { b: 2, a: 1 },
      ]).success,
    ).toBe(false);
  });

  it('contains / minContains / maxContains are enforced', () => {
    const zod = jsonSchemaToZod({
      type: 'array',
      items: { type: 'number' },
      contains: { const: 1 },
      minContains: 1,
      maxContains: 2,
    });
    expect(zod.parse([1, 2])).toEqual([1, 2]);
    expect(zod.safeParse([2, 3]).success).toBe(false);
    expect(zod.safeParse([1, 1, 1]).success).toBe(false);
  });
});

describe('object property counts and propertyNames', () => {
  it('minProperties / maxProperties are enforced after passthrough', () => {
    const zod = jsonSchemaToZod({
      type: 'object',
      properties: { a: { type: 'string' } },
      minProperties: 2,
      maxProperties: 3,
    });
    expect(zod.safeParse({ a: 'x' }).success).toBe(false);
    expect(zod.parse({ a: 'x', b: 1 })).toEqual({ a: 'x', b: 1 });
    expect(zod.safeParse({ a: 'x', b: 1, c: 2, d: 3 }).success).toBe(false);
  });

  it('propertyNames constrains keys', () => {
    const zod = jsonSchemaToZod({
      type: 'object',
      additionalProperties: { type: 'number' },
      propertyNames: { type: 'string', pattern: '^[a-z]+$' },
    });
    expect(zod.parse({ ab: 1 })).toEqual({ ab: 1 });
    expect(zod.safeParse({ 'A-b': 1 }).success).toBe(false);
  });

  it('rejects nested minProperties that would previously have been dropped', () => {
    expectRejected(
      {
        type: 'object',
        properties: {
          nested: { type: 'object', properties: {}, minProperties: 1, unevaluatedProperties: false },
        },
      },
      'unevaluatedProperties',
    );
  });
});

describe('nullable type arrays, literals, annotations', () => {
  it('applies constraints per branch of a multi-type schema', () => {
    const rebuilt = jsonSchemaToZod({ type: ['string', 'null'], minLength: 2 });
    expect(rebuilt.safeParse('ok').success).toBe(true);
    expect(rebuilt.safeParse(null).success).toBe(true);
    expect(rebuilt.safeParse('x').success).toBe(false);
  });

  it('keeps description and records default as annotation, not a parse default', () => {
    const zod = jsonSchemaToZod({
      type: 'string',
      description: 'name',
      title: 'Name',
      default: 'anon',
    });
    expect(zod.description).toBe('name');
    expect(zod.safeParse(undefined).success).toBe(false);
    expect(zod.parse('Ada')).toBe('Ada');
  });

  it('admits known string formats and rejects unknown ones', () => {
    expect(jsonSchemaToZod({ type: 'string', format: 'email' }).safeParse('a@b.com').success).toBe(true);
    expect(jsonSchemaToZod({ type: 'string', format: 'email' }).safeParse('nope').success).toBe(false);
    expectRejected({ type: 'string', format: 'hostname' }, 'format', '#/format');
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
    const json = z.toJSONSchema(original, { target: 'draft-2020-12' }) as JsonSchema;
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

  it('rejects draft-4 boolean exclusive bounds instead of misreading them', () => {
    expectRejected({ type: 'number', minimum: 1, exclusiveMinimum: true }, 'exclusiveMinimum', '#/exclusiveMinimum');
  });

  it('rejects an uncompilable string pattern instead of dropping it', () => {
    expectRejected({ type: 'string', pattern: '(' }, 'pattern', '#/pattern');
  });
});

describe('converter bounds', () => {
  it('rejects too many object properties', () => {
    const properties: Record<string, JsonSchema> = {};
    for (let i = 0; i < ADMITTED_JSON_SCHEMA_BOUNDS.maxProperties + 1; i++) {
      properties[`p${i}`] = { type: 'string' };
    }
    expectRejected({ type: 'object', properties }, 'properties');
  });

  it('rejects excessive depth', () => {
    let schema: JsonSchema = { type: 'string' };
    for (let i = 0; i < ADMITTED_JSON_SCHEMA_BOUNDS.maxDepth + 2; i++) {
      schema = { type: 'object', properties: { nested: schema } };
    }
    expectRejected(schema, 'depth');
  });

  it('rejects oversized enums', () => {
    const values = Array.from({ length: ADMITTED_JSON_SCHEMA_BOUNDS.maxEnumSize + 1 }, (_, i) => `v${i}`);
    expectRejected({ enum: values }, 'enum');
  });
});
