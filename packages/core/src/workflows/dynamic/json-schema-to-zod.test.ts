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

  it('rejects sibling constraints next to enum or const instead of dropping them', () => {
    expectRejected({ type: 'string', enum: ['a', 'bb'], maxLength: 1 }, 'maxLength', '#/maxLength');
    expectRejected({ type: 'string', const: 'x', minLength: 2 }, 'minLength', '#/minLength');
    expectRejected({ enum: ['a'], const: 'a' }, 'const', '#/const');
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

  it('returns ok for undefined (schema absent) but rejects null and empty object', () => {
    expect(validateStorableJsonSchema(undefined)).toEqual({ ok: true });
    expectRejected(null, 'type', '#');
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
      type: 'object',
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

  it('returns an issue instead of throwing when schema inspection fails', () => {
    const hostile = new Proxy(
      {},
      {
        getPrototypeOf() {
          throw new Error('inspection denied');
        },
      },
    );
    expect(() => validateStorableJsonSchema(hostile as JsonSchema)).not.toThrow();
    expect(validateStorableJsonSchema(hostile as JsonSchema)).toEqual({
      ok: false,
      issues: [expect.objectContaining({ pointer: '#', keyword: 'schema' })],
    });
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
      minItems: 2,
    });
    expect(zod.parse(['a', 1])).toEqual(['a', 1]);
    expect(zod.safeParse(['a']).success).toBe(false);
    expect(zod.safeParse(['a', 1, true]).success).toBe(false);
    expect(zod.safeParse([1, 'a']).success).toBe(false);
  });

  it('rejects open tuples (prefixItems without items: false)', () => {
    expectRejected({ type: 'array', prefixItems: [{ type: 'string' }] }, 'items');
  });

  it('rejects a closed tuple without the minimum length required by a fixed Zod tuple', () => {
    expectRejected(
      { type: 'array', prefixItems: [{ type: 'string' }, { type: 'number' }], items: false },
      'minItems',
      '#/minItems',
    );
  });

  it('rejects non-object items and contains instead of crashing convert', () => {
    expectRejected({ type: 'array', items: 'string' }, 'items', '#/items');
    expectRejected({ type: 'array', items: { type: 'number' }, contains: 1 }, 'contains', '#/contains');
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

  it('uniqueItems handles deeply nested JSON and rejects cyclic non-JSON values without throwing', () => {
    const schema = jsonSchemaToZod({
      type: 'array',
      items: { type: 'object', properties: {} },
      uniqueItems: true,
    });
    const buildDeep = () => {
      const root: Record<string, unknown> = {};
      let cursor = root;
      for (let i = 0; i < 5_000; i += 1) {
        const child: Record<string, unknown> = {};
        cursor.child = child;
        cursor = child;
      }
      return root;
    };
    expect(() => schema.safeParse([buildDeep(), buildDeep()])).not.toThrow();
    expect(schema.safeParse([buildDeep(), buildDeep()]).success).toBe(false);

    const cyclic: Record<string, unknown> = {};
    cyclic.self = cyclic;
    expect(() => schema.safeParse([cyclic])).not.toThrow();
    expect(schema.safeParse([cyclic]).success).toBe(false);
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
      propertyNames: { type: 'string', minLength: 2, maxLength: 2 },
    });
    expect(zod.parse({ ab: 1 })).toEqual({ ab: 1 });
    expect(zod.safeParse({ long: 1 }).success).toBe(false);
  });

  it('rejects own __proto__ input keys before object parsing can strip them', () => {
    const input = JSON.parse('{"__proto__":1}');
    const schemas = [
      { type: 'object' },
      { type: 'object', additionalProperties: false },
      { type: 'object', additionalProperties: { type: 'number' } },
      { type: 'object', propertyNames: false },
      { type: 'object', minProperties: 1 },
    ];
    for (const schema of schemas) {
      expect(jsonSchemaToZod(schema).safeParse(input).success).toBe(false);
    }
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

  it('preserves UUID string-representation semantics and rejects formats without a lossless converter', () => {
    const uuid = jsonSchemaToZod({ type: 'string', format: 'uuid' });
    expect(uuid.safeParse('00000000-0000-0000-0000-000000000000').success).toBe(true);
    expect(uuid.safeParse('123e4567-e89b-02d3-0456-426614174000').success).toBe(true);
    expect(uuid.safeParse('not-a-uuid').success).toBe(false);
    expectRejected({ type: 'string', format: 'email' }, 'format', '#/format');
    expectRejected({ type: 'string', format: 'uri' }, 'format', '#/format');
    expectRejected({ type: 'string', format: 'date-time' }, 'format', '#/format');
    expectRejected({ type: 'string', format: 'hostname' }, 'format', '#/format');
  });

  it('counts Unicode code points for string lengths', () => {
    const oneCharacter = jsonSchemaToZod({ type: 'string', minLength: 1, maxLength: 1 });
    expect(oneCharacter.safeParse('😀').success).toBe(true);
    expect(oneCharacter.safeParse('ab').success).toBe(false);
  });

  it('freezes string bounds when converting a retained schema object', () => {
    const schema: JsonSchema = { type: 'string', minLength: 2, maxLength: 3 };
    const rebuilt = jsonSchemaToZod(schema);

    schema.minLength = 0;
    schema.maxLength = 10;

    expect(rebuilt.safeParse('a').success).toBe(false);
    expect(rebuilt.safeParse('abcd').success).toBe(false);
  });
});

describe('primitive constraint keywords', () => {
  it('round-trips string/number/array constraints instead of silently widening', () => {
    const original = z.object({
      name: z.string().min(3).max(5),
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

  it('rejects string patterns because runtime RegExp matching is not bounded', () => {
    expectRejected({ type: 'string', pattern: '^a' }, 'pattern', '#/pattern');
  });

  it('rejects non-finite JavaScript numbers under every supported Zod peer', () => {
    const number = jsonSchemaToZod({ type: 'number' });
    expect(number.safeParse(Number.POSITIVE_INFINITY).success).toBe(false);
    expect(number.safeParse(Number.NEGATIVE_INFINITY).success).toBe(false);
  });

  it('uses JSON Schema integer and decimal multiple semantics', () => {
    const integer = jsonSchemaToZod({ type: 'integer' });
    expect(integer.safeParse(1e100).success).toBe(true);
    expect(integer.safeParse(1.5).success).toBe(false);

    const decimal = jsonSchemaToZod({ type: 'number', multipleOf: 0.1 });
    expect(decimal.safeParse(0.3).success).toBe(true);
    expect(decimal.safeParse(0.31).success).toBe(false);

    const subnormal = jsonSchemaToZod({ type: 'number', multipleOf: 5e-324 });
    expect(subnormal.safeParse(1e-323).success).toBe(true);
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

  it('measures maxSerializedBytes as UTF-8 bytes, not UTF-16 code units', () => {
    const schema = { type: 'string', default: '中'.repeat(11_000) };
    const serialized = JSON.stringify(schema);
    expect(serialized.length).toBeLessThan(ADMITTED_JSON_SCHEMA_BOUNDS.maxSerializedBytes);
    expect(new TextEncoder().encode(serialized).byteLength).toBeGreaterThan(
      ADMITTED_JSON_SCHEMA_BOUNDS.maxSerializedBytes,
    );
    expectRejected(schema, 'bytes', '#');
  });

  it('does not let rejected $defs exhaust the node budget before the keyword issue', () => {
    const definitions: Record<string, JsonSchema> = {};
    for (let i = 0; i < ADMITTED_JSON_SCHEMA_BOUNDS.maxNodes; i++) {
      definitions[`d${i}`] = { type: 'string' };
    }
    const result = validateStorableJsonSchema({
      type: 'string',
      $defs: definitions,
    });
    expect(result.ok).toBe(false);
    if (result.ok) return;
    expect(result.issues.some(issue => issue.keyword === '$defs' && issue.pointer === '#/$defs')).toBe(true);
    expect(result.issues.some(issue => issue.keyword === 'nodes')).toBe(false);
  });
});

describe('object property names', () => {
  it('rejects __proto__ property names', () => {
    expectRejected(
      JSON.parse('{"type":"object","properties":{"__proto__":{"type":"string"}}}'),
      'properties',
      '#/properties/__proto__',
    );
  });
});
