/**
 * Conformance fixtures: admitted source schema vs rehydrated Zod.
 * Each accept case asserts parse output (not a serialized snapshot).
 * Each reject case asserts JSON Pointer + keyword.
 */
import { describe, expect, it } from 'vitest';

import {
  ADMITTED_JSON_SCHEMA_DIALECT,
  jsonSchemaToZod,
  UnsupportedJsonSchemaError,
  validateStorableJsonSchema,
} from './json-schema-to-zod';
import type { JsonSchema } from './json-schema-to-zod';

type AcceptCase = {
  name: string;
  schema: JsonSchema;
  valid: Array<{ input: unknown; output: unknown }>;
  invalid: unknown[];
};

type RejectCase = {
  name: string;
  schema: unknown;
  keyword: string;
  pointer: string;
};

const ACCEPT: AcceptCase[] = [
  {
    name: 'string with length constraints',
    schema: { type: 'string', minLength: 2, maxLength: 4 },
    valid: [{ input: 'ab', output: 'ab' }],
    invalid: ['a', 'abcde'],
  },
  {
    name: 'integer exclusive bounds',
    schema: { type: 'integer', exclusiveMinimum: 0, exclusiveMaximum: 5 },
    valid: [{ input: 1, output: 1 }],
    invalid: [0, 5, 1.5],
  },
  {
    name: 'nullable string via type array',
    schema: { type: ['string', 'null'], minLength: 1 },
    valid: [
      { input: 'x', output: 'x' },
      { input: null, output: null },
    ],
    invalid: ['', 1],
  },
  {
    name: 'object additionalProperties false',
    schema: {
      type: 'object',
      properties: { id: { type: 'string' } },
      required: ['id'],
      additionalProperties: false,
    },
    valid: [{ input: { id: 'a' }, output: { id: 'a' } }],
    invalid: [{}, { id: 1 }, { id: 'a', extra: true }],
  },
  {
    name: 'object additionalProperties schema',
    schema: {
      type: 'object',
      properties: { id: { type: 'string' } },
      additionalProperties: { type: 'boolean' },
    },
    valid: [{ input: { id: 'a', flag: true }, output: { id: 'a', flag: true } }],
    invalid: [{ id: 'a', flag: 1 }],
  },
  {
    name: 'closed tuple',
    schema: {
      type: 'array',
      prefixItems: [{ const: 'ok' }, { type: 'number' }],
      items: false,
      minItems: 2,
    },
    valid: [{ input: ['ok', 2], output: ['ok', 2] }],
    invalid: [['ok'], ['ok', 2, 3], [2, 'ok']],
  },
  {
    name: 'enum mixed literals',
    schema: { enum: ['a', 1, false] },
    valid: [
      { input: 'a', output: 'a' },
      { input: 1, output: 1 },
      { input: false, output: false },
    ],
    invalid: [true, '1', null],
  },
  {
    name: 'uuid format',
    schema: { type: 'string', format: 'uuid' },
    valid: [
      { input: '00000000-0000-0000-0000-000000000000', output: '00000000-0000-0000-0000-000000000000' },
      { input: '123e4567-e89b-02d3-0456-426614174000', output: '123e4567-e89b-02d3-0456-426614174000' },
    ],
    invalid: ['not-a-uuid'],
  },
  {
    name: 'draft-07 $schema alias is admitted as historical output',
    schema: {
      $schema: 'http://json-schema.org/draft-07/schema#',
      type: 'object',
      properties: { id: { type: 'string' } },
      required: ['id'],
      additionalProperties: false,
    },
    valid: [{ input: { id: 'a' }, output: { id: 'a' } }],
    invalid: [{}, { id: 'a', extra: 1 }],
  },
];

const REJECT: RejectCase[] = [
  { name: 'oneOf', schema: { oneOf: [{ type: 'string' }] }, keyword: 'oneOf', pointer: '#/oneOf' },
  {
    name: 'anyOf nested',
    schema: { type: 'object', properties: { x: { anyOf: [{ type: 'string' }] } } },
    keyword: 'anyOf',
    pointer: '#/properties/x/anyOf',
  },
  { name: '$ref', schema: { $ref: '#/$defs/x' }, keyword: '$ref', pointer: '#/$ref' },
  {
    name: 'patternProperties',
    schema: { type: 'object', patternProperties: { '^x': { type: 'string' } } },
    keyword: 'patternProperties',
    pointer: '#/patternProperties',
  },
  {
    name: 'unevaluatedProperties',
    schema: { type: 'object', unevaluatedProperties: false },
    keyword: 'unevaluatedProperties',
    pointer: '#/unevaluatedProperties',
  },
  {
    name: 'if/then',
    schema: { type: 'object', if: { type: 'object' }, then: { type: 'object' } },
    keyword: 'if',
    pointer: '#/if',
  },
  {
    name: 'draft-07 tuple items',
    schema: { type: 'array', items: [{ type: 'string' }] },
    keyword: 'items',
    pointer: '#/items',
  },
  { name: 'boolean schema', schema: true, keyword: 'boolean-schema', pointer: '#' },
  { name: 'untyped node', schema: { title: 'x' }, keyword: 'type', pointer: '#' },
  { name: 'string pattern', schema: { type: 'string', pattern: '^a' }, keyword: 'pattern', pointer: '#/pattern' },
  { name: 'email format', schema: { type: 'string', format: 'email' }, keyword: 'format', pointer: '#/format' },
  { name: 'URI format', schema: { type: 'string', format: 'uri' }, keyword: 'format', pointer: '#/format' },
  {
    name: 'date-time format',
    schema: { type: 'string', format: 'date-time' },
    keyword: 'format',
    pointer: '#/format',
  },
  { name: 'unknown format', schema: { type: 'string', format: 'idn-email' }, keyword: 'format', pointer: '#/format' },
  {
    name: 'contentEncoding',
    schema: { type: 'string', contentEncoding: 'base64' },
    keyword: 'contentEncoding',
    pointer: '#/contentEncoding',
  },
  {
    name: 'wrong $schema',
    schema: { $schema: 'https://json-schema.org/draft/2019-09/schema', type: 'string' },
    keyword: '$schema',
    pointer: '#/$schema',
  },
  {
    name: 'nested $schema',
    schema: { type: 'object', properties: { x: { $schema: ADMITTED_JSON_SCHEMA_DIALECT, type: 'string' } } },
    keyword: '$schema',
    pointer: '#/properties/x/$schema',
  },
  {
    name: 'enum sibling maxLength',
    schema: { type: 'string', enum: ['a', 'bb'], maxLength: 1 },
    keyword: 'maxLength',
    pointer: '#/maxLength',
  },
];

describe('admitted JSON Schema conformance — accept', () => {
  it.each(ACCEPT)('$name', ({ schema, valid, invalid }) => {
    expect(validateStorableJsonSchema(schema)).toEqual({ ok: true });
    const zod = jsonSchemaToZod(schema);
    for (const { input, output } of valid) {
      expect(zod.parse(input)).toEqual(output);
    }
    for (const input of invalid) {
      expect(zod.safeParse(input).success, `should reject ${JSON.stringify(input)}`).toBe(false);
    }
  });
});

describe('admitted JSON Schema conformance — reject', () => {
  it.each(REJECT)('$name', ({ schema, keyword, pointer }) => {
    const result = validateStorableJsonSchema(schema as JsonSchema);
    expect(result.ok).toBe(false);
    if (result.ok) return;
    expect(
      result.issues.some(issue => issue.keyword === keyword && issue.pointer === pointer),
      JSON.stringify(result.issues),
    ).toBe(true);
    try {
      jsonSchemaToZod(schema as JsonSchema);
      expect.unreachable('should throw');
    } catch (error) {
      expect(error).toBeInstanceOf(UnsupportedJsonSchemaError);
    }
  });
});
