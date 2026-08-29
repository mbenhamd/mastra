import { describe, expect, it, vi } from 'vitest';

vi.mock('zod', async () => {
  const zodV3 = await import('zod/v3');
  return { ...zodV3, default: zodV3.default };
});

import { toStandardSchema } from '../../schema';
import { jsonSchemaToZod } from './json-schema-to-zod';
import type { JsonSchema } from './json-schema-to-zod';
import { toJsonSchemaOrUndefined } from './validate/schema-utils';

describe('jsonSchemaToZod with the supported Zod 3 peer', () => {
  it('rehydrates strict, passthrough, and annotated objects without Zod 4-only APIs', () => {
    const passthrough = jsonSchemaToZod({
      type: 'object',
      title: 'Payload',
      properties: { id: { type: 'string' } },
      required: ['id'],
    });
    expect(passthrough.parse({ id: 'a', extra: true })).toEqual({ id: 'a', extra: true });

    const strict = jsonSchemaToZod({
      type: 'object',
      properties: { id: { type: 'string' } },
      required: ['id'],
      additionalProperties: false,
    });
    expect(strict.safeParse({ id: 'a', extra: true }).success).toBe(false);
  });

  it('keeps finite-number, Unicode-length, and UUID behavior stable', () => {
    expect(jsonSchemaToZod({ type: 'number' }).safeParse(Number.POSITIVE_INFINITY).success).toBe(false);
    expect(jsonSchemaToZod({ type: 'integer' }).safeParse(1e100).success).toBe(true);
    expect(jsonSchemaToZod({ type: 'integer' }).safeParse(1.5).success).toBe(false);
    expect(jsonSchemaToZod({ type: 'number', multipleOf: 5e-324 }).safeParse(1e-323).success).toBe(true);
    expect(jsonSchemaToZod({ type: 'number', multipleOf: 5e-324 }).safeParse(1.5e-323).success).toBe(true);
    expect(jsonSchemaToZod({ type: 'string', maxLength: 1 }).safeParse('😀').success).toBe(true);
    expect(
      jsonSchemaToZod({ type: 'string', format: 'uuid' }).safeParse('123e4567-e89b-02d3-0456-426614174000').success,
    ).toBe(true);
  });

  it('rejects own __proto__ input keys before Zod 3 strips them', () => {
    const schema = jsonSchemaToZod({ type: 'object' });
    expect(schema.safeParse(JSON.parse('{"__proto__":1}')).success).toBe(false);
  });

  it('preserves admitted JSON Schema through the Zod 3 Standard Schema wrapper', () => {
    const source: JsonSchema = {
      type: 'array',
      items: { type: 'number' },
      contains: { const: 1 },
      minContains: 1,
    };
    const wrapped = toStandardSchema(jsonSchemaToZod(source) as any);

    expect(toJsonSchemaOrUndefined(wrapped)).toEqual(source);
  });
});
