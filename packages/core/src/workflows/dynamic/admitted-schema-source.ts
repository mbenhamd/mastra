import type { JsonSchema } from './json-schema-to-zod';

/**
 * Runtime-only provenance for Zod schemas reconstructed from admitted JSON
 * Schema. Zod's JSON-Schema exporter cannot reproduce every admitted keyword
 * (for example `contains`, Unicode string lengths, or closed 2020-12 tuples),
 * so persistence must reuse the validated source instead of widening it.
 *
 * A WeakMap keeps this metadata private and avoids changing the public schema
 * object. The Zod 3 Standard Schema adapter wraps its schema with
 * `Object.create(schema)`, so reads also check that one immediate prototype.
 * Both writes and reads clone the JSON value so callers cannot mutate the
 * remembered contract by retaining either reference.
 */
const admittedJsonSchemaSources = new WeakMap<object, JsonSchema>();

function cloneJsonSchema(schema: JsonSchema): JsonSchema {
  return JSON.parse(JSON.stringify(schema)) as JsonSchema;
}

export function rememberAdmittedJsonSchema<T extends object>(schema: T, source: JsonSchema): T {
  admittedJsonSchemaSources.set(schema, cloneJsonSchema(source));
  return schema;
}

export function getAdmittedJsonSchema(schema: unknown): JsonSchema | undefined {
  if ((typeof schema !== 'object' || schema === null) && typeof schema !== 'function') return undefined;
  const key = schema as object;
  const directSource = admittedJsonSchemaSources.get(key);
  if (directSource !== undefined) return cloneJsonSchema(directSource);

  // `toStandardSchema()` uses exactly one Object.create() layer for Zod 3.
  // Do not walk arbitrary prototype chains: provenance belongs only to the
  // admitted schema itself or that known adapter wrapper.
  let prototype: object | null;
  try {
    prototype = Object.getPrototypeOf(key) as object | null;
  } catch {
    return undefined;
  }
  const source = prototype === null ? undefined : admittedJsonSchemaSources.get(prototype);
  return source === undefined ? undefined : cloneJsonSchema(source);
}
