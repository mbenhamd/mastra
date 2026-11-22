import { Validator } from '@cfworker/json-schema';
import type { OutputUnit, Schema } from '@cfworker/json-schema';
import type { StandardSchemaV1, StandardJSONSchemaV1 } from '@standard-schema/spec';
import type { JSONSchema7 } from 'json-schema';
import traverse from 'json-schema-traverse';
import type { StandardSchemaWithJSON, StandardSchemaWithJSONProps } from '../standard-schema.types';

/**
 * Vendor name for JSON Schema wrapped schemas.
 */
const VENDOR = 'json-schema';

/**
 * Options for creating a Standard Schema from JSON Schema.
 */
export interface JsonSchemaAdapterOptions {
  /**
   * Custom name for the schema (used for error messages).
   */
  name?: string;
}

/** Keywords whose value is one subschema. */
const SCHEMA_KEYWORDS = [
  'additionalItems',
  'additionalProperties',
  'contains',
  'else',
  'if',
  'items',
  'not',
  'propertyNames',
  'then',
  'unevaluatedItems',
  'unevaluatedProperties',
] as const;
/** Keywords whose value is an array of subschemas. */
const SCHEMA_ARRAY_KEYWORDS = ['allOf', 'anyOf', 'items', 'oneOf', 'prefixItems'] as const;
/** Keywords whose value maps names to subschemas. */
const SCHEMA_MAP_KEYWORDS = [
  '$defs',
  'definitions',
  'dependencies',
  'dependentSchemas',
  'patternProperties',
  'properties',
] as const;

function isSchemaObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/**
 * Thrown for a schema that uses `$dynamicRef`. `@cfworker/json-schema` does not
 * resolve it, so the reference would add no constraint and values Ajv2020
 * rejected would pass. Validation refuses such a schema instead.
 */
const UNSUPPORTED_DYNAMIC_REF_MESSAGE =
  '$dynamicRef is not supported: this validator cannot resolve it, so the schema is refused rather than checked loosely';

/**
 * Applies `nullable` and drops `format` on one schema and every subschema, by
 * keyword (draft-07 and 2020-12), and refuses `$dynamicRef`. Data keywords such
 * as `const`, `enum`, `default` and `examples` are never entered.
 */
function normalizeSchemaNode(node: unknown): void {
  if (!isSchemaObject(node)) {
    return;
  }
  if (node.$dynamicRef !== undefined) {
    throw new Error(UNSUPPORTED_DYNAMIC_REF_MESSAGE);
  }
  if (node.nullable === true && node.type !== undefined) {
    const types: unknown[] = Array.isArray(node.type) ? node.type : [node.type];
    if (!types.includes('null')) {
      node.type = [...types, 'null'];
    }
  }
  if (typeof node.format === 'string') {
    delete node.format;
  }
  for (const keyword of SCHEMA_KEYWORDS) {
    normalizeSchemaNode(node[keyword]);
  }
  for (const keyword of SCHEMA_ARRAY_KEYWORDS) {
    const value = node[keyword];
    if (Array.isArray(value)) {
      value.forEach(normalizeSchemaNode);
    }
  }
  for (const keyword of SCHEMA_MAP_KEYWORDS) {
    const value = node[keyword];
    if (isSchemaObject(value)) {
      // `dependencies` may also map to a property-name array; those are skipped.
      Object.values(value).forEach(normalizeSchemaNode);
    }
  }
}

/**
 * Copies a JSON Schema into the form the validator checks:
 * - OpenAPI `nullable: true` beside a `type` adds `"null"` to that type, as Ajv's
 *   built-in `nullable` keyword does; `enum` and `const` still apply.
 * - `format` is dropped, because validation has never checked formats here.
 * Only schema positions are visited, so a property named `nullable` or `format`
 * is left alone.
 */
function toValidationSchema(schema: JSONSchema7): Schema {
  const copy = structuredClone(schema) as Schema;
  normalizeSchemaNode(copy);
  return copy;
}

/** Nesting below which `withoutUndefinedProperties` stops looking. */
const UNDEFINED_PROPERTY_DEPTH = 256;

/**
 * Validates a value as its JSON form would: a plain-object property whose
 * value is `undefined` is left out, because JSON has no `undefined` and the
 * validator rejects it anywhere. Ajv ignored such properties only for
 * `properties`, `required` and `dependencies`; here every keyword sees the
 * object without them, as it would after a JSON round trip.
 *
 * The value is copied only along the paths that hold such a property, so a
 * value without one is validated as is. Copies keep every other own key,
 * including `__proto__`, and the object's prototype. Arrays keep their items.
 * An object reached twice reuses its finished copy. A reference back to an
 * object that is still being walked gets the original, so a cycle cannot
 * loop but an `undefined` property reached only through it is still
 * rejected; JSON has no cycles. Nesting past
 * {@link UNDEFINED_PROPERTY_DEPTH} is left as is.
 */
function withoutUndefinedProperties(value: unknown, depth = 0, copies = new Map<object, unknown>()): unknown {
  if (typeof value !== 'object' || value === null || depth > UNDEFINED_PROPERTY_DEPTH) {
    return value;
  }
  if (copies.has(value)) {
    return copies.get(value);
  }
  if (Array.isArray(value)) {
    copies.set(value, value);
    let copy: unknown[] | undefined;
    value.forEach((item, index) => {
      const next = withoutUndefinedProperties(item, depth + 1, copies);
      if (next !== item) {
        copy ??= [...value];
        copy[index] = next;
      }
    });
    if (copy) copies.set(value, copy);
    return copy ?? value;
  }
  const prototype = Object.getPrototypeOf(value);
  if (prototype !== Object.prototype && prototype !== null) {
    return value;
  }
  copies.set(value, value);
  const entries = Object.entries(value);
  const next = entries.map(
    ([key, property]) => [key, withoutUndefinedProperties(property, depth + 1, copies)] as const,
  );
  if (next.every(([, property], index) => property !== undefined && property === entries[index]![1])) {
    return value;
  }
  // defineProperty, not assignment: an own `__proto__` key stays a key.
  const copy = Object.create(prototype) as object;
  for (const [key, property] of next) {
    if (property !== undefined) {
      Object.defineProperty(copy, key, { value: property, enumerable: true, writable: true, configurable: true });
    }
  }
  copies.set(value, copy);
  return copy;
}

/**
 * Converts a validator instance location (e.g. "#/foo/0/bar") to a Standard Schema path.
 * Segments are URI-encoded JSON Pointer tokens.
 */
function instanceLocationToPath(instanceLocation: string): Array<PropertyKey> {
  const pointer = instanceLocation.startsWith('#') ? instanceLocation.slice(1) : instanceLocation;
  if (pointer === '') {
    return [];
  }
  return pointer
    .slice(1)
    .split('/')
    .map(segment => {
      const part = decodeURIComponent(segment).replace(/~1/g, '/').replace(/~0/g, '~');
      const num = parseInt(part, 10);
      return !isNaN(num) && String(num) === part ? num : part;
    });
}

/**
 * Keywords whose error only says that a subschema failed (for example
 * `Property "x" does not match schema.`). The subschema's own error follows it,
 * so these add nothing for the reader.
 */
const SUMMARY_KEYWORDS = new Set([
  'properties',
  'patternProperties',
  'items',
  'prefixItems',
  'additionalItems',
  'dependentSchemas',
  'allOf',
  '$ref',
  '$recursiveRef',
]);

function leafErrors(errors: OutputUnit[]): OutputUnit[] {
  return errors.filter(error => !SUMMARY_KEYWORDS.has(error.keyword));
}

/**
 * A wrapper class that makes JSON Schema compatible with @standard-schema/spec.
 *
 * This class implements both `StandardSchemaV1` (validation) and `StandardJSONSchemaV1`
 * (JSON Schema conversion) interfaces. Validation is performed by
 * `@cfworker/json-schema`, which interprets the schema and never generates code, so
 * it also runs where code generation from strings is disallowed.
 *
 * @typeParam T - The TypeScript type that the JSON Schema represents
 *
 * @example
 * ```typescript
 * import { toStandardSchema } from '@mastra/schema-compat/adapters/json-schema';
 *
 * const userJsonSchema = {
 *   type: 'object',
 *   properties: {
 *     name: { type: 'string' },
 *     age: { type: 'number', minimum: 0 },
 *   },
 *   required: ['name', 'age'],
 * } as const;
 *
 * const standardSchema = toStandardSchema<{ name: string; age: number }>(userJsonSchema);
 *
 * // Use validation (from StandardSchemaV1)
 * const result = standardSchema['~standard'].validate({ name: 'John', age: 30 });
 *
 * // Get JSON Schema (from StandardJSONSchemaV1)
 * const jsonSchema = standardSchema['~standard'].jsonSchema.output({ target: 'draft-07' });
 * ```
 */
export class JsonSchemaWrapper<Input = unknown, Output = Input> implements StandardSchemaWithJSON<Input, Output> {
  readonly #schema: JSONSchema7;
  #validator: Validator | null = null;

  readonly '~standard': StandardSchemaWithJSONProps<Input, Output>;

  constructor(schema: JSONSchema7, _options: JsonSchemaAdapterOptions = {}) {
    this.#schema = schema;

    // Create the ~standard property
    this['~standard'] = {
      version: 1,
      vendor: VENDOR,
      validate: this.#validate.bind(this),
      jsonSchema: {
        input: this.#toJsonSchema.bind(this),
        output: this.#toJsonSchema.bind(this),
      },
    };
  }

  /**
   * Validates a value against the JSON Schema.
   *
   * @param value - The value to validate
   * @returns A result object with either the validated value or validation issues
   */
  #validate(value: unknown): StandardSchemaV1.Result<Output> {
    try {
      const result = this.#getValidator().validate(withoutUndefinedProperties(value));

      if (result.valid) {
        return { value: value as Output };
      }

      const issues: StandardSchemaV1.Issue[] = leafErrors(result.errors).map(error => ({
        message: error.error,
        path: instanceLocationToPath(error.instanceLocation),
      }));

      return { issues };
    } catch (error) {
      // If validation fails unexpectedly, return a validation error
      const message = error instanceof Error ? error.message : 'Unknown validation error';
      return {
        issues: [{ message: `Schema validation error: ${message}` }],
      };
    }
  }

  /**
   * Returns the JSON Schema in the requested target format.
   *
   * Since we're already working with JSON Schema, this is mostly a pass-through.
   * Different targets may require slight modifications in the future.
   *
   * @param options - Options including the target format
   * @returns The JSON Schema as a Record
   */
  #toJsonSchema(options: StandardJSONSchemaV1.Options): Record<string, unknown> {
    const { target } = options;

    // Clone the schema to avoid mutations
    const clonedSchema = JSON.parse(JSON.stringify(this.#schema)) as Record<string, unknown>;

    // Add $schema if not present, based on target
    if (!clonedSchema.$schema) {
      switch (target) {
        case 'draft-07':
          clonedSchema.$schema = 'http://json-schema.org/draft-07/schema#';
          break;
        case 'draft-2020-12':
          clonedSchema.$schema = 'https://json-schema.org/draft/2020-12/schema';
          break;
        case 'openapi-3.0':
          // OpenAPI 3.0 doesn't use $schema
          break;
        default:
          // For unknown targets, don't add $schema
          break;
      }
    }

    if (options?.libraryOptions?.override) {
      const override = options.libraryOptions.override as (ctx: { jsonSchema: traverse.SchemaObject }) => void;
      traverse(clonedSchema, {
        cb: {
          post: schema => {
            override({
              jsonSchema: schema,
            });
          },
        },
      });
    }

    return clonedSchema;
  }

  /**
   * Gets or creates the validator for the schema. It is created on first use and
   * reused. Draft 2020-12 applies when `$schema` names it, draft-07 otherwise.
   * Every error is collected, not only the first.
   */
  #getValidator(): Validator {
    if (!this.#validator) {
      const is2020 = typeof this.#schema.$schema === 'string' && this.#schema.$schema.includes('2020-12');
      this.#validator = new Validator(toValidationSchema(this.#schema), is2020 ? '2020-12' : '7', false);
    }
    return this.#validator;
  }

  /**
   * Returns the original JSON Schema.
   */
  getSchema(): JSONSchema7 {
    return this.#schema;
  }
}

/**
 * Wraps a JSON Schema to implement the full @standard-schema/spec interface.
 *
 * This function creates a wrapper that implements both `StandardSchemaV1` (validation)
 * and `StandardJSONSchemaV1` (JSON Schema conversion) interfaces. Validation is performed
 * by `@cfworker/json-schema`, which never generates code.
 *
 * @typeParam T - The TypeScript type that the JSON Schema represents
 * @param schema - The JSON Schema to wrap
 * @param options - Optional configuration options
 * @returns A wrapper implementing StandardSchemaWithJSON
 *
 * @example
 * ```typescript
 * import { toStandardSchema } from '@mastra/schema-compat/adapters/json-schema';
 *
 * const userSchema = {
 *   type: 'object',
 *   properties: {
 *     name: { type: 'string' },
 *     age: { type: 'number' },
 *   },
 *   required: ['name'],
 * } as const;
 *
 * type User = { name: string; age?: number };
 *
 * const standardSchema = toStandardSchema<User>(userSchema);
 *
 * // Validate data
 * const result = standardSchema['~standard'].validate({ name: 'John' });
 *
 * // Get JSON Schema
 * const jsonSchema = standardSchema['~standard'].jsonSchema.output({ target: 'draft-07' });
 * ```
 */
export function toStandardSchema<T = unknown>(
  schema: JSONSchema7,
  options?: JsonSchemaAdapterOptions,
): JsonSchemaWrapper<T, T> {
  return new JsonSchemaWrapper<T, T>(schema, options);
}
