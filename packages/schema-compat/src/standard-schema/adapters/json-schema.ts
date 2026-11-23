import type { OutputUnit, Schema, Validator } from '@cfworker/json-schema';
import type { StandardSchemaV1, StandardJSONSchemaV1 } from '@standard-schema/spec';
import type { JSONSchema7 } from 'json-schema';
import traverse from 'json-schema-traverse';
import type { StandardSchemaWithJSON, StandardSchemaWithJSONProps } from '../standard-schema.types';

/**
 * Vendor name for JSON Schema wrapped schemas.
 */
const VENDOR = 'json-schema';

/**
 * `@cfworker/json-schema` builds a URL from `self.location` while it is imported, which throws in
 * a browser worker created from a blob URL. It is imported lazily, starting when this module loads,
 * so importing this package never fails there. Until that import has finished, and if it fails,
 * validation fails closed with a "JSON Schema validator unavailable" issue; it never reports
 * success. Callers that can await should await {@link jsonSchemaValidatorReady} first while
 * {@link jsonSchemaValidatorLoading} is true.
 */
let ValidatorClass: typeof Validator | undefined;
let validatorUnavailableReason = 'still loading';
const validatorLoad: Promise<void> = import('@cfworker/json-schema').then(
  module => {
    ValidatorClass = module.Validator;
  },
  (error: unknown) => {
    validatorUnavailableReason = error instanceof Error ? error.message : String(error);
  },
);

/**
 * Every copy of this adapter in the process registers its load here until it settles: a package can
 * bundle its own copy (`@mastra/mcp` does), or an install can hold two versions. Awaiting any copy's
 * {@link jsonSchemaValidatorReady} then also waits for the copy that built the schema being validated.
 */
const VALIDATOR_LOADS: unique symbol = Symbol.for('mastra.schemaCompat.jsonSchemaValidatorLoads');
const validatorLoads: Set<Promise<void>> = ((globalThis as { [VALIDATOR_LOADS]?: Set<Promise<void>> })[
  VALIDATOR_LOADS
] ??= new Set());
validatorLoads.add(validatorLoad);
void validatorLoad.then(() => {
  validatorLoads.delete(validatorLoad);
});

/**
 * Whether any JSON Schema validator copy in the process is still loading. Once it is false, validation
 * needs no wait, so a caller can skip awaiting {@link jsonSchemaValidatorReady} and keep its timing.
 */
export function jsonSchemaValidatorLoading(): boolean {
  return validatorLoads.size > 0;
}

/**
 * Resolves once every JSON Schema validator copy loading so far has loaded or failed to load; it never rejects.
 */
export function jsonSchemaValidatorReady(): Promise<void> {
  return Promise.all(validatorLoads).then(() => undefined);
}

/**
 * Options for creating a Standard Schema from JSON Schema.
 */
export interface JsonSchemaAdapterOptions {
  /**
   * Custom name for the schema (used for error messages).
   */
  name?: string;
}

/** Keywords whose value maps names to subschemas. */
const SCHEMA_MAP_KEYWORDS = [
  '$defs',
  'definitions',
  'dependencies',
  'dependentRequired',
  'dependentSchemas',
  'patternProperties',
  'properties',
] as const;

/** Keywords whose value is literal data the validator compares or skips, never a schema. */
const DATA_KEYWORDS = new Set<string>(['const', 'default', 'enum']);

/**
 * Thrown for a schema that uses `$dynamicRef`. `@cfworker/json-schema` does not
 * resolve it, so the reference would add no constraint and values Ajv2020
 * rejected would pass. Validation refuses such a schema instead.
 */
const UNSUPPORTED_DYNAMIC_REF_MESSAGE =
  '$dynamicRef is not supported: this validator cannot resolve it, so the schema is refused rather than checked loosely';

const SCHEMA_MAP_KEYWORD_SET = new Set<string>(SCHEMA_MAP_KEYWORDS);

/**
 * Refuses a schema in which any object at any depth has a `$dynamicRef` key.
 * The validator can reach a subschema through a keyword, through a `$ref`
 * pointer to any path (for example `#/components/schemas/Node`), and through
 * `dependencies`, so the walk covers every object rather than mirroring one of
 * those routes. The one exception is the object under a name-map keyword
 * (`properties`, `$defs`, ...): its keys are names, so a property named
 * `$dynamicRef` with a schema value is not refused (a string value is), while
 * every value under it is still walked.
 * Data under `const` or `enum` that contains a `$dynamicRef` key is refused
 * too; failing closed there is preferred to telling data from schemas.
 */
function refuseDynamicRef(node: unknown, isNameMap = false, seen = new Set<object>()): void {
  if (typeof node !== 'object' || node === null || seen.has(node)) {
    return;
  }
  seen.add(node);
  if (Array.isArray(node)) {
    node.forEach(item => refuseDynamicRef(item, false, seen));
    return;
  }
  // On a name map a property named `$dynamicRef` holds a schema object; a string
  // there is never a name-map entry, and a `$ref` can still make the validator
  // read that object as a schema, so it is refused too.
  if (
    Object.hasOwn(node, '$dynamicRef') &&
    (!isNameMap || typeof (node as Record<string, unknown>).$dynamicRef === 'string')
  ) {
    throw new Error(UNSUPPORTED_DYNAMIC_REF_MESSAGE);
  }
  for (const [key, value] of Object.entries(node)) {
    refuseDynamicRef(value, !isNameMap && SCHEMA_MAP_KEYWORD_SET.has(key), seen);
  }
}

/**
 * Rewrites draft-07 `dependencies` as its 2020-12 forms: a property-name array
 * goes to `dependentRequired`, a schema to `dependentSchemas`. The validator
 * resolves `$ref` only inside maps it knows hold schemas; under `dependencies`
 * it skipped an entry named like a keyword (`type`, `minimum`, ...), so a
 * `$ref` there stayed unresolved and every value was refused. An existing
 * `dependentRequired` entry for the same name is merged; an existing
 * `dependentSchemas` entry is combined with `allOf`, so both still apply.
 */
function splitDependencies(record: Record<string, unknown>): void {
  const dependencies = record.dependencies;
  if (typeof dependencies !== 'object' || dependencies === null || Array.isArray(dependencies)) {
    return;
  }
  const required = { ...(record.dependentRequired as Record<string, unknown[]> | undefined) };
  const schemas = { ...(record.dependentSchemas as Record<string, unknown> | undefined) };
  for (const [name, value] of Object.entries(dependencies)) {
    if (Array.isArray(value)) {
      required[name] = Object.hasOwn(required, name) ? [...new Set([...required[name]!, ...value])] : value;
    } else {
      schemas[name] = Object.hasOwn(schemas, name) ? { allOf: [schemas[name], value] } : value;
    }
  }
  delete record.dependencies;
  if (Object.keys(required).length > 0) record.dependentRequired = required;
  if (Object.keys(schemas).length > 0) record.dependentSchemas = schemas;
}

/**
 * Applies `nullable` and drops `format` and `id` on every
 * object the validator can read as a schema. A `$ref` can point at any path
 * (for example `#/components/schemas/User`), so the walk covers every object
 * rather than only subschemas under schema keywords; a `format` left on a
 * `$ref` target was checked, and the validator's `url` pattern takes seconds on
 * a 30-character string. Values under data keywords (`const`, `enum`,
 * `default`, ...) are never entered. On a name map (`properties`, `$defs`, ...)
 * the keys are names, so the map itself is left alone and only its values are
 * walked as schemas.
 */
function normalizeSchemaObjects(node: unknown, isNameMap = false, seen = new Set<object>()): void {
  if (typeof node !== 'object' || node === null || seen.has(node)) {
    return;
  }
  seen.add(node);
  if (Array.isArray(node)) {
    node.forEach(item => normalizeSchemaObjects(item, false, seen));
    return;
  }
  const record = node as Record<string, unknown>;
  if (!isNameMap) {
    if (record.nullable === true && record.type !== undefined) {
      const types: unknown[] = Array.isArray(record.type) ? record.type : [record.type];
      if (!types.includes('null')) {
        record.type = [...types, 'null'];
      }
    }
    if (typeof record.format === 'string') {
      delete record.format;
    }
    // The validator reads `id` on every object it walks as a schema URI, so two
    // objects that are not schemas, such as two `example: { id: 1 }` or
    // `x-meta: { id: 'u' }`, made every call fail with a duplicate schema URI.
    // Only draft-04 used `id`, and Ajv ignored it; `$id` is unaffected.
    delete record.id;
    splitDependencies(record);
  }
  for (const [key, value] of Object.entries(record)) {
    if (!isNameMap && DATA_KEYWORDS.has(key)) {
      continue;
    }
    normalizeSchemaObjects(value, !isNameMap && SCHEMA_MAP_KEYWORD_SET.has(key), seen);
  }
}

/**
 * Copies a JSON Schema into the form the validator checks:
 * - OpenAPI `nullable: true` beside a `type` adds `"null"` to that type, as Ajv's
 *   built-in `nullable` keyword does; `enum` and `const` still apply.
 * - `format` is dropped, because validation has never checked formats here.
 * - `id` is dropped: only draft-04 used it, Ajv ignored it, and the validator
 *   read it on objects that are not schemas.
 * - `dependencies` is rewritten as `dependentRequired` and `dependentSchemas`.
 * Every object is visited except literal data and name maps, so a property
 * named `nullable` or `format` is left alone, and a `$ref` target is treated
 * like any other subschema.
 */
function toValidationSchema(schema: JSONSchema7): Schema {
  const copy = structuredClone(schema) as Schema;
  normalizeSchemaObjects(copy);
  return copy;
}

/** Nesting below which `withoutUndefinedProperties` stops looking. */
const UNDEFINED_PROPERTY_DEPTH = 256;

/**
 * Validates a value as its JSON form would: a plain-object property whose
 * value is `undefined` is left out, because JSON has no `undefined` and the
 * validator rejects it anywhere. As with Ajv, `additionalProperties` and
 * `unevaluatedProperties` still see the key: see {@link undefinedKeysRefusedByClosedObjects}.
 *
 * The value is copied only along the paths that hold such a property, so a
 * value without one is validated as is. Copies keep every other own key,
 * including `__proto__`, and the object's prototype. Arrays keep their items.
 * An object reached twice reuses its finished copy. A reference back to an
 * object that is still being walked gets the original, so a cycle cannot
 * loop but an `undefined` property reached only through it is still
 * rejected; JSON has no cycles. Nesting past
 * {@link UNDEFINED_PROPERTY_DEPTH} is left as is.
 *
 * With a `marker`, such a property keeps its key and gets the marker as its
 * value instead, so a closed-object check can still see it.
 */
function withoutUndefinedProperties(
  value: unknown,
  marker?: object,
  depth = 0,
  copies = new Map<object, unknown>(),
): unknown {
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
      const next = withoutUndefinedProperties(item, marker, depth + 1, copies);
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
    ([key, property]) => [key, withoutUndefinedProperties(property, marker, depth + 1, copies)] as const,
  );
  if (next.every(([, property], index) => property !== undefined && property === entries[index]![1])) {
    return value;
  }
  // defineProperty, not assignment: an own `__proto__` key stays a key.
  const copy = Object.create(prototype) as object;
  for (const [key, property] of next) {
    if (property !== undefined || marker) {
      Object.defineProperty(copy, key, {
        value: property ?? marker,
        enumerable: true,
        writable: true,
        configurable: true,
      });
    }
  }
  copies.set(value, copy);
  return copy;
}

/** The value an `undefined` property gets in the copy that closed-object checks see. */
const UNDEFINED_MARKER: object = Object.freeze({});

/**
 * The closed-object errors for properties whose value is `undefined`. The value
 * is validated a second time with each such property present, holding
 * {@link UNDEFINED_MARKER}; only `additionalProperties` and
 * `unevaluatedProperties` errors that name one of those properties are kept.
 * So, as with Ajv, `{ extra: undefined }` is refused where the schema forbids
 * `extra`, while a declared property set to `undefined` still counts as absent.
 * Model output can carry such a key: a structured-output schema wrapped to turn
 * `null` into `undefined` turns an undeclared `null` key into one.
 */
function undefinedKeysRefusedByClosedObjects(validator: Validator, value: unknown): OutputUnit[] {
  const marked = withoutUndefinedProperties(value, UNDEFINED_MARKER);
  const result = validator.validate(marked);
  if (result.valid) {
    return [];
  }
  return result.errors.filter(error => {
    if (!Object.hasOwn(CLOSED_OBJECT_MESSAGES, error.keyword)) {
      return false;
    }
    let holder: unknown = marked;
    for (const segment of instanceLocationToPath(error.instanceLocation)) {
      holder =
        typeof holder === 'object' && holder !== null ? (holder as Record<PropertyKey, unknown>)[segment] : undefined;
    }
    if (typeof holder !== 'object' || holder === null) {
      return false;
    }
    const message = CLOSED_OBJECT_MESSAGES[error.keyword]!;
    return Object.entries(holder).some(
      ([key, property]) => property === UNDEFINED_MARKER && error.error === message(key),
    );
  });
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

/** The validator's error for a property refused by `additionalProperties` or `unevaluatedProperties`. */
const CLOSED_OBJECT_MESSAGES: Record<string, (name: string) => string> = {
  additionalProperties: name => `Property "${name}" does not match additional properties schema.`,
  unevaluatedProperties: name => `Property "${name}" does not match unevaluated properties schema.`,
};

/**
 * Drops summary errors, and the `False boolean schema.` error that follows a
 * closed-object error for the same property: one extra property is reported once,
 * by the error that names it. A `false` error with no such parent is kept.
 */
function leafErrors(errors: OutputUnit[]): OutputUnit[] {
  const closedObjectErrors = new Set(
    errors
      .filter(error => error.keyword in CLOSED_OBJECT_MESSAGES)
      .map(error => `${error.instanceLocation}\n${error.error}`),
  );
  return errors.filter(error => {
    if (SUMMARY_KEYWORDS.has(error.keyword)) {
      return false;
    }
    if (error.keyword !== 'false' || closedObjectErrors.size === 0) {
      return true;
    }
    const slash = error.instanceLocation.lastIndexOf('/');
    const parent = error.instanceLocation.slice(0, slash);
    const name = String(instanceLocationToPath(error.instanceLocation).at(-1));
    return !Object.values(CLOSED_OBJECT_MESSAGES).some(message =>
      closedObjectErrors.has(`${parent}\n${message(name)}`),
    );
  });
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
      const validator = this.#getValidator();
      const jsonValue = withoutUndefinedProperties(value);
      const result = validator.validate(jsonValue);
      const errors = result.valid ? [] : leafErrors(result.errors);
      if (jsonValue !== value) {
        errors.push(...undefinedKeysRefusedByClosedObjects(validator, value));
      }

      if (errors.length === 0) {
        return { value: value as Output };
      }

      const issues: StandardSchemaV1.Issue[] = errors.map(error => ({
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
   * reused. Every error is collected, not only the first.
   *
   * Every schema, draft-07 included, is validated in the validator's 2020-12 mode.
   * In its draft-07 mode the validator skips keywords beside `$ref` (for example
   * `{ $ref, maxLength }`), which Ajv applied, so input Ajv refused would pass. The
   * validator reads the draft only for that and for draft-04, so draft-07 tuple
   * `items` and `additionalItems` behave the same in 2020-12 mode; `dependencies`
   * is rewritten as its 2020-12 forms first.
   */
  #getValidator(): Validator {
    if (!this.#validator) {
      if (!ValidatorClass) {
        throw new Error(`JSON Schema validator unavailable (${validatorUnavailableReason})`);
      }
      // Checked on the original, before the copy is normalised, so a `$dynamicRef`
      // anywhere in it is refused.
      refuseDynamicRef(this.#schema);
      const validationSchema = toValidationSchema(this.#schema);
      this.#validator = new ValidatorClass(validationSchema, '2020-12', false);
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
