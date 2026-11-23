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
const DATA_KEYWORDS = new Set<string>(['const', 'default', 'enum', 'examples']);

/**
 * Thrown for a schema that uses `$dynamicRef`. `@cfworker/json-schema` does not
 * resolve it, so the reference would add no constraint and values Ajv2020
 * rejected would pass. Validation refuses such a schema instead.
 */
const UNSUPPORTED_DYNAMIC_REF_MESSAGE =
  '$dynamicRef is not supported: this validator cannot resolve it, so the schema is refused rather than checked loosely';

const SCHEMA_MAP_KEYWORD_SET = new Set<string>(SCHEMA_MAP_KEYWORDS);

/** Keywords whose value is one subschema. */
const SUBSCHEMA_KEYWORDS = new Set<string>([
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
]);

/** Keywords whose value is an array of subschemas (`items` in draft-07 too). */
const SUBSCHEMA_ARRAY_KEYWORDS = new Set<string>(['allOf', 'anyOf', 'items', 'oneOf', 'prefixItems']);

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
 * Applies `nullable`, rewrites a boolean `exclusiveMinimum`/`exclusiveMaximum`,
 * and drops `format` and `id` on one schema object.
 */
function normalizeSchemaObject(record: Record<string, unknown>): void {
  const types: unknown[] = Array.isArray(record.type) ? record.type : [record.type];
  if (record.nullable === true && record.type !== undefined && types.every(type => typeof type === 'string')) {
    if (!types.includes('null')) {
      record.type = [...types, 'null'];
    }
  }
  // OpenAPI 3.0 and draft-04 write an exclusive bound as `minimum: 5, exclusiveMinimum: true`.
  // The validator reads `exclusiveMinimum` as a number only, and compared values with `true`
  // as if it were 1, so 5 passed. The bound is moved into the keyword, as in later drafts.
  for (const [bound, exclusive] of [
    ['minimum', 'exclusiveMinimum'],
    ['maximum', 'exclusiveMaximum'],
  ] as const) {
    if (typeof record[exclusive] === 'boolean') {
      if (record[exclusive] === true && typeof record[bound] === 'number') {
        record[exclusive] = record[bound];
        delete record[bound];
      } else {
        delete record[exclusive];
      }
    }
  }
  // The validator never checks `format` (see the cfworker patch); it is also dropped here.
  if (typeof record.format === 'string') {
    delete record.format;
  }
  // The validator reads `id` on every object it walks as a schema URI, so two
  // objects that are not schemas, such as two `example: { id: 1 }` or
  // `x-meta: { id: 'u' }`, made every call fail with a duplicate schema URI.
  // Only draft-04 used `id`, and Ajv ignored it; `$id` is unaffected.
  delete record.id;
}

const REF_ROLE_CONFLICT_MESSAGE =
  'a $ref target that the schema also uses as a name map or as literal data holds a boolean exclusiveMinimum or exclusiveMaximum, which cannot be read both ways, so the schema is refused';

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/**
 * What an object is to the validator. `schema`, `names` (a name map's own keys) and `data`
 * (literal data under `const`, `enum`, `default` or `examples`) come from reading the schema
 * from its root and from every local `$ref` (`#/...`) at a schema position. The `weak` roles
 * cover the rest, such as `components` or `x-meta`, which the validator reads only through a
 * reference this walk does not follow (an absolute URI, for example): there an object is
 * normalised as a schema unless it sits under a name-map or data keyword.
 */
type Role = 'schema' | 'names' | 'data' | 'weak' | 'weak-names' | 'weak-data';

/** Returns the role of every object, reading the schema from its root and its local `$ref`s. */
function assignRoles(root: unknown): Map<object, Set<Role>> {
  const roles = new Map<object, Set<Role>>();
  const pending: unknown[] = [];
  const resolved = new Set<string>();
  const mark = (node: object, role: Role): boolean => {
    let set = roles.get(node);
    if (!set) {
      set = new Set();
      roles.set(node, set);
    }
    if (set.has(role)) {
      return false;
    }
    set.add(role);
    return true;
  };
  const walk = (node: unknown, role: Role): void => {
    if (typeof node !== 'object' || node === null) {
      return;
    }
    if (Array.isArray(node)) {
      // Only an applicator's array holds schemas; any other array is data or unknown.
      if (mark(node, role)) {
        const itemRole = role === 'schema' ? 'schema' : role === 'data' || role === 'weak-data' ? role : 'weak';
        node.forEach(item => walk(item, itemRole));
      }
      return;
    }
    if (!mark(node, role)) {
      return;
    }
    const record = node as Record<string, unknown>;
    if (role === 'data' || role === 'weak-data') {
      Object.values(record).forEach(value => walk(value, role));
      return;
    }
    if (role === 'names' || role === 'weak-names') {
      Object.values(record).forEach(value => walk(value, role === 'names' && isPlainObject(value) ? 'schema' : 'weak'));
      return;
    }
    for (const [key, value] of Object.entries(record)) {
      if (DATA_KEYWORDS.has(key)) {
        walk(value, role === 'schema' ? 'data' : 'weak-data');
      } else if (SCHEMA_MAP_KEYWORD_SET.has(key)) {
        walk(value, role === 'schema' ? 'names' : 'weak-names');
      } else if (
        role === 'schema' &&
        (SUBSCHEMA_KEYWORDS.has(key) || (SUBSCHEMA_ARRAY_KEYWORDS.has(key) && Array.isArray(value)))
      ) {
        walk(value, 'schema');
      } else {
        walk(value, 'weak');
      }
    }
    const ref = role === 'schema' ? record.$ref : undefined;
    if (typeof ref === 'string' && ref.startsWith('#/') && !resolved.has(ref)) {
      resolved.add(ref);
      pending.push(resolvePointer(root, ref));
    }
  };
  walk(root, 'schema');
  // Each reference is resolved once, so the walk stays linear in the size of the schema.
  while (pending.length > 0) {
    walk(pending.pop(), 'schema');
  }
  return roles;
}

/** Follows a local JSON Pointer (`#/a/b`); `undefined` when it does not resolve. */
function resolvePointer(root: unknown, ref: string): unknown {
  let target: unknown = root;
  for (const token of ref.slice(2).split('/')) {
    let name: string;
    try {
      name = decodeURIComponent(token).replace(/~1/g, '/').replace(/~0/g, '~');
    } catch {
      return undefined;
    }
    if (typeof target !== 'object' || target === null || !Object.hasOwn(target, name)) {
      return undefined;
    }
    target = (target as Record<string, unknown>)[name];
  }
  return target;
}

/**
 * Applies `nullable`, rewrites boolean exclusive bounds, and drops `format` and `id` on every
 * object the validator can read as a schema, leaving name maps and literal data alone. An
 * object the schema reads both as a schema (through a `$ref`) and as a name map or literal
 * data, such as the target of `{ $ref: '#/properties' }`, is left as written for the name map
 * or the data. Left as written, `nullable`, `format` and `id` can only refuse more values, but
 * a boolean `exclusiveMinimum` or `exclusiveMaximum` would be compared as the number 1 or 0
 * and let values pass, so such a schema is refused.
 */
function normalizeSchemaObjects(root: unknown): void {
  for (const [node, set] of assignRoles(root)) {
    if (Array.isArray(node)) {
      continue;
    }
    const record = node as Record<string, unknown>;
    const strong = set.has('schema') || set.has('names') || set.has('data');
    if (strong ? set.has('names') || set.has('data') : set.has('weak-names') || set.has('weak-data')) {
      if (
        set.has('schema') &&
        (typeof record.exclusiveMinimum === 'boolean' || typeof record.exclusiveMaximum === 'boolean')
      ) {
        throw new Error(REF_ROLE_CONFLICT_MESSAGE);
      }
      continue;
    }
    normalizeSchemaObject(record);
  }
}

/**
 * Copies a JSON Schema into the form the validator checks:
 * - OpenAPI `nullable: true` beside a `type` adds `"null"` to that type, as Ajv's
 *   built-in `nullable` keyword does; `enum` and `const` still apply.
 * - `format` is dropped, because validation has never checked formats here; the
 *   patched validator does not check it either, also on a target this walk misses.
 * - A boolean `exclusiveMinimum`/`exclusiveMaximum` (OpenAPI 3.0, draft-04)
 *   takes the value of `minimum`/`maximum` when it is `true` beside a number,
 *   and is dropped otherwise.
 * - `id` is dropped: only draft-04 used it, Ajv ignored it, and the validator
 *   read it on objects that are not schemas.
 * Name maps and literal data are left alone, so a property named `nullable`
 * or `format` keeps its schema, and a `$ref` target is read like any other
 * subschema (see `normalizeSchemaObjects`).
 */
function toValidationSchema(schema: JSONSchema7): Schema {
  const copy = structuredClone(schema) as Schema;
  normalizeSchemaObjects(copy);
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
    // A draft-07 schema dependency is summarised like `dependentSchemas`; its
    // property-list form names the missing property and is kept.
    if (error.keyword === 'dependencies' && error.error.endsWith('but does not match dependant schema.')) {
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
      // A property whose value is `undefined` is handled as Ajv did, by the
      // patched validator: absent for `properties`, `required` and the
      // dependency keywords, present for `additionalProperties`,
      // `unevaluatedProperties`, `patternProperties`, `propertyNames` and the
      // property counts. Model output can hold one: a structured-output schema
      // wrapped to turn `null` into `undefined` turns an undeclared `null` key
      // into one.
      const result = this.#getValidator().validate(value);
      const leaves = result.valid ? [] : leafErrors(result.errors);
      // An invalid value is never returned as valid, even if every error was a summary.
      const errors = !result.valid && leaves.length === 0 ? result.errors : leaves;

      if (result.valid) {
        return { value: value as Output };
      }

      const issues: StandardSchemaV1.Issue[] = errors.map(error => ({
        message: error.error,
        path: instanceLocationToPath(error.instanceLocation),
      }));

      return { issues: issues.length > 0 ? issues : [{ message: 'Instance does not match the schema.' }] };
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
   * `items`, `additionalItems` and `dependencies` behave the same in 2020-12 mode.
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
