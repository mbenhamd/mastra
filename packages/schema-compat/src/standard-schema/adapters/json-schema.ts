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
let dereferenceSchema: ((schema: Schema) => Lookup) | undefined;
let validatorUnavailableReason = 'still loading';
const validatorLoad: Promise<void> = import('@cfworker/json-schema').then(
  module => {
    ValidatorClass = module.Validator;
    dereferenceSchema = module.dereference;
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

/**
 * A schema the validator refuses when it is first needed: it uses a keyword outside the
 * allow-list, a listed keyword with a value of the wrong type, or a construct the validator
 * does not evaluate as fork main's Ajv did. The message names the keyword and its JSON
 * Pointer in the schema, never a schema value.
 */
export class JsonSchemaUnsupportedError extends Error {
  readonly keyword: string;
  readonly pointer: string;

  constructor(keyword: string, pointer: string, reason: string) {
    super(`${keyword} at "${pointer}" ${reason}, so the schema is refused`);
    this.name = 'JsonSchemaUnsupportedError';
    this.keyword = keyword;
    this.pointer = pointer;
  }
}

const SIMPLE_TYPES = new Set(['array', 'boolean', 'integer', 'null', 'number', 'object', 'string']);
const isSimpleType = (value: unknown): boolean => typeof value === 'string' && SIMPLE_TYPES.has(value);
const isFiniteNumber = (value: unknown): boolean => typeof value === 'number' && Number.isFinite(value);
const isNonNegativeInteger = (value: unknown): boolean => Number.isInteger(value) && (value as number) >= 0;
const isString = (value: unknown): boolean => typeof value === 'string';
const isAny = (): boolean => true;

const isUniqueStringList = (value: unknown): boolean =>
  Array.isArray(value) && value.every(isString) && new Set(value).size === value.length;

/** An absolute URI without a fragment, as a root `$id` names the schema. */
function isAbsoluteUriWithoutFragment(value: unknown): boolean {
  if (typeof value !== 'string' || value.includes('#')) {
    return false;
  }
  try {
    new URL(value);
    return true;
  } catch {
    return false;
  }
}

/**
 * How the allow-list walk reads a keyword's value: one schema, named schemas (`in-place` when
 * they apply to the same value), schemas that apply to the same value, schemas that each apply
 * to one array item, literal data, or nothing to walk.
 */
type Reading = 'schema' | 'schema-map' | 'schema-map-in-place' | 'schema-list' | 'schema-tuple' | 'data' | 'none';
type Allowed = { valid: (value: unknown) => boolean; reading: Reading };
type Dialect = 'draft-07' | '2020-12';

/**
 * The keywords a schema may use, each with the value type it must have (as in the draft-07
 * metaschema and Ajv's own checks) and how its value is read. Only keywords on which this
 * validator and fork main's draft-07 Ajv give the same answer are listed.
 */
const ALLOWED_KEYWORDS = new Map<string, Allowed>([
  [
    'type',
    {
      valid: v =>
        isSimpleType(v) || (Array.isArray(v) && v.length > 0 && v.every(isSimpleType) && new Set(v).size === v.length),
      reading: 'none',
    },
  ],
  ['properties', { valid: v => isJsonObject(v), reading: 'schema-map' }],
  ['$defs', { valid: v => isJsonObject(v), reading: 'schema-map' }],
  ['definitions', { valid: v => isJsonObject(v), reading: 'schema-map' }],
  ['required', { valid: isUniqueStringList, reading: 'none' }],
  ['items', { valid: isAny, reading: 'schema' }],
  ['additionalProperties', { valid: isAny, reading: 'schema' }],
  ['propertyNames', { valid: isAny, reading: 'schema' }],
  ['allOf', { valid: v => Array.isArray(v) && v.length > 0, reading: 'schema-list' }],
  ['anyOf', { valid: v => Array.isArray(v) && v.length > 0, reading: 'schema-list' }],
  ['oneOf', { valid: v => Array.isArray(v) && v.length > 0, reading: 'schema-list' }],
  ['enum', { valid: v => Array.isArray(v) && v.length > 0, reading: 'data' }],
  ['const', { valid: isAny, reading: 'data' }],
  ['minimum', { valid: isFiniteNumber, reading: 'none' }],
  ['maximum', { valid: isFiniteNumber, reading: 'none' }],
  ['exclusiveMinimum', { valid: isFiniteNumber, reading: 'none' }],
  ['exclusiveMaximum', { valid: isFiniteNumber, reading: 'none' }],
  // A decimal step is compared exactly here and through floating point by Ajv.
  ['multipleOf', { valid: v => Number.isInteger(v) && (v as number) > 0, reading: 'none' }],
  ['minLength', { valid: isNonNegativeInteger, reading: 'none' }],
  ['maxLength', { valid: isNonNegativeInteger, reading: 'none' }],
  ['minItems', { valid: isNonNegativeInteger, reading: 'none' }],
  ['maxItems', { valid: isNonNegativeInteger, reading: 'none' }],
  ['minProperties', { valid: isNonNegativeInteger, reading: 'none' }],
  ['maxProperties', { valid: isNonNegativeInteger, reading: 'none' }],
  ['pattern', { valid: v => typeof v === 'string' && compilesAsUnicodePattern(v), reading: 'none' }],
  ['uniqueItems', { valid: v => typeof v === 'boolean', reading: 'none' }],
  ['nullable', { valid: v => typeof v === 'boolean', reading: 'none' }],
  ['format', { valid: isString, reading: 'none' }],
  ['$ref', { valid: isString, reading: 'none' }],
  ['title', { valid: isString, reading: 'none' }],
  ['description', { valid: isString, reading: 'none' }],
  ['default', { valid: isAny, reading: 'data' }],
  ['examples', { valid: v => Array.isArray(v), reading: 'data' }],
  ['example', { valid: isAny, reading: 'data' }],
  ['discriminator', { valid: isAny, reading: 'data' }],
  // Composio's annotations.
  ['human_parameter_description', { valid: isAny, reading: 'data' }],
  ['human_parameter_name', { valid: isAny, reading: 'data' }],
  ['file_uploadable', { valid: isAny, reading: 'data' }],
  ['file_downloadable', { valid: isAny, reading: 'data' }],
]);

/**
 * The keywords a 2020-12 schema may use besides {@link ALLOWED_KEYWORDS}, listed under the
 * same rule against fork main's `Ajv2020`. A draft-07 schema may not use them. `$id` may only
 * name the root.
 */
const DRAFT_2020_12_KEYWORDS = new Map<string, Allowed>([
  ['prefixItems', { valid: v => Array.isArray(v) && v.length > 0, reading: 'schema-tuple' }],
  ['unevaluatedProperties', { valid: isAny, reading: 'schema' }],
  ['unevaluatedItems', { valid: isAny, reading: 'schema' }],
  ['contains', { valid: isAny, reading: 'schema' }],
  ['minContains', { valid: isNonNegativeInteger, reading: 'none' }],
  ['maxContains', { valid: isNonNegativeInteger, reading: 'none' }],
  ['dependentSchemas', { valid: v => isJsonObject(v), reading: 'schema-map-in-place' }],
  ['dependentRequired', { valid: v => isJsonObject(v) && Object.values(v).every(isUniqueStringList), reading: 'none' }],
  ['$id', { valid: isAbsoluteUriWithoutFragment, reading: 'none' }],
  ['$comment', { valid: isString, reading: 'none' }],
]);

/**
 * `$schema` values the root may declare, each with its dialect. A schema without `$schema` is
 * draft-07, as fork main's Ajv read it. Fork main validated a 2020-12 schema with `Ajv2020`.
 */
const DIALECTS = new Map<string, Dialect>([
  ['http://json-schema.org/draft-07/schema#', 'draft-07'],
  ['http://json-schema.org/draft-07/schema', 'draft-07'],
  ['https://json-schema.org/draft/2020-12/schema', '2020-12'],
  ['https://json-schema.org/draft/2020-12/schema#', '2020-12'],
]);

/** A `$ref` the allow-list accepts: the root, or a named member of the root's `$defs` or `definitions`. */
const ALLOWED_REF = /^#(?:\/(\$defs|definitions)\/([A-Za-z0-9_.-]+))?$/;

/** Names that Ajv's generated code and its equality treat differently from an ordinary key. */
const PROTOTYPE_NAMES = new Set(['__proto__', 'constructor', 'toString', 'valueOf']);

function compilesAsUnicodePattern(pattern: string): boolean {
  try {
    new RegExp(pattern, 'u');
    return true;
  } catch {
    return false;
  }
}

/** A plain object as `JSON.parse` makes it: no other prototype, so nothing inherited is read. */
function isJsonObject(value: unknown): value is Record<string, unknown> {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    return false;
  }
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

/**
 * Refuses, with a {@link JsonSchemaUnsupportedError}, a schema outside the allow-list:
 * - a schema position holding anything but a plain object or a boolean;
 * - a key that is neither in {@link ALLOWED_KEYWORDS} (nor, under a 2020-12 `$schema`, in
 *   {@link DRAFT_2020_12_KEYWORDS}) nor an `x-` extension, or a listed keyword whose value has
 *   the wrong type, `$schema` other than draft-07 or 2020-12 or below the root, `$id` below the
 *   root, or `nullable` without `type` (Ajv refused those when compiling);
 * - a `$ref` other than `#` or a member of the root's `$defs` or `definitions`;
 * - a property or definition named `__proto__`, `constructor`, `toString` or `valueOf`, and
 *   such a key in `const` or `enum` data;
 * - a loop of `$ref`, `allOf`, `anyOf`, `oneOf` or `dependentSchemas` that comes back to the
 *   same schema at the same value.
 * Literal data under `const`, `enum`, `default`, `examples` and annotations is not read as a
 * schema. The schema is checked as given, before it is copied, so an inherited keyword counts.
 * Returns whether the schema compares values (`const`, `enum` or `uniqueItems`).
 */
function refuseUnlistedSchema(root: unknown): { comparesValues: boolean } {
  const declared = isJsonObject(root) ? root.$schema : undefined;
  const dialect: Dialect = (typeof declared === 'string' && DIALECTS.get(declared)) || 'draft-07';
  let comparing = false;
  const inPlaceEdges = new Map<object, Array<[object, string]>>();
  const visit = (node: unknown, pointer: string): void => {
    if (typeof node === 'boolean') {
      return;
    }
    if (!isJsonObject(node)) {
      throw new JsonSchemaUnsupportedError('schema', pointer, 'is not a JSON object or boolean');
    }
    if (inPlaceEdges.has(node)) {
      return;
    }
    const edges: Array<[object, string]> = [];
    inPlaceEdges.set(node, edges);
    for (const key of Object.keys(node)) {
      const value = node[key];
      const at = `${pointer}/${pointerToken(key)}`;
      if (key === '$schema') {
        if (pointer !== '' || !DIALECTS.has(value as string)) {
          throw new JsonSchemaUnsupportedError(key, at, 'is not a draft-07 or 2020-12 $schema at the root');
        }
        continue;
      }
      if (key.startsWith('x-')) {
        continue;
      }
      const allowed =
        ALLOWED_KEYWORDS.get(key) ?? (dialect === '2020-12' ? DRAFT_2020_12_KEYWORDS.get(key) : undefined);
      if (!allowed) {
        throw new JsonSchemaUnsupportedError(
          key,
          at,
          DRAFT_2020_12_KEYWORDS.has(key) ? 'is only supported under a 2020-12 $schema' : 'is not a supported keyword',
        );
      }
      if (key === '$id' && pointer !== '') {
        throw new JsonSchemaUnsupportedError(key, at, 'is only supported at the root');
      }
      if (
        key === 'dependentRequired' &&
        Object.entries(value as Record<string, string[]>).some(
          ([name, names]) => PROTOTYPE_NAMES.has(name) || names.some(other => PROTOTYPE_NAMES.has(other)),
        )
      ) {
        throw new JsonSchemaUnsupportedError(key, at, 'names a property like an Object prototype key');
      }
      if (!allowed.valid(value)) {
        throw new JsonSchemaUnsupportedError(key, at, 'has a value of the wrong type');
      }
      if (key === 'uniqueItems') {
        comparing = true;
      }
      switch (allowed.reading) {
        case 'schema':
          visit(value, at);
          break;
        case 'schema-map':
        case 'schema-map-in-place':
          for (const name of Object.keys(value as Record<string, unknown>)) {
            const member = (value as Record<string, unknown>)[name];
            if (PROTOTYPE_NAMES.has(name)) {
              throw new JsonSchemaUnsupportedError(
                key,
                `${at}/${pointerToken(name)}`,
                'names a member like an Object prototype key',
              );
            }
            visit(member, `${at}/${pointerToken(name)}`);
            if (allowed.reading === 'schema-map-in-place' && typeof member === 'object' && member !== null) {
              edges.push([member, `${at}/${pointerToken(name)}`]);
            }
          }
          break;
        case 'schema-tuple':
          (value as unknown[]).forEach((item, index) => visit(item, `${at}/${index}`));
          break;
        case 'schema-list':
          (value as unknown[]).forEach((branch, index) => {
            visit(branch, `${at}/${index}`);
            if (typeof branch === 'object' && branch !== null) {
              edges.push([branch, `${at}/${index}`]);
            }
          });
          break;
        case 'data':
          if (key === 'const' || key === 'enum') {
            comparing = true;
          }
          if ((key === 'const' || key === 'enum') && holdsPrototypeName(value)) {
            throw new JsonSchemaUnsupportedError(key, at, 'holds an object key like an Object prototype key');
          }
          break;
        case 'none':
          break;
      }
    }
    if (node.nullable !== undefined && node.type === undefined) {
      throw new JsonSchemaUnsupportedError('nullable', `${pointer}/nullable`, 'is used without "type"');
    }
    if (node.nullable === false && (node.type === 'null' || (Array.isArray(node.type) && node.type.includes('null')))) {
      throw new JsonSchemaUnsupportedError('nullable', `${pointer}/nullable`, 'is false beside a "null" type');
    }
    if (Array.isArray(node.required) && node.required.some(name => PROTOTYPE_NAMES.has(name as string))) {
      throw new JsonSchemaUnsupportedError(
        'required',
        `${pointer}/required`,
        'names a property like an Object prototype key',
      );
    }
    if (node.$ref !== undefined) {
      const target = namedReferenceTarget(root, node.$ref as string);
      if (target === undefined) {
        throw new JsonSchemaUnsupportedError(
          '$ref',
          `${pointer}/$ref`,
          'is not "#" or a member of the root\'s "$defs" or "definitions"',
        );
      }
      if (typeof target === 'object' && target !== null) {
        edges.push([target, `${pointer}/$ref`]);
      }
    }
  };
  visit(root, '');
  // A loop through `$ref`, `allOf`, `anyOf` and `oneOf` reads the same schema at the same
  // value again; Ajv overflowed or recursed on it, and so does this validator.
  const state = new Map<object, 'open' | 'done'>();
  const walk = (node: object): void => {
    state.set(node, 'open');
    for (const [next, at] of inPlaceEdges.get(node) ?? []) {
      const seen = state.get(next);
      if (seen === 'open') {
        throw new JsonSchemaUnsupportedError(
          at.endsWith('/$ref') ? '$ref' : at.split('/').at(-2)!,
          at,
          'comes back to the same schema at the same value',
        );
      }
      if (seen === undefined) {
        walk(next);
      }
    }
    state.set(node, 'done');
  };
  for (const node of inPlaceEdges.keys()) {
    if (!state.has(node)) {
      walk(node);
    }
  }
  return { comparesValues: comparing };
}

/** Whether literal data holds an object with a key that Ajv's equality reads from the prototype. */
function holdsPrototypeName(value: unknown): boolean {
  if (typeof value !== 'object' || value === null) {
    return false;
  }
  if (!Array.isArray(value) && Object.keys(value).some(key => PROTOTYPE_NAMES.has(key))) {
    return true;
  }
  return Object.values(value).some(holdsPrototypeName);
}

/**
 * The path of the first place in `value` that JSON cannot carry, or `undefined`: a number that
 * is not finite, an object with a prototype other than `Object.prototype` (a `Date`, a class
 * instance, an object with inherited properties), a sparse or cyclic array or object, a
 * function, a `bigint` or a symbol. `undefined` property values are allowed and handled as Ajv
 * handled them. When the schema compares values, an object key that Ajv's equality reads from
 * the prototype (`constructor`, `toString`, `valueOf`, `__proto__`) is refused too.
 */
function nonJsonLocation(
  value: unknown,
  comparing: boolean,
  path: Array<string | number> = [],
  open = new Set<object>(),
): Array<string | number> | undefined {
  switch (typeof value) {
    case 'string':
    case 'boolean':
    case 'undefined':
      return undefined;
    case 'number':
      return Number.isFinite(value) ? undefined : path;
    case 'object':
      break;
    default:
      return path;
  }
  if (value === null) {
    return undefined;
  }
  if (open.has(value) || (!Array.isArray(value) && !isJsonObject(value))) {
    return path;
  }
  open.add(value);
  let found: Array<string | number> | undefined;
  if (Array.isArray(value)) {
    for (let index = 0; index < value.length && found === undefined; index++) {
      found = index in value ? nonJsonLocation(value[index], comparing, [...path, index], open) : [...path, index];
    }
  } else {
    for (const key of Object.keys(value)) {
      found =
        comparing && PROTOTYPE_NAMES.has(key)
          ? [...path, key]
          : nonJsonLocation(value[key], comparing, [...path, key], open);
      if (found !== undefined) {
        break;
      }
    }
  }
  open.delete(value);
  return found;
}

function pointerToken(key: string): string {
  return key.replace(/~/g, '~0').replace(/\//g, '~1');
}

type Lookup = Record<string, unknown>;

/** Calls `visit` on every object the allow-list reads as a schema, with its JSON Pointer. */
function forEachSchemaObject(root: unknown, visit: (node: Record<string, unknown>, pointer: string) => void): void {
  const seen = new Set<object>();
  const walk = (node: unknown, pointer: string): void => {
    if (!isJsonObject(node) || seen.has(node)) {
      return;
    }
    seen.add(node);
    visit(node, pointer);
    for (const [key, value] of Object.entries(node)) {
      const at = `${pointer}/${pointerToken(key)}`;
      switch ((ALLOWED_KEYWORDS.get(key) ?? DRAFT_2020_12_KEYWORDS.get(key))?.reading) {
        case 'schema':
          walk(value, at);
          break;
        case 'schema-map':
        case 'schema-map-in-place':
          for (const [name, member] of Object.entries(value as Record<string, unknown>)) {
            walk(member, `${at}/${pointerToken(name)}`);
          }
          break;
        case 'schema-list':
        case 'schema-tuple':
          (value as unknown[]).forEach((branch, index) => walk(branch, `${at}/${index}`));
          break;
        default:
          break;
      }
    }
  };
  walk(root, '');
}

/** The schema a `$ref` the allow-list accepted names: the root, or a member of its `$defs` or `definitions`. */
function namedReferenceTarget(root: unknown, ref: string): unknown {
  const match = ALLOWED_REF.exec(ref);
  if (!match) {
    return undefined;
  }
  if (match[1] === undefined) {
    return root;
  }
  const members = isJsonObject(root) ? root[match[1]] : undefined;
  return isJsonObject(members) && Object.hasOwn(members, match[2]!) ? members[match[2]!] : undefined;
}

/**
 * Copies a JSON Schema the allow-list accepted into the form the validator checks. OpenAPI
 * `nullable: true` adds `"null"` to the `type` beside it, as Ajv's built-in `nullable` keyword
 * did; `enum` and `const` still apply. The copy is then checked so that the validator resolves
 * every `$ref` to the schema its JSON Pointer names (an annotation the validator walks could
 * otherwise declare an `$id` that takes that place).
 */
function toValidationSchema(
  schema: JSONSchema7,
  dereference: (schema: Schema) => Lookup,
): { schema: Schema; comparesValues: boolean } {
  const { comparesValues } = refuseUnlistedSchema(schema);
  const copy = structuredClone(schema) as Schema;
  forEachSchemaObject(copy, node => {
    if (node.nullable === true) {
      const types = Array.isArray(node.type) ? node.type : [node.type];
      if (!types.includes('null')) {
        node.type = [...types, 'null'];
      }
    }
  });
  const lookup = dereference(copy);
  forEachSchemaObject(copy, (node, pointer) => {
    if (typeof node.$ref === 'string') {
      const resolved = typeof node.__absolute_ref__ === 'string' ? lookup[node.__absolute_ref__] : undefined;
      if (!Object.is(resolved, namedReferenceTarget(copy, node.$ref))) {
        throw new JsonSchemaUnsupportedError(
          '$ref',
          `${pointer}/$ref`,
          'resolves to another schema than its JSON Pointer names',
        );
      }
    }
  });
  return { schema: copy, comparesValues };
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
  #comparesValues = false;

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
      const validator = this.#getValidator();
      // A value JSON cannot carry is refused before it is checked (see `nonJsonLocation`).
      const nonJson = nonJsonLocation(value, this.#comparesValues);
      if (nonJson !== undefined) {
        return {
          issues: [{ message: 'The value holds something JSON cannot carry, so it is refused.', path: nonJson }],
        };
      }
      const result = validator.validate(value);
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
      if (!ValidatorClass || !dereferenceSchema) {
        throw new Error(`JSON Schema validator unavailable (${validatorUnavailableReason})`);
      }
      const { schema: validationSchema, comparesValues } = toValidationSchema(this.#schema, dereferenceSchema);
      this.#comparesValues = comparesValues;
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
