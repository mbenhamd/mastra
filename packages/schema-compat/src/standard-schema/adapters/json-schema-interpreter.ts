import type { OutputUnit, Schema, Validator } from '@cfworker/json-schema';
import type { StandardSchemaV1 } from '@standard-schema/spec';

/**
 * The JSON Schema validator used where code generation from strings is disallowed (for
 * example under `--disallow-code-generation-from-strings`), where fork main's Ajv cannot
 * compile. Everywhere else the adapter validates with Ajv exactly as before; see
 * `json-schema.ts`.
 *
 * In this path:
 * - one walk copies the schema into a plain-JSON snapshot (see {@link plainJsonSnapshot}), and
 *   anything that is not plain JSON is refused with a named reason;
 * - the allow-list (see {@link refuseUnlistedSchema}) reads only that snapshot, and refuses every
 *   schema outside it; there is no second validator;
 * - `@cfworker/json-schema`, which interprets the schema and never generates code, validates a
 *   plain-JSON snapshot of each value against the snapshot of the schema.
 *
 * Over plain-JSON schemas and values, this path accepts nothing that fork main's Ajv, run where
 * it can compile, refuses. `json-schema-interpreter-parity.test.ts` compares the two.
 */

/**
 * `@cfworker/json-schema` builds a URL from `self.location` while it is imported, which throws in
 * a browser worker created from a blob URL. It is imported lazily, and only where this path is
 * used, so importing this package never fails there. Until that import has finished, and if it
 * fails, validation fails closed with a "JSON Schema validator unavailable" issue; it never
 * reports success. Callers that can await should await {@link jsonSchemaValidatorReady} first
 * while {@link jsonSchemaValidatorLoading} is true.
 */
let ValidatorClass: typeof Validator | undefined;
let dereferenceSchema: ((schema: Schema) => Lookup) | undefined;
let validatorUnavailableReason = 'still loading';
let validatorLoad: Promise<void> | undefined;

/**
 * Every copy of this adapter in the process registers its load here until it settles: a package can
 * bundle its own copy (`@mastra/mcp` does), or an install can hold two versions. Awaiting any copy's
 * {@link jsonSchemaValidatorReady} then also waits for the copy that built the schema being validated.
 */
const VALIDATOR_LOADS: unique symbol = Symbol.for('mastra.schemaCompat.jsonSchemaValidatorLoads');
const validatorLoads: Set<Promise<void>> = ((globalThis as { [VALIDATOR_LOADS]?: Set<Promise<void>> })[
  VALIDATOR_LOADS
] ??= new Set());

/** Starts loading the interpreter, once per copy of this module. */
export function startInterpreterLoad(): Promise<void> {
  if (validatorLoad === undefined) {
    const load: Promise<void> = import('@cfworker/json-schema').then(
      module => {
        ValidatorClass = module.Validator;
        dereferenceSchema = module.dereference;
      },
      (error: unknown) => {
        validatorUnavailableReason = error instanceof Error ? error.message : String(error);
      },
    );
    validatorLoad = load;
    validatorLoads.add(load);
    void load.then(() => {
      validatorLoads.delete(load);
    });
  }
  return validatorLoad;
}

/**
 * Whether any JSON Schema validator copy in the process is still loading. Once it is false, validation
 * needs no wait, so a caller can skip awaiting {@link jsonSchemaValidatorReady} and keep its timing.
 * Where code generation is allowed nothing loads, so it is always false.
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

// `filter` skips holes, so a sparse list fails as fork main's metaschema failed it.
const isUniqueStringList = (value: unknown): boolean =>
  Array.isArray(value) && value.filter(isString).length === value.length && new Set(value).size === value.length;

/** Whether an array has a hole or an `undefined` member, which fork main's Ajv refused or threw on. */
function hasHoleOrUndefined(list: unknown[]): boolean {
  for (let index = 0; index < list.length; index++) {
    if (!(index in list) || list[index] === undefined) {
      return true;
    }
  }
  return false;
}

/**
 * The only form of root `$id` this path accepts: `http` or `https`, a lower-case host of
 * letters, digits, dots and hyphens, an optional port, and path segments of unreserved
 * characters (letters, digits, `.`, `_`, `~`, `-`), with no query, fragment, percent-encoding,
 * user info, space or control character. Fork main's Ajv resolved a root `$id` with fast-uri,
 * which threw on some IDs the WHATWG URL parser accepts (leading or trailing spaces, controls,
 * malformed percent-encoding, a backslash); every ID of this form parses the same way in both,
 * which `json-schema-interpreter-parity.test.ts` checks against Ajv. Any other root `$id` is
 * refused in this path.
 */
const CANONICAL_ROOT_ID =
  /^https?:\/\/[a-z0-9](?:[a-z0-9-]*[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]*[a-z0-9])?)*(?::[1-9][0-9]{0,4})?(?:\/[A-Za-z0-9._~-]+)*\/?$/;

/**
 * Whether `value` is a root `$id` in canonical form: it matches {@link CANONICAL_ROOT_ID}, the URL
 * parser gives it back unchanged (so no default port, empty path or dot segment is normalised
 * away), and it is not a schema ID fork main's `Ajv2020` had already registered.
 */
export function isCanonicalRootId(value: unknown): boolean {
  if (typeof value !== 'string' || value.length > 2048 || !CANONICAL_ROOT_ID.test(value)) {
    return false;
  }
  let href: string;
  try {
    href = new URL(value).href;
  } catch {
    return false;
  }
  return href === value && !AJV_2020_REGISTERED_IDS.has(value);
}

/**
 * How the allow-list walk reads a keyword's value: one schema, named schemas, schemas that
 * apply to the same value, schemas that each apply to one array item, literal data, or nothing
 * to walk.
 */
type Reading = 'schema' | 'schema-map' | 'schema-list' | 'schema-tuple' | 'data' | 'none';
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
  ['items', { valid: v => !Array.isArray(v) || v.length > 0, reading: 'schema' }],
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
  ['minLength', { valid: isNonNegativeInteger, reading: 'none' }],
  ['maxLength', { valid: isNonNegativeInteger, reading: 'none' }],
  ['minItems', { valid: isNonNegativeInteger, reading: 'none' }],
  ['maxItems', { valid: isNonNegativeInteger, reading: 'none' }],
  ['minProperties', { valid: isNonNegativeInteger, reading: 'none' }],
  ['maxProperties', { valid: isNonNegativeInteger, reading: 'none' }],
  ['pattern', { valid: v => typeof v === 'string' && compilesAsUnicodePattern(v), reading: 'none' }],
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
 * The keywords a draft-07 schema may use besides {@link ALLOWED_KEYWORDS}: `additionalItems`
 * applies after a tuple `items` (Ajv and this validator both ignore it beside one `items`
 * schema). Ajv2020 had no such keyword.
 */
const DRAFT_07_KEYWORDS = new Map<string, Allowed>([['additionalItems', { valid: isAny, reading: 'schema' }]]);

/**
 * The keywords a 2020-12 schema may use besides {@link ALLOWED_KEYWORDS}, listed under the
 * same rule against fork main's `Ajv2020`. A draft-07 schema may not use them. `$id` may only
 * name the root.
 */
const DRAFT_2020_12_KEYWORDS = new Map<string, Allowed>([
  ['prefixItems', { valid: v => Array.isArray(v) && v.length > 0, reading: 'schema-tuple' }],
  ['dependentRequired', { valid: v => isJsonObject(v) && Object.values(v).every(isUniqueStringList), reading: 'none' }],
  ['$id', { valid: isCanonicalRootId, reading: 'none' }],
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

/** Names that Ajv's equality reads from an object's prototype and the validator does not. */
const EQUALITY_NAMES = new Set(['__proto__', 'constructor', 'toString', 'valueOf']);

/**
 * Every name a plain object inherits. Ajv's generated code read `data[name]` for a listed
 * property or required name, so it saw the inherited member where the validator sees none.
 */
const PROTOTYPE_NAMES = new Set(Object.getOwnPropertyNames(Object.prototype));

/**
 * The schema IDs fork main's `Ajv2020` had registered (its 2020-12 metaschemas and the
 * `http://json-schema.org/schema` alias); a root `$id` naming one made it throw when adding the
 * schema. Ajv compared the ID only after removing a trailing `#` or `#/`.
 */
const AJV_2020_REGISTERED_IDS = new Set([
  'https://json-schema.org/draft/2020-12/schema',
  'https://json-schema.org/draft/2020-12/meta/core',
  'https://json-schema.org/draft/2020-12/meta/applicator',
  'https://json-schema.org/draft/2020-12/meta/unevaluated',
  'https://json-schema.org/draft/2020-12/meta/validation',
  'https://json-schema.org/draft/2020-12/meta/meta-data',
  'https://json-schema.org/draft/2020-12/meta/format-annotation',
  'https://json-schema.org/draft/2020-12/meta/content',
  'http://json-schema.org/schema',
]);

/** Ajv's `loopEnum` default: a shorter `enum` was compiled member by member, and threw on `undefined`. */
const AJV_LOOP_ENUM = 200;

/** Keywords whose values Ajv's reference walk (json-schema-traverse with `allKeys`) did not enter. */
const AJV_TRAVERSE_SKIPPED = new Set([
  'default',
  'enum',
  'const',
  'required',
  'maximum',
  'minimum',
  'exclusiveMaximum',
  'exclusiveMinimum',
  'multipleOf',
  'maxLength',
  'minLength',
  'pattern',
  'format',
  'maxItems',
  'minItems',
  'uniqueItems',
  'maxProperties',
  'minProperties',
]);
const AJV_TRAVERSE_ARRAYS = new Set(['items', 'allOf', 'anyOf', 'oneOf']);
const AJV_TRAVERSE_MAPS = new Set(['$defs', 'definitions', 'properties', 'patternProperties', 'dependencies']);

function compilesAsUnicodePattern(pattern: string): boolean {
  try {
    new RegExp(pattern, 'u');
    return true;
  } catch {
    return false;
  }
}

/** A plain object as `JSON.parse` makes it, as every object in a snapshot is. */
function isJsonObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/**
 * Refuses, with a {@link JsonSchemaUnsupportedError}, a schema outside the allow-list:
 * - a schema position holding anything but a plain object or a boolean;
 * - a key that is neither in {@link ALLOWED_KEYWORDS} (nor, under a 2020-12 `$schema`, in
 *   {@link DRAFT_2020_12_KEYWORDS}) nor an `x-` extension, or a listed keyword whose value has
 *   the wrong type, `$schema` other than draft-07 or 2020-12 or below the root, `$id` below the
 *   root, a root `$id` Ajv had registered, a repeated draft-07 `enum` member, or `nullable`
 *   without `type` (Ajv refused those when compiling);
 * - a `$ref` other than `#` or a member of the root's `$defs` or `definitions`;
 * - a property, required or `dependentRequired` name that a plain object inherits, a
 *   definition named `__proto__`, `constructor`, `toString` or `valueOf`, and such a key in
 *   `const` or `enum` data;
 * - a `$defs` member named `$id` that Ajv read as a reference scope, and a string `$id`,
 *   `$anchor` or `$dynamicAnchor` below the root in any object Ajv's reference walk entered
 *   (see {@link refuseNestedIdsAndAnchors});
 * - a loop of `$ref`, `allOf`, `anyOf` or `oneOf` that comes back to the same schema at the
 *   same value.
 * Literal data under `const`, `enum`, `default`, `examples` and annotations is not read as a
 * schema. The schema checked is the plain-JSON snapshot, so every keyword is an own data key.
 * Returns whether the schema compares values (`const` or `enum`).
 */
function refuseUnlistedSchema(root: unknown): { comparesValues: boolean } {
  const declared = isJsonObject(root) ? root.$schema : undefined;
  const dialect: Dialect = (typeof declared === 'string' && DIALECTS.get(declared)) || 'draft-07';
  let comparing = false;
  const inPlaceEdges = new Map<object, Array<[object, string]>>();
  const children = new Map<object, object[]>();
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
    const kids: object[] = [];
    children.set(node, kids);
    // A schema Ajv compiled reached this child; a member of `$defs` or `definitions` is reached
    // only through a `$ref`.
    const enter = (child: unknown, at: string, reached = true): void => {
      visit(child, at);
      if (reached && typeof child === 'object' && child !== null) {
        kids.push(child);
      }
    };
    for (const key of Object.keys(node)) {
      const value = node[key];
      const at = `${pointer}/${pointerToken(key)}`;
      if (value === undefined) {
        // Ajv applied no keyword whose value is `undefined`. The snapshot is this path's own
        // copy, so the keyword is removed and the validator never sees it.
        delete node[key];
        continue;
      }
      if (key === '$schema') {
        // Fork main's Ajv read the dialect from the root alone and ignored a nested string
        // `$schema` (a workflow's converted input schema carries one).
        if (pointer === '' ? !DIALECTS.has(value as string) : typeof value !== 'string') {
          throw new JsonSchemaUnsupportedError(key, at, 'is not a draft-07 or 2020-12 $schema at the root');
        }
        continue;
      }
      if (key.startsWith('x-')) {
        continue;
      }
      const allowed =
        ALLOWED_KEYWORDS.get(key) ??
        (dialect === '2020-12' ? DRAFT_2020_12_KEYWORDS.get(key) : DRAFT_07_KEYWORDS.get(key));
      if (!allowed) {
        throw new JsonSchemaUnsupportedError(
          key,
          at,
          DRAFT_2020_12_KEYWORDS.has(key)
            ? 'is only supported under a 2020-12 $schema'
            : DRAFT_07_KEYWORDS.has(key)
              ? 'is only supported under a draft-07 $schema'
              : 'is not a supported keyword',
        );
      }
      if (key === '$id' && pointer !== '') {
        throw new JsonSchemaUnsupportedError(key, at, 'is only supported at the root');
      }
      if (key === '$id' && !isCanonicalRootId(value)) {
        throw new JsonSchemaUnsupportedError(key, at, 'is not an http or https ID in canonical form');
      }
      if (
        !allowed.valid(value) ||
        (Array.isArray(value) &&
          (allowed.reading !== 'data' || (key === 'enum' && value.length < AJV_LOOP_ENUM)) &&
          hasHoleOrUndefined(value))
      ) {
        throw new JsonSchemaUnsupportedError(key, at, 'has a value of the wrong type');
      }
      if (key === 'enum' && dialect === 'draft-07' && hasRepeatedMember(value as unknown[])) {
        throw new JsonSchemaUnsupportedError(key, at, 'repeats a member, which the draft-07 metaschema refuses');
      }
      if (
        key === 'dependentRequired' &&
        Object.entries(value as Record<string, string[]>).some(
          ([name, names]) => PROTOTYPE_NAMES.has(name) || names.some(other => PROTOTYPE_NAMES.has(other)),
        )
      ) {
        throw new JsonSchemaUnsupportedError(key, at, 'names a property like an Object prototype key');
      }
      switch (allowed.reading) {
        case 'schema':
          // Draft-07 `items` may list one schema per position, as Ajv's draft-07 metaschema
          // allowed (at least one); the 2020-12 metaschema allowed only one schema there.
          if (key === 'items' && dialect === 'draft-07' && Array.isArray(value) && value.length > 0) {
            value.forEach((item, index) => enter(item, `${at}/${index}`));
          } else {
            enter(value, at);
          }
          break;
        case 'schema-map':
          for (const name of Object.keys(value as Record<string, unknown>)) {
            if ((key === 'properties' ? PROTOTYPE_NAMES : EQUALITY_NAMES).has(name)) {
              throw new JsonSchemaUnsupportedError(
                key,
                `${at}/${pointerToken(name)}`,
                'names a member like an Object prototype key',
              );
            }
            enter((value as Record<string, unknown>)[name], `${at}/${pointerToken(name)}`, key === 'properties');
          }
          break;
        case 'schema-tuple':
          (value as unknown[]).forEach((item, index) => enter(item, `${at}/${index}`));
          break;
        case 'schema-list':
          (value as unknown[]).forEach((branch, index) => {
            enter(branch, `${at}/${index}`);
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
          if ((key === 'const' || key === 'enum') && holdsUndefined(value)) {
            // Ajv's equality compared an `undefined` member, which JSON cannot carry.
            throw new JsonSchemaUnsupportedError(key, at, 'holds an undefined value');
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
        kids.push(target);
      }
    }
  };
  visit(root, '');
  // Fork main's Ajv compiled only what the root reaches; it never resolved a `$ref` or entered
  // a loop in a definition nothing reaches.
  const reachable = new Set<object>();
  const pending: object[] = typeof root === 'object' && root !== null ? [root] : [];
  for (let next = pending.pop(); next !== undefined; next = pending.pop()) {
    if (!reachable.has(next)) {
      reachable.add(next);
      pending.push(...(children.get(next) ?? []));
    }
  }
  refuseDefsScopeChange(root, reachable);
  refuseNestedIdsAndAnchors(root);
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
    if (reachable.has(node) && !state.has(node)) {
      walk(node);
    }
  }
  return { comparesValues: comparing };
}

/** Whether a list holds two members that are equal as JSON, as the draft-07 metaschema's `uniqueItems` compared them. */
function hasRepeatedMember(list: unknown[]): boolean {
  const canonical = (value: unknown): string => {
    if (Array.isArray(value)) {
      return `[${value.map(canonical).join(',')}]`;
    }
    if (typeof value === 'object' && value !== null) {
      const record = value as Record<string, unknown>;
      return `{${Object.keys(record)
        .sort()
        .map(key => `${JSON.stringify(key)}:${canonical(record[key])}`)
        .join(',')}}`;
    }
    return JSON.stringify(value) ?? 'undefined';
  };
  return new Set(list.map(canonical)).size !== list.length;
}

/**
 * Refuses a `$defs` member named `$id` that fork main's Ajv read as the scope of every `$ref`
 * into `$defs` that the root reaches: it threw on a value other than a string, and resolved the
 * references inside `$defs` against a string.
 */
function refuseDefsScopeChange(root: unknown, reachable: ReadonlySet<object>): void {
  if (!isJsonObject(root) || !isJsonObject(root.$defs) || !root.$defs.$id) {
    return;
  }
  const defs = root.$defs;
  let throughDefs = false;
  let insideDefs = false;
  forEachSchemaObject(root, (node, pointer) => {
    if (reachable.has(node) && typeof node.$ref === 'string' && node.$ref.startsWith('#/$defs/')) {
      throughDefs = true;
      insideDefs ||= pointer.startsWith('/$defs/');
    }
  });
  if (throughDefs && (typeof defs.$id !== 'string' || insideDefs)) {
    throw new JsonSchemaUnsupportedError(
      '$defs',
      '/$defs/$id',
      'names a member "$id", which fork main\'s Ajv read as the scope of a "$ref" into "$defs"',
    );
  }
}

/**
 * Walks the objects fork main's Ajv walked for IDs and anchors (every schema and every
 * object-valued annotation, as json-schema-traverse with `allKeys`) and refuses a string `$id`,
 * `$anchor` or `$dynamicAnchor` in any of them below the root. Ajv registered each one as a
 * reference scope and threw on a malformed or repeated one; this validator resolves none of
 * them, so none is accepted. The root `$id` is checked where it is read.
 */
function refuseNestedIdsAndAnchors(root: unknown): void {
  const walk = (node: unknown, pointer: string): void => {
    if (typeof node !== 'object' || node === null || Array.isArray(node)) {
      return;
    }
    const record = node as Record<string, unknown>;
    if (pointer !== '') {
      for (const keyword of ['$id', '$anchor', '$dynamicAnchor']) {
        if (typeof record[keyword] === 'string') {
          throw new JsonSchemaUnsupportedError(
            keyword,
            `${pointer}/${keyword}`,
            'is below the root; nested IDs and anchors are not supported',
          );
        }
      }
    }
    for (const [key, value] of Object.entries(record)) {
      const at = `${pointer}/${pointerToken(key)}`;
      // json-schema-traverse tested `key in table`, so a name `Object.prototype` holds, such as
      // `toString`, counted as an array keyword and as a name-map keyword.
      const inherited = PROTOTYPE_NAMES.has(key);
      if (Array.isArray(value)) {
        if (inherited || AJV_TRAVERSE_ARRAYS.has(key)) {
          value.forEach((item, index) => walk(item, `${at}/${index}`));
        }
      } else if (inherited || AJV_TRAVERSE_MAPS.has(key)) {
        if (typeof value === 'object' && value !== null) {
          for (const [name, member] of Object.entries(value)) {
            walk(member, `${at}/${pointerToken(name)}`);
          }
        }
      } else if (!AJV_TRAVERSE_SKIPPED.has(key)) {
        walk(value, at);
      }
    }
  };
  walk(root, '');
}

/** Whether literal data holds an object with a key that Ajv's equality reads from the prototype. */
function holdsPrototypeName(value: unknown): boolean {
  if (typeof value !== 'object' || value === null) {
    return false;
  }
  if (!Array.isArray(value) && Object.keys(value).some(key => EQUALITY_NAMES.has(key))) {
    return true;
  }
  return Object.values(value).some(holdsPrototypeName);
}

/** Whether literal data holds `undefined` anywhere. */
function holdsUndefined(value: unknown): boolean {
  if (value === undefined) {
    return true;
  }
  return typeof value === 'object' && value !== null && Object.values(value).some(holdsUndefined);
}

/**
 * Whether `value` is a `Proxy`, where the runtime can tell (Node and Bun expose
 * `util.types.isProxy`). Elsewhere a `Proxy` is not recognised; it is then read once, through its
 * traps, into the snapshot, and only the snapshot is validated.
 */
const isProxy: (value: object) => boolean = (() => {
  const util = (globalThis as { process?: { getBuiltinModule?: (id: string) => unknown } }).process?.getBuiltinModule?.(
    'node:util',
  ) as { types?: { isProxy?: (value: unknown) => boolean } } | undefined;
  const check = util?.types?.isProxy;
  return typeof check === 'function' ? (value: object) => check(value) : () => false;
})();

/** The deepest nesting a schema or value snapshot holds; deeper is refused as not plain JSON. */
export const MAX_JSON_DEPTH = 1000;

/** Why a value or schema is not plain JSON, and where. */
export class NotPlainJsonError extends Error {
  readonly path: Array<string | number>;
  readonly reason: string;

  constructor(path: Array<string | number>, reason: string) {
    super(reason);
    this.name = 'NotPlainJsonError';
    this.path = path;
    this.reason = reason;
  }
}

/**
 * Copies `value` into plain JSON in one walk, reading each property once, or throws a
 * {@link NotPlainJsonError} naming the first place that is not plain JSON. Plain JSON is what
 * `JSON.parse` makes: objects whose prototype is `Object.prototype`, dense arrays whose prototype
 * is `Array.prototype` and that hold nothing but their items, strings, finite numbers, booleans
 * and `null`, every key an enumerable own string key held as data. So a `Proxy` (where the runtime
 * can tell), a getter or setter, a symbol or non-enumerable key, a `Date` or class instance, an
 * object with a `null` prototype, an array subclass, a sparse array, a cycle, a function, a
 * `bigint`, a symbol and a non-finite number are all refused.
 *
 * - `schema`: an object reached through two references is refused too, so the snapshot is the
 *   tree a JSON document would give, and so is a frozen, sealed or non-extensible object or a
 *   read-only member. An object member whose value is `undefined` is kept, and
 *   the allow-list reads it as an absent keyword (as Ajv did) or refuses it.
 * - `value`: an object reached through two references is copied once and shared, as Ajv never
 *   changed the value. An object member whose value is `undefined` is kept: the patched
 *   validator handles it as Ajv did (absent for `properties` and `required`, present for
 *   `additionalProperties` and the property counts). Model output can hold one.
 *
 * Nesting deeper than {@link MAX_JSON_DEPTH} levels along the first visit to each object is
 * refused; deeper shared structures under a recursive schema are caught during validation and
 * refused as non-retryable issues.
 */
export function plainJsonSnapshot(value: unknown, kind: 'schema' | 'value'): unknown {
  const open = new Set<object>();
  const copies = new Map<object, unknown>();
  const copy = (node: unknown, path: Array<string | number>, member: boolean): unknown => {
    switch (typeof node) {
      case 'string':
      case 'boolean':
        return node;
      case 'number':
        if (!Number.isFinite(node)) {
          throw new NotPlainJsonError(path, 'is a number that is not finite');
        }
        return node;
      case 'undefined':
        if (!member) {
          throw new NotPlainJsonError(path, 'is undefined outside an object member');
        }
        return undefined;
      case 'object':
        break;
      default:
        throw new NotPlainJsonError(path, `is a ${typeof node}`);
    }
    if (node === null) {
      return null;
    }
    if (open.has(node)) {
      throw new NotPlainJsonError(path, 'is part of a cycle');
    }
    if (path.length >= MAX_JSON_DEPTH) {
      throw new NotPlainJsonError(path, `is nested deeper than ${MAX_JSON_DEPTH} levels`);
    }
    if (copies.has(node)) {
      if (kind === 'schema') {
        throw new NotPlainJsonError(path, 'is an object reached through more than one reference');
      }
      return copies.get(node);
    }
    if (isProxy(node)) {
      throw new NotPlainJsonError(path, 'is a Proxy');
    }
    const array = Array.isArray(node);
    if (Object.getPrototypeOf(node) !== (array ? Array.prototype : Object.prototype)) {
      throw new NotPlainJsonError(path, array ? 'is an array subclass' : 'is not a plain object');
    }
    // Fork main's Ajv wrote to some schemas while compiling them (`nullable` adds "null" to a
    // `type` list) and threw where it could not, so a schema must be writable, as JSON.parse makes it.
    if (kind === 'schema' && !Object.isExtensible(node)) {
      throw new NotPlainJsonError(path, 'is frozen, sealed or not extensible');
    }
    open.add(node);
    const keys = Reflect.ownKeys(node);
    let result: unknown;
    if (array) {
      const list = node as unknown[];
      const items: unknown[] = [];
      for (const key of keys) {
        if (
          key !== 'length' &&
          (typeof key !== 'string' || !/^(?:0|[1-9][0-9]*)$/.test(key) || Number(key) >= list.length)
        ) {
          throw new NotPlainJsonError([...path, String(key)], 'is an array key that is not an index');
        }
      }
      for (let index = 0; index < list.length; index++) {
        const descriptor = Object.getOwnPropertyDescriptor(list, index);
        if (descriptor === undefined) {
          throw new NotPlainJsonError([...path, index], 'is a hole in a sparse array');
        }
        if (!('value' in descriptor) || !descriptor.enumerable) {
          throw new NotPlainJsonError([...path, index], 'is read through a getter or setter, or is not enumerable');
        }
        if (kind === 'schema' && !(descriptor.writable && descriptor.configurable)) {
          throw new NotPlainJsonError([...path, index], 'is read-only');
        }
        items.push(copy(descriptor.value, [...path, index], false));
      }
      result = items;
    } else {
      const record: Record<string, unknown> = {};
      for (const key of keys) {
        if (typeof key === 'symbol') {
          throw new NotPlainJsonError(path, 'has a symbol key');
        }
        const descriptor = Object.getOwnPropertyDescriptor(node, key)!;
        if (!('value' in descriptor)) {
          throw new NotPlainJsonError([...path, key], 'is read through a getter or setter');
        }
        if (!descriptor.enumerable) {
          throw new NotPlainJsonError([...path, key], 'is not enumerable');
        }
        if (kind === 'schema' && !(descriptor.writable && descriptor.configurable)) {
          throw new NotPlainJsonError([...path, key], 'is read-only');
        }
        // `defineProperty`, so a key named `__proto__` stays an own data key, as `JSON.parse` keeps it.
        Object.defineProperty(record, key, {
          value: copy(descriptor.value, [...path, key], true),
          enumerable: true,
          writable: true,
          configurable: true,
        });
      }
      result = record;
    }
    open.delete(node);
    copies.set(node, result);
    return result;
  };
  return copy(value, [], false);
}

/**
 * Where a value refused under a schema that compares values (`const` or `enum`) holds an object
 * key that Ajv's equality read from the prototype (`constructor`, `toString`, `valueOf`,
 * `__proto__`), or `undefined`. Ajv's equality compared constructors and read those keys, and this
 * validator's does not. It is checked over the whole value, not only where it is compared.
 */
function equalityNameLocation(
  value: unknown,
  path: Array<string | number> = [],
  passed: Set<object> = new Set(),
): Array<string | number> | undefined {
  // A value snapshot shares an object reached through two references; one that passed once is
  // not scanned again, so the scan stays linear in the size of the value.
  if (typeof value !== 'object' || value === null || passed.has(value)) {
    return undefined;
  }
  for (const [key, member] of Object.entries(value)) {
    const at = [...path, Array.isArray(value) ? Number(key) : key];
    if (!Array.isArray(value) && EQUALITY_NAMES.has(key)) {
      return at;
    }
    const found = equalityNameLocation(member, at, passed);
    if (found !== undefined) {
      return found;
    }
  }
  passed.add(value);
  return undefined;
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
      // A keyword whose value is `undefined` counts as absent, as the allow-list and Ajv read it.
      if (value === undefined) {
        continue;
      }
      const at = `${pointer}/${pointerToken(key)}`;
      switch ((ALLOWED_KEYWORDS.get(key) ?? DRAFT_2020_12_KEYWORDS.get(key) ?? DRAFT_07_KEYWORDS.get(key))?.reading) {
        case 'schema':
          if (Array.isArray(value)) {
            value.forEach((item, index) => walk(item, `${at}/${index}`));
          } else {
            walk(value, at);
          }
          break;
        case 'schema-map':
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
 * every `$ref` to the schema its JSON Pointer names. The allow-list already refuses every `$id`
 * and anchor that could take that place; this check fails closed if one is ever missed.
 */
function toValidationSchema(
  snapshot: unknown,
  dereference: (schema: Schema) => Lookup,
): { schema: Schema; comparesValues: boolean } {
  const { comparesValues } = refuseUnlistedSchema(snapshot);
  const copy = snapshot as Schema;
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
 * Marks a Standard Schema issue returned for a schema refusal, non-plain-JSON value, prototype-key
 * refusal, or unexpected validator failure. Tool input validation treats any issue carrying this
 * marker as final and skips null-stripping/coercion/alias retries so it always fails closed.
 */
export const JSON_SCHEMA_NON_RETRYABLE: unique symbol = Symbol.for('mastra.schemaCompat.jsonSchemaNonRetryable');

function markNonRetryable(issue: StandardSchemaV1.Issue): StandardSchemaV1.Issue {
  Object.defineProperty(issue, JSON_SCHEMA_NON_RETRYABLE, { value: true });
  return issue;
}

/**
 * Resolves a cfworker `keywordLocation` (which may cross `$ref` hops) against the compiled
 * validation schema. Returns `undefined` if any segment does not resolve.
 */
function resolveSchemaNode(root: unknown, keywordLocation: string): unknown {
  const pointer = keywordLocation.startsWith('#') ? keywordLocation.slice(1) : keywordLocation;
  if (pointer === '') {
    return root;
  }
  const segments = pointer
    .slice(1)
    .split('/')
    .map(segment => decodeURIComponent(segment).replace(/~1/g, '/').replace(/~0/g, '~'));
  let current: unknown = root;
  let inSchemaMap = false;
  for (const seg of segments) {
    if (inSchemaMap) {
      if (!isJsonObject(current) || !Object.hasOwn(current, seg)) {
        return undefined;
      }
      current = current[seg];
      inSchemaMap = false;
      continue;
    }
    if (Array.isArray(current)) {
      const idx = Number(seg);
      if (!Number.isInteger(idx) || idx < 0 || idx >= current.length) {
        return undefined;
      }
      current = current[idx];
      continue;
    }
    if (!isJsonObject(current)) {
      return undefined;
    }
    if (seg === '$ref' && typeof current.$ref === 'string') {
      current = namedReferenceTarget(root, current.$ref);
      continue;
    }
    if (!Object.hasOwn(current, seg)) {
      return undefined;
    }
    current = current[seg];
    if (seg === 'properties' || seg === '$defs' || seg === 'definitions') {
      inSchemaMap = true;
    }
  }
  return current;
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
const SUMMARY_KEYWORDS = new Set(['properties', 'prefixItems', 'allOf', '$ref']);

/**
 * Normalises cfworker's `OutputUnit[]` to match Ajv's leaf errors and instance paths:
 * - `propertyNames`: keeps the parent error and rewrites every sub-error's `instanceLocation` to
 *   the parent object (dropping a direct `propertyNames: false` duplicate), matching Ajv;
 * - `additionalProperties: false`, `additionalItems: false`, and 2020-12 `prefixItems` + `items:
 *   false`: keeps the parent error at the container path and drops the paired `False boolean
 *   schema.` child error, matching Ajv;
 * - `additionalProperties`, `additionalItems`, and `items` with a non-false subschema (or
 *   standalone `items: false`): drops the parent summary error and keeps the child errors at the
 *   property/item path, matching Ajv;
 * - `properties`, `prefixItems`, `allOf`, `$ref`: drops summary errors.
 */
function leafErrors(errors: OutputUnit[], compiledSchema: unknown): OutputUnit[] {
  const out: OutputUnit[] = [];
  for (let i = 0; i < errors.length; i++) {
    const error = errors[i]!;
    if (error.keyword === 'propertyNames') {
      out.push(error);
      const pnParent = error.instanceLocation;
      const pnKwPrefix = `${error.keywordLocation}/`;
      const next = errors[i + 1];
      const childInst =
        next !== undefined && next.instanceLocation.startsWith(`${pnParent}/`) ? next.instanceLocation : undefined;
      if (childInst !== undefined) {
        const directFalse = resolveSchemaNode(compiledSchema, error.keywordLocation) === false;
        while (i + 1 < errors.length) {
          const child = errors[i + 1]!;
          const isSubError =
            child.instanceLocation === childInst &&
            (child.keywordLocation.startsWith(pnKwPrefix) ||
              (child.keyword === 'false' && child.keywordLocation === childInst));
          if (!isSubError) {
            break;
          }
          i += 1;
          if (directFalse && child.keyword === 'false') {
            continue;
          }
          if (SUMMARY_KEYWORDS.has(child.keyword)) {
            continue;
          }
          out.push({ ...child, instanceLocation: pnParent });
        }
      }
      continue;
    }

    if (error.keyword === 'additionalProperties') {
      const target = resolveSchemaNode(compiledSchema, error.keywordLocation);
      if (target === false) {
        out.push(error);
        const next = errors[i + 1];
        if (next?.keyword === 'false' && next.keywordLocation === next.instanceLocation) {
          i += 1;
        }
      }
      continue;
    }

    if (error.keyword === 'additionalItems') {
      const target = resolveSchemaNode(compiledSchema, error.keywordLocation);
      if (target === false) {
        out.push(error);
        const next = errors[i + 1];
        if (next?.keyword === 'false' && next.keywordLocation === next.instanceLocation) {
          i += 1;
        }
      }
      continue;
    }

    if (error.keyword === 'items') {
      const parentKwLoc = error.keywordLocation.endsWith('/items')
        ? error.keywordLocation.slice(0, -'/items'.length)
        : '';
      const parentNode = resolveSchemaNode(compiledSchema, parentKwLoc);
      if (isJsonObject(parentNode) && Array.isArray(parentNode.prefixItems) && parentNode.items === false) {
        out.push(error);
        const next = errors[i + 1];
        if (next?.keyword === 'false' && next.keywordLocation === next.instanceLocation) {
          i += 1;
        }
      }
      continue;
    }

    if (SUMMARY_KEYWORDS.has(error.keyword)) {
      continue;
    }

    out.push(error);
  }
  return out;
}

/** A Standard Schema issue for a value refused because it is not plain JSON. */
function notPlainJsonIssue(error: NotPlainJsonError): StandardSchemaV1.Issue {
  return markNonRetryable({
    message: `The value ${error.reason}, which JSON cannot carry, so it is refused.`,
    path: error.path,
  });
}

/**
 * One JSON Schema in the path where code generation is disallowed. The schema is read once, into
 * a plain-JSON snapshot, when the object is made; it is checked against the allow-list and
 * compiled when it is first needed, by {@link refusal} or {@link validate}, and the result is kept.
 */
export class InterpretedJsonSchema {
  #snapshot: unknown;
  #snapshotError: string | undefined;
  #compiledSchema: unknown;
  #validator: Validator | null = null;
  #comparesValues = false;
  #refusal: string | undefined;

  /** `refusedBecause` refuses the schema without reading it, for a reason the caller names. */
  constructor(schema: unknown, refusedBecause?: string) {
    let snapshot: unknown;
    let snapshotError: string | undefined;
    if (refusedBecause !== undefined) {
      this.#snapshotError = new JsonSchemaUnsupportedError('schema', '', refusedBecause).message;
      return;
    }
    try {
      snapshot = plainJsonSnapshot(schema, 'schema');
    } catch (error) {
      if (!(error instanceof NotPlainJsonError)) {
        throw error;
      }
      const pointer = error.path.map(part => `/${pointerToken(String(part))}`).join('');
      snapshotError = new JsonSchemaUnsupportedError('schema', pointer, `${error.reason}, which JSON cannot carry`)
        .message;
    }
    this.#snapshot = snapshot;
    this.#snapshotError = snapshotError;
  }

  /**
   * Builds the validator if it is not built yet. Returns why the schema is refused, or
   * `undefined` once it is compiled. While the validator is still loading it reports that, and
   * tries again on the next call.
   */
  refusal(): string | undefined {
    if (this.#validator !== null) {
      return undefined;
    }
    if (this.#refusal !== undefined) {
      return this.#refusal;
    }
    if (this.#snapshotError !== undefined) {
      this.#refusal = this.#snapshotError;
      return this.#refusal;
    }
    if (!ValidatorClass || !dereferenceSchema) {
      return `JSON Schema validator unavailable (${validatorUnavailableReason})`;
    }
    try {
      const { schema, comparesValues } = toValidationSchema(this.#snapshot, dereferenceSchema);
      this.#compiledSchema = schema;
      this.#comparesValues = comparesValues;
      // Every schema, draft-07 included, is validated in the validator's 2020-12 mode. In its
      // draft-07 mode the validator skips keywords beside `$ref` (for example `{ $ref, maxLength }`),
      // which Ajv applied, so input Ajv refused would pass. The validator reads the draft only for
      // that and for draft-04, so draft-07 tuple `items`, `additionalItems` and `dependencies`
      // behave the same in 2020-12 mode.
      this.#validator = new ValidatorClass(schema, '2020-12', false);
      return undefined;
    } catch (error) {
      this.#refusal = error instanceof JsonSchemaUnsupportedError ? error.message : 'the schema cannot be compiled';
      return this.#refusal;
    }
  }

  /** Validates a plain-JSON snapshot of `value`, and returns `value` itself when it is valid. */
  validate<Output>(value: unknown): StandardSchemaV1.Result<Output> {
    const refusal = this.refusal();
    if (refusal !== undefined) {
      return { issues: [markNonRetryable({ message: `Schema validation error: ${refusal}` })] };
    }
    let snapshot: unknown;
    try {
      snapshot = plainJsonSnapshot(value, 'value');
    } catch (error) {
      if (error instanceof NotPlainJsonError) {
        return { issues: [notPlainJsonIssue(error)] };
      }
      throw error;
    }
    if (this.#comparesValues) {
      const at = equalityNameLocation(snapshot);
      if (at !== undefined) {
        return {
          issues: [
            markNonRetryable({
              message:
                'The value holds a key that Ajv compared through the prototype (constructor, toString, valueOf or __proto__) under a schema with const or enum, so it is refused.',
              path: at,
            }),
          ],
        };
      }
    }
    let result: ReturnType<Validator['validate']>;
    try {
      result = this.#validator!.validate(snapshot);
    } catch (error) {
      // For example a stack overflow under a recursive schema, or a key holding a lone surrogate
      // that the validator cannot write into an error location. The value is refused as a
      // non-retryable issue so tool-input validation never strips nulls and retries past it.
      const reason = error instanceof Error ? error.message : 'unknown error';
      return {
        issues: [markNonRetryable({ message: `The value could not be checked (${reason}), so it is refused.` })],
      };
    }
    if (result.valid) {
      return { value: value as Output };
    }
    const leaves = leafErrors(result.errors, this.#compiledSchema);
    // An invalid value is never returned as valid, even if every error was a summary.
    const errors = leaves.length === 0 ? result.errors : leaves;
    const issues: StandardSchemaV1.Issue[] = errors.map(error => ({
      message: error.error,
      path: instanceLocationToPath(error.instanceLocation),
    }));
    return { issues: issues.length > 0 ? issues : [{ message: 'Instance does not match the schema.' }] };
  }
}
