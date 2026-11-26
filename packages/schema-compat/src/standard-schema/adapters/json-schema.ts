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
 * Keywords that name or reach a schema by URI or anchor. `@cfworker/json-schema` resolves only
 * some of them as Ajv did (`$dynamicRef` not at all), and the code that resolved the rest kept
 * producing schemas that were checked against the wrong target. Real tool schemas do not use
 * them, so a schema that does is refused.
 */
const UNSUPPORTED_KEYWORDS = new Set<string>([
  '$anchor',
  '$dynamicAnchor',
  '$dynamicRef',
  '$id',
  '$recursiveAnchor',
  '$recursiveRef',
]);

/** Keys the validator writes into a schema while resolving references; a schema must not bring its own. */
const VALIDATOR_INTERNAL_KEYS = new Set<string>(['__absolute_recursive_ref__', '__absolute_ref__', '__absolute_uri__']);

/** Keys the validator's dereference reads on every object it walks, whatever their value. */
const DEREFERENCE_READ_KEYS = new Set<string>([
  '$ref',
  '$recursiveRef',
  '$anchor',
  '__absolute_recursive_ref__',
  '__absolute_ref__',
  '__absolute_uri__',
]);

/** A `$ref` the adapter resolves: the whole schema (`#`) or a JSON Pointer into it (`#/...`). */
function isLocalPointerRef(ref: string): boolean {
  return ref === '#' || ref.startsWith('#/');
}

function pointerToken(key: string): string {
  return key.replace(/~/g, '~0').replace(/\//g, '~1');
}

/** Throws when `key` (holding `value`) at `at` is a keyword the adapter refuses. */
function refuseUnsupportedKey(key: string, value: unknown, at: string): void {
  if (UNSUPPORTED_KEYWORDS.has(key)) {
    throw new Error(
      `${key} at "${at}" is not supported: only "#" and "#/..." references within the schema are resolved, so the schema is refused rather than checked loosely`,
    );
  }
  if (VALIDATOR_INTERNAL_KEYS.has(key)) {
    throw new Error(`${key} at "${at}" is reserved for the validator, so the schema is refused`);
  }
  if (key === '$ref' && value === '#/') {
    throw new Error(
      `$ref "#/" at "${at}" is not supported: the validator reads it as the root's "" member, not as the root, so the schema is refused; write "#" for the root`,
    );
  }
  if (key === '$ref' && (typeof value !== 'string' || !isLocalPointerRef(value))) {
    throw new Error(
      `$ref ${typeof value === 'string' ? `"${value}"` : 'that is not a string'} at "${at}" is not supported: only "#" and "#/..." references within the schema are resolved, so the schema is refused rather than checked loosely`,
    );
  }
}

/**
 * Refuses a schema that uses `$id`, `$anchor`, `$dynamicAnchor`, `$dynamicRef`,
 * `$recursiveAnchor` or `$recursiveRef`, a `$ref` other than a `#` or `#/...` string, or one of
 * the keys the validator writes while resolving references (`__absolute_ref__` and the like),
 * wherever the validator can read the object holding it as a schema. The error names the
 * keyword and its JSON Pointer in the schema.
 *
 * What each object is comes from `assignRoles`, which reads the schema from its root and
 * through every `#`/`#/...` reference, as the validator does:
 * - an object read as a schema (`schema`), including a name map or literal data that a
 *   `$ref` reaches, is checked for every key;
 * - an object no reading reaches (`weak`, such as the value of `x-meta` or an unused
 *   `components` entry) is checked too, because the validator's dereference still walks it;
 * - a name map's own keys and literal data under `const`, `enum`, `default` or `examples` are
 *   names and values, so a property named `$id` or a `const` holding `{ "$id": "k" }` is kept.
 *   In a name map, a key whose value is neither an object, a boolean nor an array (so no
 *   property schema or dependency list) is refused. Where the dereference walk also reaches such an object as an unknown keyword's
 *   value, a key whose value is not an object (which the walk could read as that keyword), and
 *   a `$ref`, `$recursiveRef`, `$anchor` or validator-internal key with any value (the walk
 *   reads those on every object it visits), are refused.
 */
function refuseUnsupportedKeywords(root: unknown): void {
  const pointers = new Map<object, string>();
  const collect = (node: unknown, pointer: string): void => {
    if (typeof node !== 'object' || node === null || pointers.has(node)) {
      return;
    }
    pointers.set(node, pointer);
    for (const [key, value] of Object.entries(node)) {
      collect(value, `${pointer}/${Array.isArray(node) ? key : pointerToken(key)}`);
    }
  };
  collect(root, '');
  const roles = assignRoles(root, node => {
    const ref = node.$ref;
    if (typeof ref !== 'string' || !isLocalPointerRef(ref)) {
      return [];
    }
    return [ref === '#' ? root : resolveByPointer({ '': root }, ref)];
  });
  for (const [node, pointer] of pointers) {
    const set = roles.get(node);
    if (Array.isArray(node) || !set) {
      continue;
    }
    const strong = set.has('schema') || set.has('names') || set.has('data');
    const readAsSchema = set.has('schema') || (!strong && set.has('weak'));
    const walkedAsSchema = set.has('weak');
    const nameMap = set.has('names') || set.has('weak-names');
    for (const [key, value] of Object.entries(node)) {
      if (
        readAsSchema ||
        (walkedAsSchema && (!isPlainObject(value) || DEREFERENCE_READ_KEYS.has(key))) ||
        (nameMap && !isPlainObject(value) && typeof value !== 'boolean' && !Array.isArray(value))
      ) {
        refuseUnsupportedKey(key, value, `${pointer}/${pointerToken(key)}`);
      }
    }
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
  // Only draft-04 used `id`. Ajv refused a schema holding it, and the patched
  // validator ignores it (it reads only a string `$id`); `$id` is unaffected.
  delete record.id;
}

const REF_PLACEMENT_MESSAGE =
  'a $ref resolves to a different target once the schema is prepared for validation, so the schema is refused';

const REF_ROLE_CONFLICT_MESSAGE =
  'a $ref target that the schema also uses as a name map or as literal data holds a boolean exclusiveMinimum or exclusiveMaximum, which cannot be read both ways, so the schema is refused';

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/**
 * What an object is to the validator. `schema`, `names` (a name map's own keys) and `data`
 * (literal data under `const`, `enum`, `default` or `examples`) come from reading the schema
 * from its root and from every `$ref` at a schema position, resolved by JSON Pointer as the
 * validator resolves them. The `weak`
 * roles cover objects that reading never reaches, such as an unused `components` or `x-meta`:
 * there an object is normalised as a schema unless it sits under a name-map or data keyword.
 */
type Role = 'schema' | 'names' | 'data' | 'weak' | 'weak-names' | 'weak-data';

/**
 * The entries the validator reads from an object-valued `prefixItems`: indices below `length`,
 * each with its key.
 */
function prefixItemsRead(value: Record<string, unknown>): Array<[string, unknown]> {
  const length = Number(value.length);
  return Object.entries(value).filter(([key]) => /^(0|[1-9][0-9]*)$/.test(key) && Number(key) < length);
}

/**
 * Returns the role of every object, reading the schema from its root and through its references.
 */
function assignRoles(root: unknown, targetsOf: (node: Record<string, unknown>) => unknown[]): Map<object, Set<Role>> {
  const roles = new Map<object, Set<Role>>();
  const pending: unknown[] = [];
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
      } else if (role === 'schema' && SCHEMA_MAP_KEYWORD_SET.has(key) && Array.isArray(value)) {
        // The validator reads an array-valued name map member by member (`for...in` or
        // `Object.entries`, named members included), so its members are schemas; the dereference
        // skips it.
        mark(value, 'schema');
        Object.values(value).forEach(item => walk(item, 'schema'));
      } else if (SCHEMA_MAP_KEYWORD_SET.has(key)) {
        walk(value, role === 'schema' ? 'names' : 'weak-names');
      } else if (
        role === 'schema' &&
        (SUBSCHEMA_KEYWORDS.has(key) || (SUBSCHEMA_ARRAY_KEYWORDS.has(key) && Array.isArray(value)))
      ) {
        walk(value, 'schema');
      } else if (role === 'schema' && key === 'prefixItems' && isPlainObject(value)) {
        // The validator reads an object-valued `prefixItems` item by item through its `length`
        // (`$prefixItems[i]`), so the values at its indices below `length` are schemas; the
        // dereference walks the object itself as an unknown keyword's value.
        prefixItemsRead(value).forEach(([, item]) => walk(item, 'schema'));
        walk(value, 'weak');
      } else {
        walk(value, 'weak');
      }
    }
    if (role === 'schema') {
      pending.push(...targetsOf(record));
    }
  };
  walk(root, 'schema');
  // Each object is walked once per role, so the walk stays linear in the size of the schema.
  while (pending.length > 0) {
    walk(pending.pop(), 'schema');
  }
  return roles;
}

type Lookup = Record<string, unknown>;

/**
 * The validator's fallback for a reference missing from its lookup (see the cfworker patch):
 * the JSON Pointer in the fragment, walked from the document the URI names.
 */
function resolveByPointer(lookup: Lookup, uri: string): unknown {
  const hash = uri.indexOf('#');
  if (hash < 0 || uri[hash + 1] !== '/') {
    return undefined;
  }
  let node: unknown = lookup[uri.slice(0, hash)];
  for (const token of uri.slice(hash + 2).split('/')) {
    let name: string;
    try {
      name = decodeURIComponent(token).replace(/~1/g, '/').replace(/~0/g, '~');
    } catch {
      return undefined;
    }
    if (typeof node !== 'object' || node === null || !Object.hasOwn(node, name)) {
      return undefined;
    }
    node = (node as Record<string, unknown>)[name];
  }
  return node;
}

/**
 * What the `$ref` on `node` resolves to, as the validator resolves it after `dereference`
 * recorded its absolute URI on the object.
 */
function referenceTargets(node: Record<string, unknown>, lookup: Lookup): unknown[] {
  if (typeof node.$ref !== 'string') {
    return [];
  }
  const uri = typeof node.__absolute_ref__ === 'string' ? node.__absolute_ref__ : node.$ref;
  return [lookup[uri] ?? resolveByPointer(lookup, uri)];
}

/**
 * Refuses a schema in which a `$ref` the validator can reach from the root, through its
 * applicators and other references, points nowhere or at a value that is not a schema. The
 * validator would otherwise throw only for a value that reaches the reference. Ajv refused most
 * of these when compiling; this also refuses them where fork main's draft-07 Ajv compiled them.
 * Definitions no reference uses, `then` and `else` without `if`, `additionalItems` without an
 * array `items`, and `unevaluatedProperties` beside `additionalProperties` are not read, as the
 * validator does not read them.
 */
function refuseUnresolvedReferences(root: unknown): void {
  const seen = new Set<object>();
  const unevaluatedPropertiesHolders: Array<[Record<string, unknown>, string]> = [];
  const pending: Array<[unknown, string]> = [[root, '']];
  while (pending.length > 0) {
    const [node, pointer] = pending.pop()!;
    if (typeof node !== 'object' || node === null || seen.has(node)) {
      continue;
    }
    seen.add(node);
    if (Array.isArray(node)) {
      node.forEach((item, index) => pending.push([item, `${pointer}/${index}`]));
      continue;
    }
    const record = node as Record<string, unknown>;
    if (typeof record.$ref === 'string') {
      const target = record.$ref === '#' ? root : resolveByPointer({ '': root }, record.$ref);
      if (target === undefined) {
        throw new Error(
          `$ref "${record.$ref}" at "${pointer}/$ref" does not resolve within the schema, so the schema is refused`,
        );
      }
      if (!isPlainObject(target) && typeof target !== 'boolean') {
        throw new Error(
          `$ref "${record.$ref}" at "${pointer}/$ref" points at a value that is not a schema, so the schema is refused`,
        );
      }
      pending.push([target, record.$ref.slice(1)]);
    }
    for (const [key, value] of Object.entries(record)) {
      const at = `${pointer}/${pointerToken(key)}`;
      if (
        ((key === 'then' || key === 'else') && record.if === undefined) ||
        (key === 'additionalItems' && !Array.isArray(record.items)) ||
        (key === 'unevaluatedProperties' && record.additionalProperties !== undefined)
      ) {
        continue;
      }
      if (SUBSCHEMA_KEYWORDS.has(key) || (SUBSCHEMA_ARRAY_KEYWORDS.has(key) && Array.isArray(value))) {
        pending.push([value, at]);
      } else if (key === 'prefixItems' && isPlainObject(value)) {
        for (const [index, item] of prefixItemsRead(value)) {
          pending.push([item, `${at}/${index}`]);
        }
      } else if (
        SCHEMA_MAP_KEYWORD_SET.has(key) &&
        key !== '$defs' &&
        key !== 'definitions' &&
        (Array.isArray(value) || isPlainObject(value))
      ) {
        // Named members of an array-valued map are read too.
        for (const [name, item] of Object.entries(value as Record<string, unknown>)) {
          if (isPlainObject(item) || (Array.isArray(value) && typeof item === 'object' && item !== null)) {
            pending.push([item, `${at}/${pointerToken(name)}`]);
          }
        }
      }
      if (key === 'unevaluatedProperties') {
        unevaluatedPropertiesHolders.push([record, pointer]);
      }
    }
  }
  for (const [holder, holderPointer] of unevaluatedPropertiesHolders) {
    refuseUnrecordedDependencies(root, holder, holderPointer);
  }
}

/**
 * The validator checks a schema-valued `dependencies` entry without recording the properties it
 * evaluates, so an `unevaluatedProperties` that reads the same object, beside it or through
 * in-place applicators and references, would refuse them, and under `oneOf` or `not` accept what
 * Ajv refused. Such a schema is refused (`dependentSchemas` records them). Subschemas applied to
 * another instance location, such as under `properties` or `items`, are not followed.
 */
function refuseUnrecordedDependencies(root: unknown, holder: Record<string, unknown>, holderPointer: string): void {
  const seen = new Set<object>();
  const pending: Array<[unknown, string]> = [[holder, holderPointer]];
  while (pending.length > 0) {
    const [node, pointer] = pending.pop()!;
    if (!isPlainObject(node) || seen.has(node)) {
      continue;
    }
    seen.add(node);
    const record = node as Record<string, unknown>;
    const dependencies = record.dependencies;
    if (isPlainObject(dependencies)) {
      for (const [name, entry] of Object.entries(dependencies)) {
        if (isPlainObject(entry) || typeof entry === 'boolean') {
          throw new Error(
            `a schema-valued "dependencies" entry at "${pointer}/dependencies/${pointerToken(name)}" beside "unevaluatedProperties" at "${holderPointer}/unevaluatedProperties" is not supported: the validator does not record the properties it evaluates, so the schema is refused; use "dependentSchemas"`,
          );
        }
      }
    }
    if (typeof record.$ref === 'string') {
      pending.push([record.$ref === '#' ? root : resolveByPointer({ '': root }, record.$ref), record.$ref.slice(1)]);
    }
    for (const key of ['allOf', 'anyOf', 'oneOf'] as const) {
      const branches = record[key];
      if (Array.isArray(branches)) {
        branches.forEach((branch, index) => pending.push([branch, `${pointer}/${key}/${index}`]));
      }
    }
    pending.push([record.not, `${pointer}/not`]);
    if (record.if !== undefined) {
      pending.push([record.if, `${pointer}/if`], [record.then, `${pointer}/then`], [record.else, `${pointer}/else`]);
    }
    if (isPlainObject(record.dependentSchemas)) {
      for (const [name, entry] of Object.entries(record.dependentSchemas as Record<string, unknown>)) {
        pending.push([entry, `${pointer}/dependentSchemas/${pointerToken(name)}`]);
      }
    }
  }
}

/**
 * Applies `nullable`, rewrites boolean exclusive bounds, and drops `format` and `id` on every
 * object the validator can read as a schema, leaving name maps and literal data alone. An
 * object the schema reads both as a schema (through a `$ref`) and as a name map or literal
 * data, such as the target of `{ $ref: '#/properties' }`, is left as written for the name map
 * or the data. Left as written, `nullable` and `format` refuse more values there, and so
 * let more through under `not`, `oneOf` or `if`; a boolean `exclusiveMinimum` or
 * `exclusiveMaximum` would be compared as the number 1 or 0 and let values pass directly, so
 * such a schema is refused.
 */
function normalizeSchemaObjects(root: Schema, dereference: (schema: Schema) => Lookup): void {
  // Every `$ref` is `#` or `#/...` (see `refuseUnsupportedKeywords`), so its target is predicted
  // by walking the JSON Pointer from the root, before normalisation changes the schema.
  const predicted = new Map<object, unknown[]>();
  const targetsOf = (node: Record<string, unknown>): unknown[] => {
    const targets =
      typeof node.$ref === 'string' ? [node.$ref === '#' ? root : resolveByPointer({ '': root }, node.$ref)] : [];
    predicted.set(node, targets);
    return targets;
  };
  const roles = assignRoles(root, targetsOf);
  // Every other `$ref` string in the schema, with the target its JSON Pointer names before
  // normalisation changes the schema (see the check after `dereference` below).
  const pointerTargets = new Map<object, unknown>();
  const seen = new Set<object>();
  const collect = (node: unknown): void => {
    if (typeof node !== 'object' || node === null || seen.has(node)) {
      return;
    }
    seen.add(node);
    const record = node as Record<string, unknown>;
    if (!predicted.has(node) && typeof record.$ref === 'string') {
      pointerTargets.set(node, record.$ref === '#' ? root : resolveByPointer({ '': root }, record.$ref));
    }
    Object.values(record).forEach(collect);
  };
  collect(root);
  for (const [node, set] of roles) {
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
  // The validator resolves references on the normalised schema. A reference that lands
  // anywhere else than where the roles were read from, for example on a key that
  // normalisation removed, is refused rather than read with the wrong roles.
  // The validator only resolves references in places it reads as schemas. A `$ref` inside an
  // object it never reached that way (say a component named `type`, entered only through a
  // `$ref`) has no resolved target yet; the validator looks it up when it gets there, with the
  // same lookup, and throws if it is missing, so that case is left to it.
  const lookup = dereference(root);
  for (const [node, targets] of predicted) {
    const record = node as Record<string, unknown>;
    const actual = referenceTargets(record, lookup);
    if (actual.length === 1 && actual[0] === undefined && typeof record.__absolute_ref__ !== 'string') {
      continue;
    }
    if (actual.length !== targets.length || actual.some((target, index) => !Object.is(target, targets[index]))) {
      throw new Error(REF_PLACEMENT_MESSAGE);
    }
  }
  // The same check for every other `$ref` the dereference resolved: its target, by the
  // validator's URI, must be the object its JSON Pointer named before normalisation (the one
  // the keyword scan checked).
  for (const [node, expected] of pointerTargets) {
    const record = node as Record<string, unknown>;
    if (typeof record.__absolute_ref__ !== 'string') {
      continue;
    }
    // A reference the validator could not resolve throws when the validator reads it.
    const [actual] = referenceTargets(record, lookup);
    if (actual !== undefined && !Object.is(actual, expected)) {
      throw new Error(REF_PLACEMENT_MESSAGE);
    }
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
 * - `id` is dropped: only draft-04 used it, Ajv refused a schema holding it,
 *   and the patched validator ignores it.
 * Name maps and literal data are left alone, so a property named `nullable`
 * or `format` keeps its schema, and a `$ref` target is read like any other
 * subschema (see `normalizeSchemaObjects`).
 */
function toValidationSchema(schema: JSONSchema7, dereference: (schema: Schema) => Lookup): Schema {
  // The copy is checked, so the scan and the validator read the same snapshot.
  const copy = structuredClone(schema) as Schema;
  refuseUnsupportedKeywords(copy);
  refuseUnresolvedReferences(copy);
  normalizeSchemaObjects(copy, dereference);
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
      if (!ValidatorClass || !dereferenceSchema) {
        throw new Error(`JSON Schema validator unavailable (${validatorUnavailableReason})`);
      }
      const validationSchema = toValidationSchema(this.#schema, dereferenceSchema);
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
