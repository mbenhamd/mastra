/**
 * Fail-closed JSON Schema ↔ Zod bridge for dynamic workflows.
 *
 * Persisted schemas must belong to one admitted dialect and a positive
 * keyword/value-shape contract per node. Unknown or unimplemented
 * behavior-bearing keywords are rejected at write time with JSON Pointer
 * evidence so rehydration cannot silently widen validation.
 */
import { z } from 'zod';
import { rememberAdmittedJsonSchema } from './admitted-schema-source';

/**
 * Minimal JSON-Schema shape we accept. Intentionally untyped on the value side
 * — different JSON Schema producers emit slightly different shapes and the
 * converter inspects admitted fields only after validation.
 */
export type JsonSchema = Record<string, any>;

/** Canonical dialect this converter admits. Nested `$schema` is rejected. */
export const ADMITTED_JSON_SCHEMA_DIALECT = 'https://json-schema.org/draft/2020-12/schema';

/**
 * `$schema` URIs that may label an admitted document. Draft-07 is accepted as
 * a URI alias for historical Zod `standardSchemaToJSONSchema` output; the
 * keyword contract is still the 2020-12 subset (tuple-form `items` arrays
 * remain rejected).
 */
const ADMITTED_DIALECT_URIS = new Set([
  ADMITTED_JSON_SCHEMA_DIALECT,
  `${ADMITTED_JSON_SCHEMA_DIALECT}#`,
  'http://json-schema.org/draft-07/schema#',
  'http://json-schema.org/draft-07/schema',
  'https://json-schema.org/draft-07/schema#',
  'https://json-schema.org/draft-07/schema',
]);

export const ADMITTED_JSON_SCHEMA_BOUNDS = {
  maxDepth: 16,
  maxNodes: 256,
  maxProperties: 64,
  maxEnumSize: 128,
  maxSerializedBytes: 32 * 1024,
  maxPrefixItems: 16,
} as const;

const ANNOTATION_KEYS = ['title', 'description', '$comment', 'examples', 'default'] as const;
const STRING_FORMATS = new Set(['uuid']);
const UUID_FORMAT_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const JSON_TYPES = new Set(['object', 'array', 'string', 'number', 'integer', 'boolean', 'null']);

const KEYS_BY_TYPE: Record<string, ReadonlySet<string>> = {
  string: new Set([...ANNOTATION_KEYS, 'type', 'enum', 'const', 'minLength', 'maxLength', 'format']),
  number: new Set([
    ...ANNOTATION_KEYS,
    'type',
    'enum',
    'const',
    'minimum',
    'maximum',
    'exclusiveMinimum',
    'exclusiveMaximum',
    'multipleOf',
  ]),
  integer: new Set([
    ...ANNOTATION_KEYS,
    'type',
    'enum',
    'const',
    'minimum',
    'maximum',
    'exclusiveMinimum',
    'exclusiveMaximum',
    'multipleOf',
  ]),
  boolean: new Set([...ANNOTATION_KEYS, 'type', 'enum', 'const']),
  null: new Set([...ANNOTATION_KEYS, 'type', 'enum', 'const']),
  object: new Set([
    ...ANNOTATION_KEYS,
    'type',
    'enum',
    'const',
    'properties',
    'required',
    'additionalProperties',
    'minProperties',
    'maxProperties',
    'propertyNames',
  ]),
  array: new Set([
    ...ANNOTATION_KEYS,
    'type',
    'enum',
    'const',
    'items',
    'prefixItems',
    'minItems',
    'maxItems',
    'uniqueItems',
    'contains',
    'minContains',
    'maxContains',
  ]),
};

/** Values `z.literal()` can represent losslessly. */
function isLiteralValue(v: unknown): v is string | number | boolean | null {
  return v === null || typeof v === 'string' || typeof v === 'boolean' || (typeof v === 'number' && Number.isFinite(v));
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

function isJsonValue(value: unknown): boolean {
  return canonicalJson(value) !== undefined;
}

function pointerSegment(segment: string): string {
  return segment.replace(/~/g, '~0').replace(/\//g, '~1');
}

function childPointer(pointer: string, segment: string): string {
  return `${pointer === '#' ? '#' : pointer}/${pointerSegment(segment)}`;
}

export type JsonSchemaAdmissionIssue = {
  pointer: string;
  keyword: string;
  message: string;
  path?: string;
};

export type StorableJsonSchemaValidation = { ok: true } | { ok: false; issues: JsonSchemaAdmissionIssue[] };

export type QuarantinedDynamicWorkflow = {
  id: string;
  reason: 'unsupported-schema';
  issues: JsonSchemaAdmissionIssue[];
};

export class UnsupportedJsonSchemaError extends Error {
  readonly issues: JsonSchemaAdmissionIssue[];

  constructor(issues: JsonSchemaAdmissionIssue[]) {
    const detail = issues.map(issue => `${issue.pointer} (${issue.keyword})`).join('; ');
    super(`JSON Schema is outside the admitted ${ADMITTED_JSON_SCHEMA_DIALECT} subset: ${detail || 'invalid schema'}.`);
    this.name = 'UnsupportedJsonSchemaError';
    this.issues = issues;
  }
}

type VisitCtx = {
  issues: JsonSchemaAdmissionIssue[];
  nodeCount: number;
};

function addIssue(ctx: VisitCtx, pointer: string, keyword: string, message: string): void {
  ctx.issues.push({ pointer, keyword, message });
}

function allowedKeysFor(types: string[], isRoot: boolean): Set<string> {
  const keys = new Set<string>(['type', 'enum', 'const', ...ANNOTATION_KEYS]);
  if (isRoot) keys.add('$schema');
  for (const type of types) {
    const extra = KEYS_BY_TYPE[type];
    if (extra) {
      for (const key of extra) keys.add(key);
    }
  }
  return keys;
}

function jsonTypeOf(value: unknown): string {
  if (value === null) return 'null';
  if (Array.isArray(value)) return 'array';
  if (typeof value === 'number') return Number.isInteger(value) ? 'integer' : 'number';
  return typeof value;
}

function constMatchesTypes(value: unknown, types: string[]): boolean {
  if (types.length === 0) return true;
  const actual = jsonTypeOf(value);
  return types.some(type => type === actual || (type === 'number' && actual === 'integer'));
}

function visit(node: unknown, pointer: string, depth: number, ctx: VisitCtx): void {
  ctx.nodeCount += 1;
  if (ctx.nodeCount > ADMITTED_JSON_SCHEMA_BOUNDS.maxNodes) {
    if (ctx.nodeCount === ADMITTED_JSON_SCHEMA_BOUNDS.maxNodes + 1) {
      addIssue(ctx, pointer, 'nodes', `schema exceeds ${ADMITTED_JSON_SCHEMA_BOUNDS.maxNodes} nodes`);
    }
    return;
  }
  if (depth > ADMITTED_JSON_SCHEMA_BOUNDS.maxDepth) {
    addIssue(ctx, pointer, 'depth', `schema exceeds max depth ${ADMITTED_JSON_SCHEMA_BOUNDS.maxDepth}`);
    return;
  }

  if (typeof node === 'boolean') {
    addIssue(
      ctx,
      pointer,
      'boolean-schema',
      'boolean JSON Schemas (true/false) are not admitted; they would rehydrate as unconstrained or never',
    );
    return;
  }
  if (!isPlainObject(node)) {
    addIssue(ctx, pointer, 'type', 'schema node must be a JSON object');
    return;
  }

  const schema = node;
  const isRoot = pointer === '#';

  if ('$schema' in schema) {
    if (!isRoot) {
      addIssue(ctx, childPointer(pointer, '$schema'), '$schema', '$schema is only admitted at the document root');
    } else if (typeof schema.$schema !== 'string' || !ADMITTED_DIALECT_URIS.has(schema.$schema)) {
      addIssue(
        ctx,
        childPointer(pointer, '$schema'),
        '$schema',
        `$schema must be ${ADMITTED_JSON_SCHEMA_DIALECT} (draft-07 URIs are accepted as aliases)`,
      );
    }
  }

  let types: string[] = [];
  if (schema.type === undefined) {
    types = [];
  } else if (typeof schema.type === 'string') {
    types = [schema.type];
  } else if (Array.isArray(schema.type)) {
    if (schema.type.length === 0) {
      addIssue(ctx, childPointer(pointer, 'type'), 'type', 'type arrays must be non-empty');
    }
    const seen = new Set<string>();
    for (const [i, entry] of schema.type.entries()) {
      if (typeof entry !== 'string' || !JSON_TYPES.has(entry)) {
        addIssue(
          ctx,
          childPointer(pointer, 'type') + `/${i}`,
          'type',
          `unsupported JSON Schema type ${JSON.stringify(entry)}`,
        );
        continue;
      }
      if (seen.has(entry)) {
        addIssue(ctx, childPointer(pointer, 'type') + `/${i}`, 'type', `duplicate type ${JSON.stringify(entry)}`);
        continue;
      }
      seen.add(entry);
      types.push(entry);
    }
  } else {
    addIssue(ctx, childPointer(pointer, 'type'), 'type', 'type must be a string or array of strings');
  }

  for (const type of types) {
    if (!JSON_TYPES.has(type)) {
      addIssue(ctx, childPointer(pointer, 'type'), 'type', `unsupported JSON Schema type ${JSON.stringify(type)}`);
    }
  }

  const hasEnum = 'enum' in schema;
  const hasConst = 'const' in schema;
  if (types.length === 0 && !hasEnum && !hasConst) {
    addIssue(
      ctx,
      pointer,
      'type',
      'schema must declare type, enum, or const; untyped nodes would rehydrate unconstrained',
    );
  }

  const allowed = allowedKeysFor(types, isRoot);
  if (hasEnum) allowed.add('enum');
  if (hasConst) allowed.add('const');

  for (const key of Object.keys(schema)) {
    if (!allowed.has(key)) {
      addIssue(
        ctx,
        childPointer(pointer, key),
        key,
        `keyword "${key}" is not admitted on this node in ${ADMITTED_JSON_SCHEMA_DIALECT}`,
      );
    }
  }

  if (hasEnum && hasConst) {
    addIssue(
      ctx,
      childPointer(pointer, 'const'),
      'const',
      'const and enum cannot both be present; conversion would keep only const and drop enum',
    );
  }

  if (hasEnum || hasConst) {
    const reserved = new Set<string>(['type', 'enum', 'const', '$schema', ...ANNOTATION_KEYS]);
    for (const key of Object.keys(schema)) {
      if (allowed.has(key) && !reserved.has(key)) {
        addIssue(
          ctx,
          childPointer(pointer, key),
          key,
          `keyword "${key}" is not admitted next to enum/const; conversion would drop it and widen validation`,
        );
      }
    }
  }

  if (hasConst) {
    if (!isLiteralValue(schema.const)) {
      addIssue(
        ctx,
        childPointer(pointer, 'const'),
        'const',
        'only string, number, boolean, and null const values are admitted',
      );
    } else if (!constMatchesTypes(schema.const, types)) {
      addIssue(ctx, childPointer(pointer, 'const'), 'const', 'const value does not match declared type');
    }
  }

  if (hasEnum) {
    if (!Array.isArray(schema.enum) || schema.enum.length === 0) {
      addIssue(ctx, childPointer(pointer, 'enum'), 'enum', 'enum must be a non-empty array');
    } else if (schema.enum.length > ADMITTED_JSON_SCHEMA_BOUNDS.maxEnumSize) {
      addIssue(
        ctx,
        childPointer(pointer, 'enum'),
        'enum',
        `enum exceeds ${ADMITTED_JSON_SCHEMA_BOUNDS.maxEnumSize} members`,
      );
    } else {
      for (const [i, member] of schema.enum.entries()) {
        if (!isLiteralValue(member)) {
          addIssue(
            ctx,
            `${childPointer(pointer, 'enum')}/${i}`,
            'enum',
            'only string, number, boolean, and null enum members are admitted',
          );
        } else if (!constMatchesTypes(member, types)) {
          addIssue(ctx, `${childPointer(pointer, 'enum')}/${i}`, 'enum', 'enum member does not match declared type');
        }
      }
    }
  }

  if ('title' in schema && typeof schema.title !== 'string') {
    addIssue(ctx, childPointer(pointer, 'title'), 'title', 'title must be a string');
  }
  if ('description' in schema && typeof schema.description !== 'string') {
    addIssue(ctx, childPointer(pointer, 'description'), 'description', 'description must be a string');
  }
  if ('$comment' in schema && typeof schema.$comment !== 'string') {
    addIssue(ctx, childPointer(pointer, '$comment'), '$comment', '$comment must be a string');
  }
  if ('examples' in schema && (!Array.isArray(schema.examples) || !schema.examples.every(isJsonValue))) {
    addIssue(ctx, childPointer(pointer, 'examples'), 'examples', 'examples must be an array of JSON values');
  }
  if ('default' in schema && !isJsonValue(schema.default)) {
    addIssue(
      ctx,
      childPointer(pointer, 'default'),
      'default',
      'default must be a JSON value (annotation only; not applied at parse)',
    );
  }

  if (types.includes('string')) visitStringKeywords(schema, pointer, ctx);
  if (types.includes('number') || types.includes('integer')) visitNumberKeywords(schema, pointer, ctx);
  if (types.includes('object')) visitObjectKeywords(schema, pointer, ctx);
  if (types.includes('array')) visitArrayKeywords(schema, pointer, ctx);
  visitChildSchemas(schema, pointer, depth, types, ctx);
}

function visitChildSchemas(
  schema: Record<string, unknown>,
  pointer: string,
  depth: number,
  types: string[],
  ctx: VisitCtx,
): void {
  const next = depth + 1;
  if (types.includes('object')) {
    if (isPlainObject(schema.properties)) {
      for (const [key, child] of Object.entries(schema.properties)) {
        visit(child, `${childPointer(pointer, 'properties')}/${pointerSegment(key)}`, next, ctx);
      }
    }
    if (isPlainObject(schema.additionalProperties)) {
      visit(schema.additionalProperties, childPointer(pointer, 'additionalProperties'), next, ctx);
    }
    if (isPlainObject(schema.propertyNames)) {
      visit(schema.propertyNames, childPointer(pointer, 'propertyNames'), next, ctx);
    }
  }
  if (types.includes('array')) {
    if (isPlainObject(schema.items)) {
      visit(schema.items, childPointer(pointer, 'items'), next, ctx);
    }
    if (Array.isArray(schema.prefixItems)) {
      schema.prefixItems.forEach((item, i) => visit(item, `${childPointer(pointer, 'prefixItems')}/${i}`, next, ctx));
    }
    if (isPlainObject(schema.contains)) {
      visit(schema.contains, childPointer(pointer, 'contains'), next, ctx);
    }
  }
}

function visitStringKeywords(schema: Record<string, unknown>, pointer: string, ctx: VisitCtx): void {
  if ('minLength' in schema && (!Number.isInteger(schema.minLength) || (schema.minLength as number) < 0)) {
    addIssue(ctx, childPointer(pointer, 'minLength'), 'minLength', 'minLength must be a non-negative integer');
  }
  if ('maxLength' in schema && (!Number.isInteger(schema.maxLength) || (schema.maxLength as number) < 0)) {
    addIssue(ctx, childPointer(pointer, 'maxLength'), 'maxLength', 'maxLength must be a non-negative integer');
  }
  if (
    typeof schema.minLength === 'number' &&
    typeof schema.maxLength === 'number' &&
    schema.minLength > schema.maxLength
  ) {
    addIssue(ctx, childPointer(pointer, 'minLength'), 'minLength', 'minLength must be <= maxLength');
  }
  if ('format' in schema) {
    if (typeof schema.format !== 'string' || !STRING_FORMATS.has(schema.format)) {
      addIssue(
        ctx,
        childPointer(pointer, 'format'),
        'format',
        `format ${JSON.stringify(schema.format)} is not admitted; admitted formats: ${[...STRING_FORMATS].join(', ')}`,
      );
    }
  }
}

function visitNumberKeywords(schema: Record<string, unknown>, pointer: string, ctx: VisitCtx): void {
  for (const key of ['minimum', 'maximum', 'exclusiveMinimum', 'exclusiveMaximum', 'multipleOf'] as const) {
    if (key in schema && (typeof schema[key] !== 'number' || !Number.isFinite(schema[key]))) {
      addIssue(
        ctx,
        childPointer(pointer, key),
        key,
        `${key} must be a finite number (draft-4 booleans are not admitted)`,
      );
    }
  }
  if (typeof schema.multipleOf === 'number' && schema.multipleOf <= 0) {
    addIssue(ctx, childPointer(pointer, 'multipleOf'), 'multipleOf', 'multipleOf must be > 0');
  }
  if (typeof schema.minimum === 'number' && typeof schema.maximum === 'number' && schema.minimum > schema.maximum) {
    addIssue(ctx, childPointer(pointer, 'minimum'), 'minimum', 'minimum must be <= maximum');
  }
}

function visitObjectKeywords(schema: Record<string, unknown>, pointer: string, ctx: VisitCtx): void {
  const properties = schema.properties;
  const propertyKeys = new Set<string>();
  if (properties !== undefined) {
    if (!isPlainObject(properties)) {
      addIssue(ctx, childPointer(pointer, 'properties'), 'properties', 'properties must be an object');
    } else {
      const keys = Object.keys(properties);
      if (keys.length > ADMITTED_JSON_SCHEMA_BOUNDS.maxProperties) {
        addIssue(
          ctx,
          childPointer(pointer, 'properties'),
          'properties',
          `object exceeds ${ADMITTED_JSON_SCHEMA_BOUNDS.maxProperties} properties`,
        );
      }
      for (const key of keys) {
        if (key === '__proto__') {
          addIssue(
            ctx,
            `${childPointer(pointer, 'properties')}/${pointerSegment(key)}`,
            'properties',
            'property name "__proto__" is not admitted',
          );
        }
        propertyKeys.add(key);
      }
    }
  }

  if ('required' in schema) {
    if (!Array.isArray(schema.required) || schema.required.some(item => typeof item !== 'string')) {
      addIssue(ctx, childPointer(pointer, 'required'), 'required', 'required must be an array of strings');
    } else {
      const seen = new Set<string>();
      for (const [i, key] of schema.required.entries()) {
        if (typeof key !== 'string') continue;
        if (seen.has(key)) {
          addIssue(
            ctx,
            `${childPointer(pointer, 'required')}/${i}`,
            'required',
            `duplicate required key ${JSON.stringify(key)}`,
          );
        }
        seen.add(key);
        if (!propertyKeys.has(key)) {
          addIssue(
            ctx,
            `${childPointer(pointer, 'required')}/${i}`,
            'required',
            `required key ${JSON.stringify(key)} is not declared in properties`,
          );
        }
      }
    }
  }

  if ('additionalProperties' in schema) {
    if (typeof schema.additionalProperties === 'boolean' || isPlainObject(schema.additionalProperties)) {
      // boolean or schema; schema children are visited by visitChildSchemas
    } else {
      addIssue(
        ctx,
        childPointer(pointer, 'additionalProperties'),
        'additionalProperties',
        'additionalProperties must be a boolean or an admitted schema',
      );
    }
  }

  for (const key of ['minProperties', 'maxProperties'] as const) {
    if (key in schema && (!Number.isInteger(schema[key]) || (schema[key] as number) < 0)) {
      addIssue(ctx, childPointer(pointer, key), key, `${key} must be a non-negative integer`);
    }
  }
  if (
    typeof schema.minProperties === 'number' &&
    typeof schema.maxProperties === 'number' &&
    schema.minProperties > schema.maxProperties
  ) {
    addIssue(ctx, childPointer(pointer, 'minProperties'), 'minProperties', 'minProperties must be <= maxProperties');
  }

  if ('propertyNames' in schema) {
    if (typeof schema.propertyNames === 'boolean' || isPlainObject(schema.propertyNames)) {
      // boolean or schema; schema children are visited by visitChildSchemas
    } else {
      addIssue(
        ctx,
        childPointer(pointer, 'propertyNames'),
        'propertyNames',
        'propertyNames must be a boolean or an admitted schema',
      );
    }
  }
}

function visitArrayKeywords(schema: Record<string, unknown>, pointer: string, ctx: VisitCtx): void {
  const hasPrefix = 'prefixItems' in schema;
  const hasItems = 'items' in schema;

  if (hasPrefix) {
    if (!Array.isArray(schema.prefixItems) || schema.prefixItems.length === 0) {
      addIssue(
        ctx,
        childPointer(pointer, 'prefixItems'),
        'prefixItems',
        'prefixItems must be a non-empty array of schemas',
      );
    } else if (schema.prefixItems.length > ADMITTED_JSON_SCHEMA_BOUNDS.maxPrefixItems) {
      addIssue(
        ctx,
        childPointer(pointer, 'prefixItems'),
        'prefixItems',
        `prefixItems exceeds ${ADMITTED_JSON_SCHEMA_BOUNDS.maxPrefixItems} entries`,
      );
    }
    if (schema.items !== false) {
      addIssue(
        ctx,
        childPointer(pointer, 'items'),
        'items',
        'tuples require items: false (closed). Rest/open tuples are not admitted',
      );
    }
    const prefixLen = Array.isArray(schema.prefixItems) ? schema.prefixItems.length : 0;
    if (schema.minItems !== prefixLen) {
      addIssue(
        ctx,
        childPointer(pointer, 'minItems'),
        'minItems',
        'closed tuples must set minItems equal to prefixItems.length',
      );
    }
    if (typeof schema.maxItems === 'number' && schema.maxItems !== prefixLen) {
      addIssue(
        ctx,
        childPointer(pointer, 'maxItems'),
        'maxItems',
        'closed tuples must set maxItems equal to prefixItems.length when present',
      );
    }
  } else if (!hasItems) {
    addIssue(
      ctx,
      pointer,
      'items',
      'arrays must declare items (homogeneous) or prefixItems with items: false (closed tuple); unconstrained items are not admitted',
    );
  } else if (Array.isArray(schema.items)) {
    addIssue(
      ctx,
      childPointer(pointer, 'items'),
      'items',
      'draft-07 tuple-form items arrays are not admitted; use prefixItems with items: false',
    );
  } else if (typeof schema.items === 'boolean') {
    addIssue(
      ctx,
      childPointer(pointer, 'items'),
      'items',
      'boolean items is not admitted; unconstrained array elements would rehydrate as z.any()',
    );
  } else if (!isPlainObject(schema.items)) {
    addIssue(ctx, childPointer(pointer, 'items'), 'items', 'items must be an admitted schema object');
  }

  for (const key of ['minItems', 'maxItems'] as const) {
    if (key in schema && (!Number.isInteger(schema[key]) || (schema[key] as number) < 0)) {
      addIssue(ctx, childPointer(pointer, key), key, `${key} must be a non-negative integer`);
    }
  }
  if (typeof schema.minItems === 'number' && typeof schema.maxItems === 'number' && schema.minItems > schema.maxItems) {
    addIssue(ctx, childPointer(pointer, 'minItems'), 'minItems', 'minItems must be <= maxItems');
  }
  if ('uniqueItems' in schema && typeof schema.uniqueItems !== 'boolean') {
    addIssue(ctx, childPointer(pointer, 'uniqueItems'), 'uniqueItems', 'uniqueItems must be a boolean');
  }

  if ('contains' in schema) {
    if (typeof schema.contains === 'boolean') {
      addIssue(ctx, childPointer(pointer, 'contains'), 'contains', 'boolean contains is not admitted');
    } else if (!isPlainObject(schema.contains)) {
      addIssue(ctx, childPointer(pointer, 'contains'), 'contains', 'contains must be an admitted schema object');
    }
  } else if ('minContains' in schema || 'maxContains' in schema) {
    addIssue(ctx, pointer, 'contains', 'minContains/maxContains require contains');
  }
  for (const key of ['minContains', 'maxContains'] as const) {
    if (key in schema && (!Number.isInteger(schema[key]) || (schema[key] as number) < 0)) {
      addIssue(ctx, childPointer(pointer, key), key, `${key} must be a non-negative integer`);
    }
  }
  if (
    typeof schema.minContains === 'number' &&
    typeof schema.maxContains === 'number' &&
    schema.minContains > schema.maxContains
  ) {
    addIssue(ctx, childPointer(pointer, 'minContains'), 'minContains', 'minContains must be <= maxContains');
  }
}

/**
 * Non-throwing write-path admission check. Walks a JSON Schema against the
 * admitted dialect and reports every offense with a JSON Pointer. Never
 * throws for any input shape. `undefined` means "no schema present".
 */
export function validateStorableJsonSchema(schema: JsonSchema | undefined): StorableJsonSchemaValidation {
  if (schema === undefined) return { ok: true };
  const ctx: VisitCtx = { issues: [], nodeCount: 0 };
  try {
    const serialized = JSON.stringify(schema);
    if (new TextEncoder().encode(serialized).byteLength > ADMITTED_JSON_SCHEMA_BOUNDS.maxSerializedBytes) {
      addIssue(ctx, '#', 'bytes', `schema exceeds ${ADMITTED_JSON_SCHEMA_BOUNDS.maxSerializedBytes} serialized bytes`);
      return { ok: false, issues: ctx.issues };
    }
  } catch {
    addIssue(ctx, '#', 'bytes', 'schema is not JSON-serializable');
    return { ok: false, issues: ctx.issues };
  }
  try {
    visit(schema, '#', 1, ctx);
  } catch {
    addIssue(ctx, '#', 'schema', 'schema inspection failed');
  }
  return ctx.issues.length === 0 ? { ok: true } : { ok: false, issues: ctx.issues };
}

function applyAnnotations(out: z.ZodTypeAny, schema: JsonSchema): z.ZodTypeAny {
  if (typeof schema.description === 'string' && schema.description.length > 0) {
    out = out.describe(schema.description);
  }
  const meta: Record<string, unknown> = {};
  if (typeof schema.title === 'string') meta.title = schema.title;
  if (typeof schema.$comment === 'string') meta.$comment = schema.$comment;
  if (Array.isArray(schema.examples)) meta.examples = schema.examples;
  if ('default' in schema) meta.default = schema.default;
  const annotationTarget = out as unknown as {
    meta?: (value: Record<string, unknown>) => z.ZodTypeAny;
  };
  if (Object.keys(meta).length > 0 && typeof annotationTarget.meta === 'function') {
    out = annotationTarget.meta(meta);
  }
  return out;
}

function convert(schema: JsonSchema): z.ZodTypeAny {
  if ('const' in schema) {
    return applyAnnotations(z.literal(schema.const as string | number | boolean | null), schema);
  }

  if (Array.isArray(schema.enum) && schema.enum.length > 0) {
    const values = schema.enum as Array<string | number | boolean | null>;
    if (values.every(v => typeof v === 'string')) {
      const stringValues = values as string[];
      const first = stringValues[0];
      if (first === undefined) {
        throw new Error('admitted enum unexpectedly empty during convert');
      }
      const out = stringValues.length === 1 ? z.literal(first) : z.enum(stringValues as [string, ...string[]]);
      return applyAnnotations(out, schema);
    }
    const literals = values.map(v => z.literal(v));
    const first = literals[0];
    if (!first) throw new Error('admitted enum unexpectedly empty during convert');
    const out =
      literals.length === 1 ? first : z.union(literals as unknown as [z.ZodTypeAny, z.ZodTypeAny, ...z.ZodTypeAny[]]);
    return applyAnnotations(out, schema);
  }

  if (Array.isArray(schema.type)) {
    const options = schema.type.map((t: string) => convert({ ...schema, type: t }));
    const first = options[0];
    if (!first) throw new Error('admitted type array unexpectedly empty during convert');
    const out =
      options.length === 1 ? first : z.union(options as unknown as [z.ZodTypeAny, z.ZodTypeAny, ...z.ZodTypeAny[]]);
    return applyAnnotations(out, schema);
  }

  let out: z.ZodTypeAny;
  switch (schema.type) {
    case 'object':
      out = convertObject(schema);
      break;
    case 'array':
      out = convertArray(schema);
      break;
    case 'string':
      out = convertString(schema);
      break;
    case 'number':
    case 'integer':
      out = convertNumber(schema);
      break;
    case 'boolean':
      out = z.boolean();
      break;
    case 'null':
      out = z.null();
      break;
    default:
      throw new Error(`admitted schema missing convert branch for type ${String(schema.type)}`);
  }
  return applyAnnotations(out, schema);
}

function convertString(schema: JsonSchema): z.ZodTypeAny {
  let str = z.string();
  if (schema.format === 'uuid') str = str.regex(UUID_FORMAT_PATTERN, 'invalid UUID');
  // Capture admitted bounds now. Refinements must not retain the caller's
  // mutable schema object and drift from the cloned schema we persist.
  const minLength = typeof schema.minLength === 'number' ? schema.minLength : undefined;
  const maxLength = typeof schema.maxLength === 'number' ? schema.maxLength : undefined;
  if (minLength === undefined && maxLength === undefined) return str;

  return str.superRefine((value, ctx) => {
    let length = 0;
    for (const _codePoint of value) {
      length += 1;
      // Once maxLength is violated, the exact remaining length is irrelevant.
      if (maxLength !== undefined && length > maxLength) break;
    }
    if (minLength !== undefined && length < minLength) {
      ctx.addIssue({ code: 'custom', message: `must contain at least ${minLength} characters` });
    }
    if (maxLength !== undefined && length > maxLength) {
      ctx.addIssue({ code: 'custom', message: `must contain at most ${maxLength} characters` });
    }
  });
}

function convertNumber(schema: JsonSchema): z.ZodTypeAny {
  let num = z.number();
  if (typeof schema.exclusiveMinimum === 'number') num = num.gt(schema.exclusiveMinimum);
  if (typeof schema.minimum === 'number') num = num.gte(schema.minimum);
  if (typeof schema.exclusiveMaximum === 'number') num = num.lt(schema.exclusiveMaximum);
  if (typeof schema.maximum === 'number') num = num.lte(schema.maximum);

  let out: z.ZodTypeAny = num.refine(Number.isFinite, { message: 'must be a finite JSON number' });
  if (schema.type === 'integer') {
    // Zod 4's `.int()` restricts values to safe integers while Zod 3 admits
    // every finite mathematical integer. JSON Schema uses the latter meaning.
    out = out.refine(value => typeof value === 'number' && Number.isInteger(value), { message: 'must be an integer' });
  }
  if (typeof schema.multipleOf === 'number') {
    const divisor = schema.multipleOf;
    // Zod 3's `.multipleOf()` rejects valid subnormal/scientific-notation
    // multiples. Compare the exact base-10 values represented by the two JS
    // numbers so every supported peer enforces the same JSON-number contract.
    out = out.refine(value => typeof value === 'number' && isDecimalMultiple(value, divisor), {
      message: `must be a multiple of ${divisor}`,
    });
  }
  return out;
}

function decimalParts(value: number): { coefficient: bigint; exponent: number } {
  const [mantissa = '0', exponentText] = Math.abs(value).toString().toLowerCase().split('e');
  const dot = mantissa.indexOf('.');
  const fractionDigits = dot === -1 ? 0 : mantissa.length - dot - 1;
  const digits = dot === -1 ? mantissa : `${mantissa.slice(0, dot)}${mantissa.slice(dot + 1)}`;
  return {
    coefficient: BigInt(digits),
    exponent: (exponentText === undefined ? 0 : Number(exponentText)) - fractionDigits,
  };
}

function isDecimalMultiple(value: number, divisor: number): boolean {
  if (!Number.isFinite(value)) return false;
  if (value === 0) return true;
  const dividendParts = decimalParts(value);
  const divisorParts = decimalParts(divisor);
  const exponentDelta = dividendParts.exponent - divisorParts.exponent;
  if (exponentDelta >= 0) {
    const scaledDividend = dividendParts.coefficient * 10n ** BigInt(exponentDelta);
    return scaledDividend % divisorParts.coefficient === 0n;
  }
  const scaledDivisor = divisorParts.coefficient * 10n ** BigInt(-exponentDelta);
  return dividendParts.coefficient % scaledDivisor === 0n;
}

function convertObject(schema: JsonSchema): z.ZodTypeAny {
  const shape = Object.create(null) as Record<string, z.ZodTypeAny>;
  const required = new Set<string>(Array.isArray(schema.required) ? schema.required : []);
  for (const [key, child] of Object.entries((schema.properties ?? {}) as Record<string, JsonSchema>)) {
    const childSchema = convert(child);
    Object.defineProperty(shape, key, {
      value: required.has(key) ? childSchema : childSchema.optional(),
      enumerable: true,
      configurable: true,
      writable: true,
    });
  }

  const additional = schema.additionalProperties;
  let obj: z.ZodTypeAny;
  const zodCompat = z as unknown as {
    looseObject?: (shape: Record<string, z.ZodTypeAny>) => z.ZodTypeAny;
  };
  if (additional === false) {
    obj = z.object(shape).strict();
  } else if (additional && typeof additional === 'object') {
    obj = z.object(shape).catchall(convert(additional as JsonSchema));
  } else if (typeof zodCompat.looseObject === 'function') {
    // omitted or true — JSON Schema 2020-12 default additionalProperties: true
    obj = zodCompat.looseObject(shape);
  } else {
    // Zod 3 peer: passthrough is the admitted extra-key equivalent of looseObject.
    obj = z.object(shape).passthrough();
  }

  // Every supported Zod version silently removes an own `__proto__` key before
  // object refinements run. The dialect cannot then validate or preserve that
  // JSON member, so reject it on the original input instead of widening strict,
  // catchall, propertyNames, or property-count semantics.
  const objectWithSafeKeys = z
    .unknown()
    .superRefine((value, ctx) => {
      if (isPlainObject(value) && Object.prototype.hasOwnProperty.call(value, '__proto__')) {
        ctx.addIssue({ code: 'custom', message: 'property name "__proto__" is not admitted' });
      }
    })
    .pipe(obj);

  const minProperties = schema.minProperties;
  const maxProperties = schema.maxProperties;
  const propertyNames = schema.propertyNames;
  if (typeof minProperties !== 'number' && typeof maxProperties !== 'number' && propertyNames === undefined) {
    return objectWithSafeKeys;
  }

  const namesZod =
    propertyNames === undefined
      ? undefined
      : propertyNames === false
        ? z.never()
        : propertyNames === true
          ? z.string()
          : convert(propertyNames as JsonSchema);

  return objectWithSafeKeys.superRefine((value, ctx) => {
    if (!isPlainObject(value)) return;
    const keys = Object.keys(value);
    if (typeof minProperties === 'number' && keys.length < minProperties) {
      ctx.addIssue({ code: 'custom', message: `must have at least ${minProperties} properties` });
    }
    if (typeof maxProperties === 'number' && keys.length > maxProperties) {
      ctx.addIssue({ code: 'custom', message: `must have at most ${maxProperties} properties` });
    }
    if (namesZod) {
      for (const key of keys) {
        if (!namesZod.safeParse(key).success) {
          ctx.addIssue({ code: 'custom', message: `property name ${JSON.stringify(key)} is invalid` });
        }
      }
    }
  });
}

type CanonicalJsonAction =
  | { kind: 'value'; value: unknown }
  | { kind: 'text'; value: string }
  | { kind: 'leave'; value: object };

/**
 * Stable, non-recursive JSON encoding for `uniqueItems` equality. Returns
 * undefined for cyclic or non-JSON values instead of throwing from a Zod
 * refinement.
 */
function canonicalJson(value: unknown): string | undefined {
  try {
    return canonicalJsonUnchecked(value);
  } catch {
    return undefined;
  }
}

function canonicalJsonUnchecked(value: unknown): string | undefined {
  const active = new WeakSet<object>();
  const parts: string[] = [];
  const stack: CanonicalJsonAction[] = [{ kind: 'value', value }];

  while (stack.length > 0) {
    const action = stack.pop();
    if (!action) break;
    if (action.kind === 'text') {
      parts.push(action.value);
      continue;
    }
    if (action.kind === 'leave') {
      active.delete(action.value);
      continue;
    }

    const current = action.value;
    if (current === null) {
      parts.push('null');
      continue;
    }
    if (typeof current === 'string' || typeof current === 'boolean') {
      parts.push(JSON.stringify(current));
      continue;
    }
    if (typeof current === 'number') {
      if (!Number.isFinite(current)) return undefined;
      parts.push(JSON.stringify(current));
      continue;
    }
    if (typeof current !== 'object' || (!Array.isArray(current) && !isPlainObject(current))) {
      return undefined;
    }
    if (active.has(current)) return undefined;
    active.add(current);
    stack.push({ kind: 'leave', value: current });

    if (Array.isArray(current)) {
      stack.push({ kind: 'text', value: ']' });
      for (let i = current.length - 1; i >= 0; i -= 1) {
        stack.push({ kind: 'value', value: current[i] });
        if (i > 0) stack.push({ kind: 'text', value: ',' });
      }
      stack.push({ kind: 'text', value: '[' });
      continue;
    }

    const keys = Object.keys(current).sort();
    stack.push({ kind: 'text', value: '}' });
    for (let i = keys.length - 1; i >= 0; i -= 1) {
      const key = keys[i];
      if (key === undefined) continue;
      stack.push({ kind: 'value', value: (current as Record<string, unknown>)[key] });
      stack.push({ kind: 'text', value: ':' });
      stack.push({ kind: 'text', value: JSON.stringify(key) });
      if (i > 0) stack.push({ kind: 'text', value: ',' });
    }
    stack.push({ kind: 'text', value: '{' });
  }

  return parts.join('');
}

function convertArray(schema: JsonSchema): z.ZodTypeAny {
  let arr: z.ZodTypeAny;
  if (Array.isArray(schema.prefixItems)) {
    const prefix = (schema.prefixItems as JsonSchema[]).map(item => convert(item));
    const first = prefix[0];
    if (!first) throw new Error('admitted prefixItems unexpectedly empty during convert');
    arr = z.tuple(prefix as unknown as [z.ZodTypeAny, ...z.ZodTypeAny[]]);
  } else {
    arr = z.array(convert((schema.items ?? {}) as JsonSchema));
    if (typeof schema.minItems === 'number') arr = (arr as z.ZodArray<z.ZodTypeAny>).min(schema.minItems);
    if (typeof schema.maxItems === 'number') arr = (arr as z.ZodArray<z.ZodTypeAny>).max(schema.maxItems);
  }

  if (schema.uniqueItems === true) {
    arr = arr.superRefine((value, ctx) => {
      if (!Array.isArray(value)) return;
      const seen = new Set<string>();
      for (const item of value) {
        const key = canonicalJson(item);
        if (key === undefined) {
          ctx.addIssue({ code: 'custom', message: 'items must be JSON values to evaluate uniqueness' });
          return;
        }
        if (seen.has(key)) {
          ctx.addIssue({ code: 'custom', message: 'items must be unique' });
          return;
        }
        seen.add(key);
      }
    });
  }

  if (schema.contains !== undefined) {
    const containsZod = convert(schema.contains as JsonSchema);
    const minContains = typeof schema.minContains === 'number' ? schema.minContains : 1;
    const maxContains = typeof schema.maxContains === 'number' ? schema.maxContains : undefined;
    arr = arr.superRefine((value, ctx) => {
      if (!Array.isArray(value)) return;
      let count = 0;
      for (const item of value) {
        if (containsZod.safeParse(item).success) count += 1;
      }
      if (count < minContains) {
        ctx.addIssue({ code: 'custom', message: `must contain at least ${minContains} matching item(s)` });
      }
      if (maxContains !== undefined && count > maxContains) {
        ctx.addIssue({ code: 'custom', message: `must contain at most ${maxContains} matching item(s)` });
      }
    });
  }

  return arr;
}

/**
 * Convert an admitted JSON Schema to Zod. Throws {@link UnsupportedJsonSchemaError}
 * when the schema is outside the dialect. Never returns `z.any()`.
 */
export function jsonSchemaToZod(schema: JsonSchema): z.ZodTypeAny {
  const result = validateStorableJsonSchema(schema);
  if (!result.ok) {
    throw new UnsupportedJsonSchemaError(result.issues);
  }
  return rememberAdmittedJsonSchema(convert(schema), schema);
}
