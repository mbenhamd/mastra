/**
 * Fail-closed JSON Schema ↔ Zod bridge for dynamic workflows.
 *
 * Persisted schemas must belong to one admitted dialect and a positive
 * keyword/value-shape contract per node. Unknown or unimplemented
 * behavior-bearing keywords are rejected at write time with JSON Pointer
 * evidence so rehydration cannot silently widen validation.
 */
import { z } from 'zod';

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
  maxPatternLength: 256,
  maxPrefixItems: 16,
} as const;

const ANNOTATION_KEYS = ['title', 'description', '$comment', 'examples', 'default'] as const;
const STRING_FORMATS = new Set(['email', 'uri', 'uuid', 'date-time']);
const JSON_TYPES = new Set(['object', 'array', 'string', 'number', 'integer', 'boolean', 'null']);

const KEYS_BY_TYPE: Record<string, ReadonlySet<string>> = {
  string: new Set([...ANNOTATION_KEYS, 'type', 'enum', 'const', 'minLength', 'maxLength', 'pattern', 'format']),
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
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function isJsonValue(value: unknown): boolean {
  if (value === null) return true;
  const t = typeof value;
  if (t === 'string' || t === 'boolean') return true;
  if (t === 'number') return Number.isFinite(value);
  if (t !== 'object') return false;
  if (Array.isArray(value)) return value.every(isJsonValue);
  const proto = Object.getPrototypeOf(value);
  if (proto !== Object.prototype && proto !== null) return false;
  return Object.values(value as Record<string, unknown>).every(isJsonValue);
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
  visitChildSchemas(schema, pointer, depth, ctx);
}

function visitChildSchemas(schema: Record<string, unknown>, pointer: string, depth: number, ctx: VisitCtx): void {
  const next = depth + 1;
  if (isPlainObject(schema.properties)) {
    for (const [key, child] of Object.entries(schema.properties)) {
      visit(child, `${childPointer(pointer, 'properties')}/${pointerSegment(key)}`, next, ctx);
    }
  }
  if (isPlainObject(schema.patternProperties)) {
    for (const [key, child] of Object.entries(schema.patternProperties)) {
      visit(child, `${childPointer(pointer, 'patternProperties')}/${pointerSegment(key)}`, next, ctx);
    }
  }
  if (isPlainObject(schema.additionalProperties)) {
    visit(schema.additionalProperties, childPointer(pointer, 'additionalProperties'), next, ctx);
  }
  if (isPlainObject(schema.propertyNames)) {
    visit(schema.propertyNames, childPointer(pointer, 'propertyNames'), next, ctx);
  }
  if (isPlainObject(schema.items)) {
    visit(schema.items, childPointer(pointer, 'items'), next, ctx);
  } else if (Array.isArray(schema.items)) {
    schema.items.forEach((item, i) => visit(item, `${childPointer(pointer, 'items')}/${i}`, next, ctx));
  }
  if (Array.isArray(schema.prefixItems)) {
    schema.prefixItems.forEach((item, i) => visit(item, `${childPointer(pointer, 'prefixItems')}/${i}`, next, ctx));
  }
  if (isPlainObject(schema.contains)) {
    visit(schema.contains, childPointer(pointer, 'contains'), next, ctx);
  }
  for (const combinator of ['oneOf', 'anyOf', 'allOf'] as const) {
    const value = schema[combinator];
    if (Array.isArray(value)) {
      value.forEach((item, i) => visit(item, `${childPointer(pointer, combinator)}/${i}`, next, ctx));
    }
  }
  for (const key of ['not', 'if', 'then', 'else'] as const) {
    if (isPlainObject(schema[key])) visit(schema[key], childPointer(pointer, key), next, ctx);
  }
  for (const defsKey of ['$defs', 'definitions'] as const) {
    if (isPlainObject(schema[defsKey])) {
      for (const [key, child] of Object.entries(schema[defsKey])) {
        visit(child, `${childPointer(pointer, defsKey)}/${pointerSegment(key)}`, next, ctx);
      }
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
  if ('pattern' in schema) {
    if (typeof schema.pattern !== 'string') {
      addIssue(ctx, childPointer(pointer, 'pattern'), 'pattern', 'pattern must be a string');
    } else if (schema.pattern.length > ADMITTED_JSON_SCHEMA_BOUNDS.maxPatternLength) {
      addIssue(
        ctx,
        childPointer(pointer, 'pattern'),
        'pattern',
        `pattern exceeds ${ADMITTED_JSON_SCHEMA_BOUNDS.maxPatternLength} characters`,
      );
    } else {
      try {
        new RegExp(schema.pattern);
      } catch {
        addIssue(
          ctx,
          childPointer(pointer, 'pattern'),
          'pattern',
          `pattern ${JSON.stringify(schema.pattern)} is not a valid regular expression`,
        );
      }
    }
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
      for (const key of keys) propertyKeys.add(key);
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
    if (typeof schema.minItems === 'number' && schema.minItems !== prefixLen) {
      addIssue(
        ctx,
        childPointer(pointer, 'minItems'),
        'minItems',
        'closed tuples must set minItems equal to prefixItems.length (or omit it)',
      );
    }
    if (typeof schema.maxItems === 'number' && schema.maxItems !== prefixLen) {
      addIssue(
        ctx,
        childPointer(pointer, 'maxItems'),
        'maxItems',
        'closed tuples must set maxItems equal to prefixItems.length (or omit it)',
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
  if (schema === undefined || schema === null) return { ok: true };
  const ctx: VisitCtx = { issues: [], nodeCount: 0 };
  try {
    const serialized = JSON.stringify(schema);
    if (serialized.length > ADMITTED_JSON_SCHEMA_BOUNDS.maxSerializedBytes) {
      addIssue(ctx, '#', 'bytes', `schema exceeds ${ADMITTED_JSON_SCHEMA_BOUNDS.maxSerializedBytes} serialized bytes`);
      return { ok: false, issues: ctx.issues };
    }
  } catch {
    addIssue(ctx, '#', 'bytes', 'schema is not JSON-serializable');
    return { ok: false, issues: ctx.issues };
  }
  visit(schema, '#', 1, ctx);
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
  if (Object.keys(meta).length > 0) {
    out = out.meta(meta);
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
  if (schema.format === 'email') str = str.email();
  else if (schema.format === 'uri') str = str.url();
  else if (schema.format === 'uuid') str = str.uuid();
  else if (schema.format === 'date-time') str = str.datetime();
  if (typeof schema.minLength === 'number') str = str.min(schema.minLength);
  if (typeof schema.maxLength === 'number') str = str.max(schema.maxLength);
  if (typeof schema.pattern === 'string') str = str.regex(new RegExp(schema.pattern));
  return str;
}

function convertNumber(schema: JsonSchema): z.ZodTypeAny {
  let num = z.number();
  if (schema.type === 'integer') num = num.int();
  if (typeof schema.exclusiveMinimum === 'number') num = num.gt(schema.exclusiveMinimum);
  if (typeof schema.minimum === 'number') num = num.gte(schema.minimum);
  if (typeof schema.exclusiveMaximum === 'number') num = num.lt(schema.exclusiveMaximum);
  if (typeof schema.maximum === 'number') num = num.lte(schema.maximum);
  if (typeof schema.multipleOf === 'number') num = num.multipleOf(schema.multipleOf);
  return num;
}

function convertObject(schema: JsonSchema): z.ZodTypeAny {
  const shape: Record<string, z.ZodTypeAny> = {};
  const required = new Set<string>(Array.isArray(schema.required) ? schema.required : []);
  for (const [key, child] of Object.entries((schema.properties ?? {}) as Record<string, JsonSchema>)) {
    const childSchema = convert(child);
    shape[key] = required.has(key) ? childSchema : childSchema.optional();
  }

  const additional = schema.additionalProperties;
  let obj: z.ZodTypeAny;
  if (additional === false) {
    obj = z.strictObject(shape);
  } else if (additional && typeof additional === 'object') {
    obj = z.object(shape).catchall(convert(additional as JsonSchema));
  } else {
    // omitted or true — JSON Schema 2020-12 default additionalProperties: true
    obj = z.looseObject(shape);
  }

  const minProperties = schema.minProperties;
  const maxProperties = schema.maxProperties;
  const propertyNames = schema.propertyNames;
  if (typeof minProperties !== 'number' && typeof maxProperties !== 'number' && propertyNames === undefined) {
    return obj;
  }

  const namesZod =
    propertyNames === undefined
      ? undefined
      : propertyNames === false
        ? z.never()
        : propertyNames === true
          ? z.string()
          : convert(propertyNames as JsonSchema);

  return obj.superRefine((value, ctx) => {
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

function canonicalJson(value: unknown): string {
  if (value === null || typeof value !== 'object') return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`;
  const keys = Object.keys(value as object).sort();
  return `{${keys.map(key => `${JSON.stringify(key)}:${canonicalJson((value as Record<string, unknown>)[key])}`).join(',')}}`;
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
  return convert(schema);
}
