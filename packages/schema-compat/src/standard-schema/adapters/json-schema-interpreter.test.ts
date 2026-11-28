import { beforeAll, describe, expect, it } from 'vitest';

import {
  InterpretedJsonSchema,
  JSON_SCHEMA_NON_RETRYABLE,
  jsonSchemaValidatorLoading,
  jsonSchemaValidatorReady,
  startInterpreterLoad,
} from './json-schema-interpreter';

// The interpreter is the adapter's path where code generation from strings is disallowed. This
// process allows it, so these tests build the interpreter directly; json-schema-no-codegen.test.ts
// covers the adapter choosing it under the flag.
beforeAll(async () => {
  await startInterpreterLoad();
});

const validate = (schema: unknown, value: unknown = {}) => new InterpretedJsonSchema(schema).validate(value);
const refused = (keyword: string, pointer: string, reason: string) => ({
  issues: [{ message: `Schema validation error: ${keyword} at "${pointer}" ${reason}, so the schema is refused` }],
});
const notJson = (pointer: string, reason: string) => refused('schema', pointer, `${reason}, which JSON cannot carry`);
const valueNotJson = (path: Array<string | number>, reason: string) => ({
  issues: [{ message: `The value ${reason}, which JSON cannot carry, so it is refused.`, path }],
});
type Case = [schema: unknown, keyword: string, pointer: string];

describe('JSON Schema interpreter (code generation disallowed)', () => {
  it('validates an undefined property as absent and returns the value unchanged', () => {
    const value = { ok: true, error: undefined };
    const result = validate(
      {
        type: 'object',
        properties: { ok: { type: 'boolean' }, error: { type: 'string' } },
        required: ['ok'],
        additionalProperties: false,
      },
      value,
    );

    expect(result).toEqual({ value });
    expect('value' in result && result.value).toBe(value);
  });

  it('validates a recursive union in time that does not double with each level', () => {
    const child = { $ref: '#/$defs/node' };
    const byConst = () => ({
      anyOf: [
        { type: 'object', properties: { child: { ...child }, kind: { const: 'a' } } },
        { type: 'object', properties: { child: { ...child }, kind: { const: 'b' } } },
      ],
    });
    const byRequired = () => ({
      oneOf: [
        { type: 'object', required: ['text'], properties: { child: { ...child } } },
        { type: 'object', required: ['label'], properties: { child: { ...child } } },
      ],
    });
    // Checking each branch's child again at every level took 2^depth steps: 13-25 s at depth 20.
    for (const [node, leaf] of [
      [byConst, { kind: 'b' }],
      [byRequired, { label: 'x' }],
    ] as const) {
      const schema = new InterpretedJsonSchema({ $defs: { node: node() }, $ref: '#/$defs/node' });
      let valid: Record<string, unknown> = leaf;
      let invalid: Record<string, unknown> = { kind: 'c' };
      for (let depth = 0; depth < 40; depth++) {
        valid = { ...leaf, child: valid };
        invalid = { ...leaf, child: invalid };
      }

      expect(schema.validate(valid)).toEqual({ value: valid });
      expect('issues' in schema.validate(invalid)).toBe(true);
    }
  });

  it('still rejects an own __proto__ key that the schema does not allow', () => {
    const schema = { type: 'object', properties: { a: { type: 'string' } }, additionalProperties: false };

    expect(validate(schema, { a: 'x' })).toEqual({ value: { a: 'x' } });
    expect(validate(schema, JSON.parse('{"a":"x","__proto__":{}}')).issues?.map(issue => issue.message)).toEqual([
      'Property "__proto__" does not match additional properties schema.',
    ]);
  });

  it('applies keywords beside $ref in a schema without $schema, as Ajv did', () => {
    const schema = new InterpretedJsonSchema({
      type: 'object',
      properties: { id: { $ref: '#/definitions/Id', maxLength: 3 } },
      definitions: { Id: { type: 'string' } },
    });

    expect('issues' in schema.validate({ id: 'toolong' })).toBe(true);
    expect(schema.validate({ id: 'abc' })).toEqual({ value: { id: 'abc' } });
  });

  it('reports an extra property once, and still reports a property declared false', () => {
    const schema = new InterpretedJsonSchema({
      type: 'object',
      properties: { ok: { type: 'boolean' }, banned: false },
      additionalProperties: false,
    });

    expect(schema.validate({ ok: true, extra: 1 })).toEqual({
      issues: [{ message: 'Property "extra" does not match additional properties schema.', path: [] }],
    });
    expect(schema.validate({ banned: 1, extra: 1 })).toEqual({
      issues: [
        { message: 'False boolean schema.', path: ['banned'] },
        { message: 'Property "extra" does not match additional properties schema.', path: [] },
      ],
    });
  });

  describe('allow-list', () => {
    it('refuses a keyword outside the list, wherever a schema sits', () => {
      const s2020 = 'https://json-schema.org/draft/2020-12/schema';
      const cases: Case[] = [
        [{ if: { type: 'string' }, then: { minLength: 2 } }, 'if', '/if'],
        [
          { properties: { a: { patternProperties: { '^x': {} } } } },
          'patternProperties',
          '/properties/a/patternProperties',
        ],
        // Fork main's Ajv checked `not` without `allErrors`, which lost a required "" there.
        [{ anyOf: [{ not: { type: 'string' } }] }, 'not', '/anyOf/0/not'],
        [{ $defs: { A: { $anchor: 'a' } } }, '$anchor', '/$defs/A/$anchor'],
        [{ properties: { a: { type: 'string', id: 'x' } } }, 'id', '/properties/a/id'],
        // Fork main's Ajv compared these differently: a float step, and duplicates of "__proto__".
        [{ type: 'integer', multipleOf: 1 }, 'multipleOf', '/multipleOf'],
        [{ items: { type: 'string' }, uniqueItems: true }, 'uniqueItems', '/uniqueItems'],
        // Under 2020-12 too: Ajv2020 marked evaluated items and properties its own way.
        [{ $schema: s2020, contains: true }, 'contains', '/contains'],
        [{ $schema: s2020, unevaluatedProperties: false }, 'unevaluatedProperties', '/unevaluatedProperties'],
        [{ $schema: s2020, dependentSchemas: { a: true } }, 'dependentSchemas', '/dependentSchemas'],
      ];
      for (const [schema, keyword, pointer] of cases) {
        expect(validate(schema)).toEqual(refused(keyword, pointer, 'is not a supported keyword'));
      }
    });

    it('refuses a listed keyword holding a value of the wrong type', () => {
      const cases: Case[] = [
        [{ type: 'number', minimum: 5, exclusiveMinimum: true }, 'exclusiveMinimum', '/exclusiveMinimum'],
        [{ minLength: 1.5 }, 'minLength', '/minLength'],
        [{ required: true }, 'required', '/required'],
        [
          { $schema: 'https://json-schema.org/draft/2020-12/schema', dependentRequired: { x: null } },
          'dependentRequired',
          '/dependentRequired',
        ],
        [{ enum: [] }, 'enum', '/enum'],
      ];
      for (const [schema, keyword, pointer] of cases) {
        expect(validate(schema, 5)).toEqual(refused(keyword, pointer, 'has a value of the wrong type'));
      }
    });

    it('refuses a $schema other than draft-07 or 2020-12, a 2020-12 keyword without it, and an $id below the root', () => {
      expect(validate({ $schema: 'https://json-schema.org/draft/2019-09/schema', type: 'string' }, 'a')).toEqual(
        refused('$schema', '/$schema', 'is not a draft-07 or 2020-12 $schema at the root'),
      );
      expect(validate({ type: 'array', prefixItems: [{ type: 'string' }] }, ['a'])).toEqual(
        refused('prefixItems', '/prefixItems', 'is only supported under a 2020-12 $schema'),
      );
      const nestedId = {
        $schema: 'https://json-schema.org/draft/2020-12/schema',
        properties: { a: { $id: 'https://example.test/a', type: 'string' } },
      };
      expect(validate(nestedId)).toEqual(refused('$id', '/properties/a/$id', 'is only supported at the root'));
    });

    it('accepts a root $id only in canonical http or https form', () => {
      const s2020 = 'https://json-schema.org/draft/2020-12/schema';
      const notCanonical = (id: string) =>
        expect(validate({ $schema: s2020, $id: id })).toEqual(
          refused('$id', '/$id', 'is not an http or https ID in canonical form'),
        );
      expect(validate({ $schema: s2020, $id: 'https://example.test/schemas/a.json' })).toEqual({ value: {} });
      expect(validate({ $schema: s2020, $id: 'http://example.test:8080/' })).toEqual({ value: {} });
      // Fork main's Ajv threw on some of these and resolved others; none is accepted here.
      for (const id of [
        'urn:x',
        'urn:example:x',
        'tree',
        'https://example.test/s#frag',
        'https://example.test/a?b',
        'https://Example.test/a',
        'https://example.test:443/a',
        'https://example.test/a/../b',
        'https://example.test/%7e',
        ' https://example.test/a',
        s2020,
      ]) {
        notCanonical(id);
      }
    });

    it("refuses what fork main's Ajv threw on while compiling a listed keyword", () => {
      const s2020 = 'https://json-schema.org/draft/2020-12/schema';
      expect(
        validate({
          enum: [
            { a: 1, b: 2 },
            { b: 2, a: 1 },
          ],
        }),
      ).toEqual(refused('enum', '/enum', 'repeats a member, which the draft-07 metaschema refuses'));
      expect(validate({ $schema: s2020, enum: [0, 0] }, 0)).toEqual({ value: 0 });
      const nested = (keyword: string, pointer: string) =>
        refused(keyword, pointer, 'is below the root; nested IDs and anchors are not supported');
      // Ajv registered every ID and anchor its reference walk entered below the root, in
      // annotations too; none is accepted here, whether or not Ajv threw on it.
      expect(validate({ 'x-a': { $anchor: '!' } })).toEqual(nested('$anchor', '/x-a/$anchor'));
      expect(validate({ 'x-a': { $anchor: 'a' } })).toEqual(nested('$anchor', '/x-a/$anchor'));
      expect(validate({ $schema: s2020, 'x-a': { $dynamicAnchor: 'a' } })).toEqual(
        nested('$dynamicAnchor', '/x-a/$dynamicAnchor'),
      );
      expect(validate({ 'x-a': { $id: s2020 } })).toEqual(nested('$id', '/x-a/$id'));
      expect(validate({ $defs: { $id: true, A: true }, $ref: '#/$defs/A' })).toEqual(
        refused(
          '$defs',
          '/$defs/$id',
          'names a member "$id", which fork main\'s Ajv read as the scope of a "$ref" into "$defs"',
        ),
      );
      // Ajv walked no annotation under a skipped keyword.
      expect(validate({ default: { $anchor: '!', $id: 'https://example.test/s' } })).toEqual({ value: {} });
    });

    it('refuses schemas fork main refused or threw on, and accepts what it never compiled', () => {
      const wrongType = (keyword: string, pointer: string) =>
        refused(keyword, pointer, 'has a value of the wrong type');
      expect(validate({ required: ['a', 'a'] })).toEqual(wrongType('required', '/required'));
      expect(validate({ type: ['string', 'string'] }, 'a')).toEqual(wrongType('type', '/type'));
      // A keyword whose value is `undefined` is absent, as Ajv read it.
      expect(validate({ type: 'string', maxLength: undefined }, 'abc')).toEqual({ value: 'abc' });
      // Ajv's reference walk entered an array under a name Object.prototype holds.
      expect(validate({ example: { toString: [{ $anchor: 'a' }] } })).toEqual(
        refused(
          '$anchor',
          '/example/toString/0/$anchor',
          'is below the root; nested IDs and anchors are not supported',
        ),
      );
      expect(validate({ example: { toString: { $anchor: 'a' } } })).toEqual({ value: {} });
      // Fork main's Ajv never compiled a definition nothing reaches.
      expect(validate({ $defs: { A: { anyOf: [{ $ref: '#/$defs/A' }] } } })).toEqual({ value: {} });
      expect(validate({ $defs: { $id: true, A: { $ref: '#/$defs/B' }, B: true } })).toEqual({ value: {} });
    });

    it('validates a draft-07 tuple and ignores a nested $schema, as fork main did', () => {
      const tuple = () => ({
        type: 'array',
        items: [{ type: 'string' }, { type: 'integer' }],
        additionalItems: false,
      });
      expect(validate(tuple(), ['a', 1])).toEqual({ value: ['a', 1] });
      expect(validate(tuple(), ['a', 1, 2])).toHaveProperty('issues');
      expect(validate(tuple(), [1, 'a'])).toHaveProperty('issues');
      expect(validate({ items: [] }, [])).toEqual(refused('items', '/items', 'has a value of the wrong type'));
      expect(
        validate({ $schema: 'https://json-schema.org/draft/2020-12/schema', items: [{ type: 'string' }] }, ['a']),
      ).toEqual(refused('schema', '/items', 'is not a JSON object or boolean'));
      // A workflow's converted input schema keeps its own $schema; the root's dialect applies.
      const nested = () => ({
        type: 'object',
        properties: {
          inputData: { $schema: 'https://json-schema.org/draft/2019-09/schema', type: 'object', required: ['t'] },
        },
      });
      expect(validate(nested(), { inputData: { t: 1 } })).toEqual({ value: { inputData: { t: 1 } } });
      expect(validate(nested(), { inputData: {} })).toHaveProperty('issues');
      expect(validate({ properties: { a: { $schema: 5 } } })).toEqual(
        refused('$schema', '/properties/a/$schema', 'is not a draft-07 or 2020-12 $schema at the root'),
      );
    });

    it('validates a 2020-12 schema with the keywords fork main checked with Ajv2020', () => {
      const schema = new InterpretedJsonSchema({
        $schema: 'https://json-schema.org/draft/2020-12/schema',
        $id: 'https://example.test/schemas/tree',
        type: 'object',
        $defs: { leaf: { type: 'array', prefixItems: [{ type: 'string' }, { type: 'integer' }], items: false } },
        properties: { value: { anyOf: [{ $ref: '#/$defs/leaf' }, { type: 'null' }] } },
        required: ['value'],
        additionalProperties: false,
      });
      expect(schema.validate({ value: ['a', 1] })).toEqual({ value: { value: ['a', 1] } });
      expect(schema.validate({ value: ['a', 1, true] })).toHaveProperty('issues');
      expect(schema.validate({ value: null, extra: 1 })).toHaveProperty('issues');
    });

    it('refuses a schema position that is not a JSON object or a boolean', () => {
      const cases: Case[] = [
        [{ properties: { x: [] } }, 'schema', '/properties/x'],
        [{ additionalProperties: 'false' }, 'schema', '/additionalProperties'],
      ];
      for (const [schema, keyword, pointer] of cases) {
        expect(validate(schema)).toEqual(refused(keyword, pointer, 'is not a JSON object or boolean'));
      }
    });

    it('refuses a $ref other than "#" or a member of the root\'s $defs or definitions', () => {
      for (const ref of ['#/properties/a', '#/$defs/A#', '#/$defs/missing', '#/$defs/A/properties/p']) {
        const schema = { properties: { a: { type: 'string' }, b: { $ref: ref } }, $defs: { A: { type: 'string' } } };
        expect(validate(schema)).toEqual(
          refused('$ref', '/properties/b/$ref', 'is not "#" or a member of the root\'s "$defs" or "definitions"'),
        );
      }
    });

    it('refuses a loop of $ref, allOf, anyOf or oneOf that reads the same schema at the same value', () => {
      const reason = 'comes back to the same schema at the same value';
      expect(validate({ properties: { x: { $ref: '#/$defs/A' } }, $defs: { A: { $ref: '#/$defs/A' } } })).toEqual(
        refused('$ref', '/$defs/A/$ref', reason),
      );
      expect(validate({ anyOf: [{ type: 'null' }, { $ref: '#' }] }, null)).toEqual(
        refused('$ref', '/anyOf/1/$ref', reason),
      );
    });

    it('refuses names that Ajv read from the Object prototype', () => {
      expect(validate(JSON.parse('{"properties":{"__proto__":{"type":"string"}}}'))).toEqual(
        refused('properties', '/properties/__proto__', 'names a member like an Object prototype key'),
      );
      expect(validate({ required: ['toString'] })).toEqual(
        refused('required', '/required', 'names a property like an Object prototype key'),
      );
      // Ajv's generated code read every inherited member, not only the four its equality reads.
      expect(validate({ properties: { hasOwnProperty: false } })).toEqual(
        refused('properties', '/properties/hasOwnProperty', 'names a member like an Object prototype key'),
      );
      expect(validate({ oneOf: [true, { required: ['isPrototypeOf'] }] })).toEqual(
        refused('required', '/oneOf/1/required', 'names a property like an Object prototype key'),
      );
      expect(validate({ const: { constructor: {} } })).toEqual(
        refused('const', '/const', 'holds an object key like an Object prototype key'),
      );
    });

    it('reads a keyword whose value is undefined as absent, as Ajv did', () => {
      expect(validate({ type: 'object', properties: undefined, anyOf: undefined }, { a: 1 })).toEqual({
        value: { a: 1 },
      });
      expect(validate({ type: 'object', properties: undefined }, 1)).toMatchObject({ issues: [{ path: [] }] });
      expect(validate({ properties: { a: { const: undefined } } }, { a: 1 })).toEqual({ value: { a: 1 } });
      expect(validate({ properties: { a: { const: { b: undefined } } } }, { a: { b: 1 } })).toEqual(
        refused('const', '/properties/a/const', 'holds an undefined value'),
      );
    });

    it('refuses nullable without type, or false beside a null type, as Ajv did', () => {
      expect(validate({ nullable: true })).toEqual(refused('nullable', '/nullable', 'is used without "type"'));
      expect(validate({ type: ['string', 'null'], nullable: false })).toEqual(
        refused('nullable', '/nullable', 'is false beside a "null" type'),
      );
    });

    it('refuses an $id in an annotation that would move a $ref to another schema', () => {
      const schema = {
        properties: { a: { $ref: '#/$defs/A' } },
        $defs: { A: { type: 'string' } },
        'x-meta': { $id: '#/$defs/A', type: 'number' },
      };
      expect(validate(schema, { a: 1 })).toEqual(
        refused('$id', '/x-meta/$id', 'is below the root; nested IDs and anchors are not supported'),
      );
    });
  });

  describe('plain-JSON snapshot', () => {
    it('refuses a schema that is not plain JSON, naming where and why', () => {
      const shared = { type: 'string' };
      const cases: Array<[schema: unknown, pointer: string, reason: string]> = [
        [new Proxy({ type: 'string' }, {}), '', 'is a Proxy'],
        [{ properties: { a: { [Symbol('s')]: 1 } } }, '/properties/a', 'has a symbol key'],
        [Object.defineProperty({}, 'type', { value: 'string', enumerable: false }), '/type', 'is not enumerable'],
        [
          { properties: Object.defineProperty({}, 'a', { get: () => ({}), enumerable: true }) },
          '/properties/a',
          'is read through a getter or setter',
        ],
        [{ const: new Date(0) }, '/const', 'is not a plain object'],
        [{ enum: [Object.create(null)] }, '/enum/0', 'is not a plain object'],
        [Object.assign(Object.create(null), { type: 'string' }), '', 'is not a plain object'],
        [{ properties: { x: Object.create({ minLength: 2 }) } }, '/properties/x', 'is not a plain object'],
        [{ required: Array(1) }, '/required/0', 'is a hole in a sparse array'],
        [{ anyOf: [true, undefined] }, '/anyOf/1', 'is undefined outside an object member'],
        [{ maximum: Infinity }, '/maximum', 'is a number that is not finite'],
        [{ default: 1n }, '/default', 'is a bigint'],
        // One object reached twice: fork main's Ajv wrote to such a schema while compiling the first use.
        [
          { properties: { a: shared, b: shared } },
          '/properties/b',
          'is an object reached through more than one reference',
        ],
        [{ type: Object.freeze(['string']), nullable: true }, '/type', 'is frozen, sealed or not extensible'],
      ];
      for (const [schema, pointer, reason] of cases) {
        expect(validate(schema, 'a')).toEqual(notJson(pointer, reason));
      }
      const cyclic: Record<string, unknown> = {};
      cyclic.items = cyclic;
      expect(validate(cyclic, [])).toEqual(notJson('/items', 'is part of a cycle'));
    });

    it('refuses a value that is not plain JSON before checking it', () => {
      const open = { type: 'object', properties: { at: {} } };
      expect(validate(open, { at: new Date(0) })).toEqual(valueNotJson(['at'], 'is not a plain object'));
      expect(validate(open, { at: [1, Infinity] })).toEqual(valueNotJson(['at', 1], 'is a number that is not finite'));
      expect(validate(open, Object.create({ at: 1 }))).toEqual(valueNotJson([], 'is not a plain object'));
      expect(validate(open, { at: new Proxy({}, {}) })).toEqual(valueNotJson(['at'], 'is a Proxy'));
      expect(validate(open, { at: Object.defineProperty({}, 'b', { get: () => 1, enumerable: true }) })).toEqual(
        valueNotJson(['at', 'b'], 'is read through a getter or setter'),
      );
      expect(validate(open, { at: Object.assign(Object.create(null), { b: 1 }) })).toEqual(
        valueNotJson(['at'], 'is not a plain object'),
      );
      expect(validate(open, { at: Object.setPrototypeOf([1], class extends Array {}.prototype) })).toEqual(
        valueNotJson(['at'], 'is an array subclass'),
      );
      expect(validate(open, { at: Object.assign([], { 4294967295: 1 }) })).toEqual(
        valueNotJson(['at', '4294967295'], 'is an array key that is not an index'),
      );
      let deep: unknown = 1;
      for (let depth = 0; depth < 100_000; depth++) {
        deep = [deep];
      }
      expect(validate({}, deep)).toEqual(
        valueNotJson(Array<number>(1000).fill(0), 'is nested deeper than 1000 levels'),
      );
      // A key with a lone surrogate is plain JSON, but the validator cannot write it into an error location.
      expect(validate({ additionalProperties: false }, JSON.parse('{"\\ud800":1}'))).toEqual({
        issues: [{ message: 'The value could not be checked (URI malformed), so it is refused.' }],
      });
      // A frozen value is plain JSON: neither validator writes to a value.
      const frozen = Object.freeze({ at: Object.freeze([1]) });
      expect(validate(open, frozen)).toEqual({ value: frozen });
    });

    it('refuses a key that Ajv compared through the prototype, under a schema with const or enum', () => {
      expect(validate({ properties: { a: { const: { p: 1 } } } }, JSON.parse('{"a":{"valueOf":1}}'))).toEqual({
        issues: [
          {
            message:
              'The value holds a key that Ajv compared through the prototype (constructor, toString, valueOf or __proto__) under a schema with const or enum, so it is refused.',
            path: ['a', 'valueOf'],
          },
        ],
      });
      expect(validate({ properties: { a: {} } }, JSON.parse('{"a":{"valueOf":1}}'))).toEqual({
        value: { a: { valueOf: 1 } },
      });
    });

    it('checks an object reached through many references once', () => {
      let shared: Record<string, unknown> = {};
      for (let depth = 0; depth < 40; depth++) {
        shared = { a: shared, b: shared };
      }
      expect(validate({ type: 'object' }, shared)).toEqual({ value: shared });
      // Under `const`, every key of the value is scanned for prototype names, also once per object.
      const schema = { type: 'object', properties: { status: { const: 'sent' } } };
      expect(validate(schema, { status: 'sent', meta: shared })).toEqual({ value: { status: 'sent', meta: shared } });
    });

    it('reads the schema once, when it is wrapped, so a later change does not reach validation', () => {
      const schema = { type: 'object', properties: { a: { type: 'string' } } };
      const interpreted = new InterpretedJsonSchema(schema);
      schema.properties.a.type = 'number';

      expect(interpreted.validate({ a: 'x' })).toEqual({ value: { a: 'x' } });
      expect(interpreted.validate({ a: 1 })).toHaveProperty('issues');
    });

    it('reports Ajv-compatible issue paths for propertyNames, additionalProperties, additionalItems, and prefixItems+items:false', () => {
      const paths = (schema: unknown, value: unknown) => {
        const res = validate(schema, value);
        if (!('issues' in res)) throw new Error('expected issues');
        return [...new Set(res.issues.map(i => JSON.stringify(i.path ?? [])))].sort();
      };

      expect(paths({ type: 'object', propertyNames: { maxLength: 1 } }, { long: null })).toEqual(['[]']);
      expect(
        paths(
          { type: 'object', properties: { sub: { type: 'object', propertyNames: { pattern: '^[a-z]$' } } } },
          {
            sub: { BAD: null },
          },
        ),
      ).toEqual(['["sub"]']);
      expect(paths({ type: 'object', propertyNames: false }, { a: null })).toEqual(['[]']);
      expect(paths({ type: 'object', $defs: { F: false }, propertyNames: { $ref: '#/$defs/F' } }, { a: null })).toEqual(
        ['[]'],
      );
      expect(
        paths({ type: 'object', properties: { a: { type: 'string' } }, additionalProperties: false }, { extra: null }),
      ).toEqual(['[]']);
      expect(
        paths(
          { type: 'object', properties: { a: { type: 'string' } }, additionalProperties: { type: 'string' } },
          { extra: null },
        ),
      ).toEqual(['["extra"]']);
      expect(
        paths({ type: 'object', $defs: { F: false }, additionalProperties: { $ref: '#/$defs/F' } }, { extra: null }),
      ).toEqual(['["extra"]']);
      expect(paths({ type: 'array', items: [{ type: 'string' }], additionalItems: false }, ['ok', null])).toEqual([
        '[]',
      ]);
      expect(
        paths(
          {
            $schema: 'https://json-schema.org/draft/2020-12/schema',
            type: 'array',
            prefixItems: [{ type: 'string' }],
            items: false,
          },
          ['ok', null],
        ),
      ).toEqual(['[]']);
      expect(paths({ type: 'array', items: false }, [null])).toEqual(['[0]']);
    });

    it('marks schema refusals, non-plain values, prototype-key refusals, and validator throws as non-retryable', () => {
      const isNonRetryable = (res: ReturnType<typeof validate>) =>
        'issues' in res &&
        res.issues.some(i => (i as { [JSON_SCHEMA_NON_RETRYABLE]?: boolean })[JSON_SCHEMA_NON_RETRYABLE] === true);

      expect(isNonRetryable(validate({ not: {} }, 'a'))).toBe(true);
      expect(isNonRetryable(validate({ type: 'object' }, { n: 1n }))).toBe(true);
      expect(isNonRetryable(validate({ const: {} }, JSON.parse('{"valueOf":1}')))).toBe(true);
      expect(
        isNonRetryable(
          validate(
            { type: 'object', properties: { x: { type: 'string' } }, additionalProperties: false },
            {
              x: null,
              '\uD800': null,
            },
          ),
        ),
      ).toBe(true);
      // Ordinary schema violations stay retryable (e.g. null-stripping for optional fields).
      expect(isNonRetryable(validate({ type: 'object', properties: { x: { type: 'string' } } }, { x: null }))).toBe(
        false,
      );
    });
  });

  describe('refusal', () => {
    it('names why a schema is refused, and nothing for a schema it compiles', () => {
      expect(new InterpretedJsonSchema({ type: 'string' }).refusal()).toBeUndefined();
      expect(new InterpretedJsonSchema({ not: {} }).refusal()).toBe(
        'not at "/not" is not a supported keyword, so the schema is refused',
      );
      expect(new InterpretedJsonSchema({ type: 'string' }, 'is given ajvOptions').refusal()).toBe(
        'schema at "" is given ajvOptions, so the schema is refused',
      );
    });
  });

  describe('jsonSchemaValidatorReady', () => {
    it('also waits for the validator load of another adapter copy in the process', async () => {
      // A bundled or second-version copy of this adapter registers its load in the same global set
      // and removes it once it settles.
      const loads = (globalThis as Record<symbol, Set<Promise<void>>>)[
        Symbol.for('mastra.schemaCompat.jsonSchemaValidatorLoads')
      ]!;
      let finishOtherCopy!: () => void;
      const otherCopyLoad = new Promise<void>(resolve => {
        finishOtherCopy = resolve;
      });
      loads.add(otherCopyLoad);
      void otherCopyLoad.then(() => {
        loads.delete(otherCopyLoad);
      });
      try {
        let ready = false;
        const waiting = jsonSchemaValidatorReady().then(() => {
          ready = true;
        });
        await new Promise(resolve => setTimeout(resolve, 0));
        expect(ready).toBe(false);
        expect(jsonSchemaValidatorLoading()).toBe(true);

        finishOtherCopy();
        await waiting;
        expect(ready).toBe(true);
        expect(jsonSchemaValidatorLoading()).toBe(false);
      } finally {
        loads.delete(otherCopyLoad);
      }
    });
  });
});
