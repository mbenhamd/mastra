import type { JSONSchema7 } from 'json-schema';
import { describe, it, expect } from 'vitest';
import { isStandardSchemaWithJSON } from '../standard-schema';
import { jsonSchemaValidatorLoading, jsonSchemaValidatorReady, toStandardSchema } from './json-schema';

describe('json-schema standard-schema adapter', () => {
  describe('toStandardSchema', () => {
    it('should wrap a JSON Schema with StandardSchemaV1 interface', () => {
      const jsonSchema: JSONSchema7 = {
        type: 'object',
        properties: {
          name: { type: 'string' },
          age: { type: 'number' },
        },
        required: ['name', 'age'],
      };

      const standardSchema = toStandardSchema(jsonSchema);

      // Should have ~standard property
      expect('~standard' in standardSchema).toBe(true);
      expect(standardSchema['~standard'].version).toBe(1);
      expect(standardSchema['~standard'].vendor).toBe('json-schema');
    });

    it('should validate data correctly', async () => {
      const jsonSchema: JSONSchema7 = {
        type: 'object',
        properties: {
          name: { type: 'string' },
          age: { type: 'number', minimum: 0 },
        },
        required: ['name', 'age'],
      };

      const standardSchema = toStandardSchema<{ name: string; age: number }>(jsonSchema);

      // Test valid data
      const validResult = await standardSchema['~standard'].validate({ name: 'John', age: 30 });
      expect(validResult).toEqual({ value: { name: 'John', age: 30 } });

      // Test invalid data - missing required field
      const invalidResult = await standardSchema['~standard'].validate({ name: 'John' });
      expect('issues' in invalidResult).toBe(true);
      if ('issues' in invalidResult && invalidResult.issues) {
        expect(invalidResult.issues.length).toBeGreaterThan(0);
      }
    });

    it('should have jsonSchema converter', () => {
      const jsonSchema: JSONSchema7 = {
        type: 'object',
        properties: {
          name: { type: 'string' },
        },
      };

      const standardSchema = toStandardSchema(jsonSchema);

      // Should have jsonSchema property
      expect('jsonSchema' in standardSchema['~standard']).toBe(true);
      expect(typeof standardSchema['~standard'].jsonSchema.input).toBe('function');
      expect(typeof standardSchema['~standard'].jsonSchema.output).toBe('function');
    });

    it('should return JSON Schema with draft-07 target', () => {
      const jsonSchema: JSONSchema7 = {
        type: 'object',
        properties: {
          name: { type: 'string' },
          age: { type: 'number' },
        },
      };

      const standardSchema = toStandardSchema(jsonSchema);
      const outputSchema = standardSchema['~standard'].jsonSchema.output({ target: 'draft-07' });

      expect(outputSchema.type).toBe('object');
      expect(outputSchema.properties).toBeDefined();
      expect(outputSchema.$schema).toBe('http://json-schema.org/draft-07/schema#');
      expect(outputSchema).toMatchSnapshot();
    });

    it('should return JSON Schema with draft-2020-12 target', () => {
      const jsonSchema: JSONSchema7 = {
        type: 'string',
      };

      const standardSchema = toStandardSchema(jsonSchema);
      const outputSchema = standardSchema['~standard'].jsonSchema.output({ target: 'draft-2020-12' });

      expect(outputSchema.type).toBe('string');
      expect(outputSchema.$schema).toBe('https://json-schema.org/draft/2020-12/schema');
      expect(outputSchema).toMatchSnapshot();
    });

    it('should return JSON Schema with openapi-3.0 target (no $schema)', () => {
      const jsonSchema: JSONSchema7 = {
        type: 'string',
      };

      const standardSchema = toStandardSchema(jsonSchema);
      const outputSchema = standardSchema['~standard'].jsonSchema.output({ target: 'openapi-3.0' });

      expect(outputSchema.type).toBe('string');
      // OpenAPI 3.0 doesn't use $schema
      expect(outputSchema.$schema).toBeUndefined();
      expect(outputSchema).toMatchSnapshot();
    });

    it('should not overwrite existing $schema', () => {
      const jsonSchema: JSONSchema7 = {
        $schema: 'http://custom-schema.org/schema#',
        type: 'string',
      };

      const standardSchema = toStandardSchema(jsonSchema);
      const outputSchema = standardSchema['~standard'].jsonSchema.output({ target: 'draft-07' });

      expect(outputSchema.$schema).toBe('http://custom-schema.org/schema#');
      expect(outputSchema).toMatchSnapshot();
    });

    it('should handle nested object schemas', () => {
      const jsonSchema: JSONSchema7 = {
        type: 'object',
        properties: {
          user: {
            type: 'object',
            properties: {
              name: { type: 'string' },
              email: { type: 'string', format: 'email' },
            },
            required: ['name'],
          },
          tags: {
            type: 'array',
            items: { type: 'string' },
          },
        },
        required: ['user'],
      };

      const standardSchema = toStandardSchema(jsonSchema);
      const outputSchema = standardSchema['~standard'].jsonSchema.output({ target: 'draft-07' });

      expect(outputSchema.type).toBe('object');
      expect((outputSchema.properties as any).user.type).toBe('object');
      expect((outputSchema.properties as any).tags.type).toBe('array');
      expect(outputSchema).toMatchSnapshot();
    });

    it('should handle array schemas', () => {
      const jsonSchema: JSONSchema7 = {
        type: 'array',
        items: {
          type: 'object',
          properties: {
            id: { type: 'number' },
          },
          required: ['id'],
        },
        minItems: 1,
      };

      const standardSchema = toStandardSchema(jsonSchema);
      const outputSchema = standardSchema['~standard'].jsonSchema.output({ target: 'draft-07' });

      expect(outputSchema.type).toBe('array');
      expect(outputSchema.minItems).toBe(1);
    });

    it('should handle enum schemas', () => {
      const jsonSchema: JSONSchema7 = {
        type: 'string',
        enum: ['red', 'green', 'blue'],
      };

      const standardSchema = toStandardSchema(jsonSchema);
      const outputSchema = standardSchema['~standard'].jsonSchema.output({ target: 'draft-07' });

      expect(outputSchema.type).toBe('string');
      expect(outputSchema.enum).toEqual(['red', 'green', 'blue']);
    });

    it('should validate enum values correctly', async () => {
      const jsonSchema: JSONSchema7 = {
        type: 'string',
        enum: ['red', 'green', 'blue'],
      };

      const standardSchema = toStandardSchema<'red' | 'green' | 'blue'>(jsonSchema);

      const validResult = await standardSchema['~standard'].validate('red');
      expect(validResult).toEqual({ value: 'red' });

      const invalidResult = await standardSchema['~standard'].validate('yellow');
      expect('issues' in invalidResult).toBe(true);
    });

    it('validates an undefined property as absent and returns the value unchanged', async () => {
      const standardSchema = toStandardSchema<{ ok: boolean; error?: string }>({
        type: 'object',
        properties: { ok: { type: 'boolean' }, error: { type: 'string' } },
        required: ['ok'],
        additionalProperties: false,
      });
      const value = { ok: true, error: undefined };

      const result = await standardSchema['~standard'].validate(value);

      expect(result).toEqual({ value });
      expect('value' in result && result.value).toBe(value);
    });

    it('validates a recursive union in time that does not double with each level', async () => {
      const child = { $ref: '#/$defs/node' };
      // Branches told apart by a discriminator after the recursive property, and by `required` alone.
      const byConst = {
        anyOf: [
          { type: 'object', properties: { child, kind: { const: 'a' } } },
          { type: 'object', properties: { child, kind: { const: 'b' } } },
        ],
      };
      const byRequired = {
        oneOf: [
          { type: 'object', required: ['text'], properties: { child } },
          { type: 'object', required: ['label'], properties: { child } },
        ],
      };
      if (jsonSchemaValidatorLoading()) await jsonSchemaValidatorReady();
      // Checking each branch's child again at every level took 2^depth steps: 13-25 s at depth 20.
      for (const [node, leaf] of [
        [byConst, { kind: 'b' }],
        [byRequired, { label: 'x' }],
      ] as const) {
        const standardSchema = toStandardSchema({ $defs: { node }, $ref: '#/$defs/node' } as JSONSchema7);
        let valid: Record<string, unknown> = leaf;
        let invalid: Record<string, unknown> = { kind: 'c' };
        for (let depth = 0; depth < 40; depth++) {
          valid = { ...leaf, child: valid };
          invalid = { ...leaf, child: invalid };
        }

        expect(await standardSchema['~standard'].validate(valid)).toEqual({ value: valid });
        expect('issues' in (await standardSchema['~standard'].validate(invalid))).toBe(true);
      }

      // An "extends" chain: allOf reaches the child through the base and again directly.
      const extendsChain = toStandardSchema({
        $defs: {
          base: { type: 'object', properties: { id: { type: 'string' }, child: { $ref: '#/$defs/node' } } },
          node: { allOf: [{ $ref: '#/$defs/base' }, { properties: { child: { $ref: '#/$defs/node' } } }] },
        },
        $ref: '#/$defs/node',
      } as JSONSchema7);
      let valid: Record<string, unknown> = { id: 'leaf' };
      let invalid: Record<string, unknown> = { id: 5 };
      for (let depth = 0; depth < 40; depth++) {
        valid = { id: 'x', child: valid };
        invalid = { id: 'x', child: invalid };
      }

      expect(await extendsChain['~standard'].validate(valid)).toEqual({ value: valid });
      expect('issues' in (await extendsChain['~standard'].validate(invalid))).toBe(true);
    });

    it('still rejects an own __proto__ key that the schema does not allow', async () => {
      const standardSchema = toStandardSchema({
        type: 'object',
        properties: { a: { type: 'string' } },
        additionalProperties: false,
      });

      expect(await standardSchema['~standard'].validate({ a: 'x' })).toEqual({ value: { a: 'x' } });
      const result = await standardSchema['~standard'].validate(JSON.parse('{"a":"x","__proto__":{}}'));

      expect(result.issues?.map(issue => issue.message)).toEqual([
        'Property "__proto__" does not match additional properties schema.',
      ]);
    });

    it('should preserve recursive $ref schemas when converting output JSON Schema', () => {
      const jsonSchema = {
        type: 'object',
        properties: {
          root: { $ref: '#/$defs/node' },
        },
        required: ['root'],
        $defs: {
          node: {
            type: 'object',
            properties: {
              name: { type: 'string' },
              children: {
                type: 'array',
                items: { $ref: '#/$defs/node' },
              },
            },
            required: ['name'],
          },
        },
      } as JSONSchema7 & {
        properties: {
          root: { $ref: string };
        };
        $defs: {
          node: {
            properties: {
              children: {
                items: { $ref: string };
              };
            };
          };
        };
      };

      const standardSchema = toStandardSchema(jsonSchema);
      const outputSchema = standardSchema['~standard'].jsonSchema.output({
        target: 'draft-07',
      }) as unknown as typeof jsonSchema & {
        $schema?: string;
      };

      expect(outputSchema.$schema).toBe('http://json-schema.org/draft-07/schema#');
      expect(outputSchema.properties.root).toEqual({ $ref: '#/$defs/node' });
      expect(outputSchema.$defs.node.properties.children.items).toEqual({ $ref: '#/$defs/node' });
    });

    it('should expose getSchema method', () => {
      const jsonSchema: JSONSchema7 = {
        type: 'string',
      };

      const standardSchema = toStandardSchema(jsonSchema);

      expect(standardSchema.getSchema()).toEqual(jsonSchema);
    });
  });

  describe('keywords beside $ref', () => {
    it('applies keywords beside $ref in a schema without $schema, as Ajv did', async () => {
      const standardSchema = toStandardSchema({
        type: 'object',
        properties: { id: { $ref: '#/definitions/Id', maxLength: 3 } },
        definitions: { Id: { type: 'string' } },
      } as JSONSchema7);

      const tooLong = await standardSchema['~standard'].validate({ id: 'toolong' });
      expect('issues' in tooLong && tooLong.issues?.length).toBeGreaterThan(0);
      expect(await standardSchema['~standard'].validate({ id: 'abc' })).toEqual({ value: { id: 'abc' } });
    });
  });

  describe('allow-list', () => {
    const validate = async (schema: unknown, value: unknown = {}) => {
      const standardSchema = toStandardSchema(schema as JSONSchema7);
      if (jsonSchemaValidatorLoading()) await jsonSchemaValidatorReady();
      return standardSchema['~standard'].validate(value);
    };
    const refused = (keyword: string, pointer: string, reason: string) => ({
      issues: [{ message: `Schema validation error: ${keyword} at "${pointer}" ${reason}, so the schema is refused` }],
    });
    type Case = [schema: unknown, keyword: string, pointer: string];

    it('refuses a keyword outside the list, wherever a schema sits', async () => {
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
        [{ $schema: 'https://json-schema.org/draft/2020-12/schema', contains: true }, 'contains', '/contains'],
        [
          { $schema: 'https://json-schema.org/draft/2020-12/schema', unevaluatedProperties: false },
          'unevaluatedProperties',
          '/unevaluatedProperties',
        ],
        [
          { $schema: 'https://json-schema.org/draft/2020-12/schema', dependentSchemas: { a: true } },
          'dependentSchemas',
          '/dependentSchemas',
        ],
      ];
      for (const [schema, keyword, pointer] of cases) {
        expect(await validate(schema)).toEqual(refused(keyword, pointer, 'is not a supported keyword'));
      }
    });

    it('refuses a listed keyword whose value has the wrong type', async () => {
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
        expect(await validate(schema, 5)).toEqual(refused(keyword, pointer, 'has a value of the wrong type'));
      }
    });

    it('refuses a $schema other than draft-07 or 2020-12, a 2020-12 keyword without it, and an $id below the root', async () => {
      expect(await validate({ $schema: 'https://json-schema.org/draft/2019-09/schema', type: 'string' }, 'a')).toEqual(
        refused('$schema', '/$schema', 'is not a draft-07 or 2020-12 $schema at the root'),
      );
      expect(await validate({ type: 'array', prefixItems: [{ type: 'string' }] }, ['a'])).toEqual(
        refused('prefixItems', '/prefixItems', 'is only supported under a 2020-12 $schema'),
      );
      const nestedId = {
        $schema: 'https://json-schema.org/draft/2020-12/schema',
        properties: { a: { $id: 'https://example.test/a', type: 'string' } },
      };
      expect(await validate(nestedId)).toEqual(refused('$id', '/properties/a/$id', 'is only supported at the root'));
    });

    it("refuses what fork main's Ajv threw on while compiling a listed keyword", async () => {
      const s2020 = 'https://json-schema.org/draft/2020-12/schema';
      expect(await validate({ $schema: s2020, $id: s2020 })).toEqual(
        refused('$id', '/$id', "names a schema fork main's Ajv had already registered"),
      );
      expect(
        await validate({
          enum: [
            { a: 1, b: 2 },
            { b: 2, a: 1 },
          ],
        }),
      ).toEqual(refused('enum', '/enum', 'repeats a member, which the draft-07 metaschema refuses'));
      expect(await validate({ $schema: s2020, enum: [0, 0] }, 0)).toEqual({ value: 0 });
      const nested = (keyword: string, pointer: string) =>
        refused(keyword, pointer, 'is below the root; nested IDs and anchors are not supported');
      // Ajv registered every ID and anchor its reference walk entered below the root, in
      // annotations too; none is accepted here, whether or not Ajv threw on it.
      expect(await validate({ 'x-a': { $anchor: '!' } })).toEqual(nested('$anchor', '/x-a/$anchor'));
      expect(await validate({ 'x-a': { $anchor: 'a' } })).toEqual(nested('$anchor', '/x-a/$anchor'));
      expect(await validate({ $schema: s2020, 'x-a': { $dynamicAnchor: 'a' } })).toEqual(
        nested('$dynamicAnchor', '/x-a/$dynamicAnchor'),
      );
      expect(await validate({ 'x-a': { $id: s2020 } })).toEqual(nested('$id', '/x-a/$id'));
      expect(await validate({ $defs: { $id: true, A: true }, $ref: '#/$defs/A' })).toEqual(
        refused(
          '$defs',
          '/$defs/$id',
          'names a member "$id", which fork main\'s Ajv read as the scope of a "$ref" into "$defs"',
        ),
      );
      // Ajv walked no annotation under a skipped keyword.
      expect(await validate({ default: { $anchor: '!', $id: 'https://example.test/s' } })).toEqual({ value: {} });
    });

    it('refuses schemas fork main refused or threw on, and accepts what it never compiled', async () => {
      const s2020 = 'https://json-schema.org/draft/2020-12/schema';
      const wrongType = (keyword: string, pointer: string) =>
        refused(keyword, pointer, 'has a value of the wrong type');
      expect(await validate({ $schema: s2020, $id: 'urn:x' })).toEqual(wrongType('$id', '/$id'));
      expect(await validate({ $schema: s2020, $id: 'urn:example:x' })).toEqual({ value: {} });
      expect(await validate({ required: Array(1) })).toEqual(wrongType('required', '/required'));
      expect(await validate({ anyOf: [true, undefined] })).toEqual(wrongType('anyOf', '/anyOf'));
      expect(await validate({ enum: [1, undefined] }, 1)).toEqual(wrongType('enum', '/enum'));
      expect(await validate({ type: Object.freeze(['string']), nullable: true }, null)).toEqual(
        refused('nullable', '/nullable', 'is true beside a "type" list that cannot be extended'),
      );
      // A keyword whose value is `undefined` is absent, as Ajv read it.
      expect(await validate({ type: 'string', maxLength: undefined }, 'abc')).toEqual({ value: 'abc' });
      // Ajv's reference walk entered an array under a name Object.prototype holds.
      expect(await validate({ example: { toString: [{ $anchor: 'a' }] } })).toEqual(
        refused(
          '$anchor',
          '/example/toString/0/$anchor',
          'is below the root; nested IDs and anchors are not supported',
        ),
      );
      expect(await validate({ example: { toString: { $anchor: 'a' } } })).toEqual({ value: {} });
      // Fork main's Ajv never compiled a definition nothing reaches.
      expect(await validate({ $defs: { A: { anyOf: [{ $ref: '#/$defs/A' }] } } })).toEqual({ value: {} });
      expect(await validate({ $defs: { $id: true, A: { $ref: '#/$defs/B' }, B: true } })).toEqual({ value: {} });
    });

    it('validates a draft-07 tuple and ignores a nested $schema, as fork main did', async () => {
      const tuple = { type: 'array', items: [{ type: 'string' }, { type: 'integer' }], additionalItems: false };
      expect(await validate(tuple, ['a', 1])).toEqual({ value: ['a', 1] });
      expect(await validate(tuple, ['a', 1, 2])).toHaveProperty('issues');
      expect(await validate(tuple, [1, 'a'])).toHaveProperty('issues');
      expect(await validate({ items: [] }, [])).toEqual(refused('items', '/items', 'has a value of the wrong type'));
      expect(
        await validate({ $schema: 'https://json-schema.org/draft/2020-12/schema', items: [{ type: 'string' }] }, ['a']),
      ).toEqual(refused('schema', '/items', 'is not a JSON object or boolean'));
      // A workflow's converted input schema keeps its own $schema; the root's dialect applies.
      const nested = {
        type: 'object',
        properties: {
          inputData: { $schema: 'https://json-schema.org/draft/2019-09/schema', type: 'object', required: ['t'] },
        },
      };
      expect(await validate(nested, { inputData: { t: 1 } })).toEqual({ value: { inputData: { t: 1 } } });
      expect(await validate(nested, { inputData: {} })).toHaveProperty('issues');
      expect(await validate({ properties: { a: { $schema: 5 } } })).toEqual(
        refused('$schema', '/properties/a/$schema', 'is not a draft-07 or 2020-12 $schema at the root'),
      );
    });

    it('validates a 2020-12 schema with the keywords fork main checked with Ajv2020', async () => {
      const schema = {
        $schema: 'https://json-schema.org/draft/2020-12/schema',
        $id: 'https://example.test/schemas/tree',
        type: 'object',
        $defs: { leaf: { type: 'array', prefixItems: [{ type: 'string' }, { type: 'integer' }], items: false } },
        properties: { value: { anyOf: [{ $ref: '#/$defs/leaf' }, { type: 'null' }] } },
        required: ['value'],
        additionalProperties: false,
      };
      expect(await validate(schema, { value: ['a', 1] })).toEqual({ value: { value: ['a', 1] } });
      expect(await validate(schema, { value: ['a', 1, true] })).toHaveProperty('issues');
      expect(await validate(schema, { value: null, extra: 1 })).toHaveProperty('issues');
    });

    it('refuses a schema position that is not a JSON object or a boolean', async () => {
      const cases: Case[] = [
        [{ properties: { x: [] } }, 'schema', '/properties/x'],
        [{ properties: { x: Object.create({ minLength: 2 }) } }, 'schema', '/properties/x'],
        [{ additionalProperties: 'false' }, 'schema', '/additionalProperties'],
      ];
      for (const [schema, keyword, pointer] of cases) {
        expect(await validate(schema)).toEqual(refused(keyword, pointer, 'is not a JSON object or boolean'));
      }
    });

    it('refuses a $ref other than "#" or a member of the root\'s $defs or definitions', async () => {
      for (const ref of ['#/properties/a', '#/$defs/A#', '#/$defs/missing', '#/$defs/A/properties/p']) {
        const schema = { properties: { a: { type: 'string' }, b: { $ref: ref } }, $defs: { A: { type: 'string' } } };
        expect(await validate(schema)).toEqual(
          refused('$ref', '/properties/b/$ref', 'is not "#" or a member of the root\'s "$defs" or "definitions"'),
        );
      }
    });

    it('refuses a loop of $ref, allOf, anyOf or oneOf that reads the same schema at the same value', async () => {
      const reason = 'comes back to the same schema at the same value';
      expect(await validate({ properties: { x: { $ref: '#/$defs/A' } }, $defs: { A: { $ref: '#/$defs/A' } } })).toEqual(
        refused('$ref', '/$defs/A/$ref', reason),
      );
      expect(await validate({ anyOf: [{ type: 'null' }, { $ref: '#' }] }, null)).toEqual(
        refused('$ref', '/anyOf/1/$ref', reason),
      );
    });

    it('refuses names that Ajv read from the Object prototype', async () => {
      expect(await validate(JSON.parse('{"properties":{"__proto__":{"type":"string"}}}'))).toEqual(
        refused('properties', '/properties/__proto__', 'names a member like an Object prototype key'),
      );
      expect(await validate({ required: ['toString'] })).toEqual(
        refused('required', '/required', 'names a property like an Object prototype key'),
      );
      // Ajv's generated code read every inherited member, not only the four its equality reads.
      expect(await validate({ properties: { hasOwnProperty: false } })).toEqual(
        refused('properties', '/properties/hasOwnProperty', 'names a member like an Object prototype key'),
      );
      expect(await validate({ oneOf: [true, { required: ['isPrototypeOf'] }] })).toEqual(
        refused('required', '/oneOf/1/required', 'names a property like an Object prototype key'),
      );
      expect(await validate({ const: { constructor: {} } })).toEqual(
        refused('const', '/const', 'holds an object key like an Object prototype key'),
      );
    });

    it('refuses nullable without type, or false beside a null type, as Ajv did', async () => {
      expect(await validate({ nullable: true })).toEqual(refused('nullable', '/nullable', 'is used without "type"'));
      expect(await validate({ type: ['string', 'null'], nullable: false })).toEqual(
        refused('nullable', '/nullable', 'is false beside a "null" type'),
      );
    });

    it('refuses an $id in an annotation that would move a $ref to another schema', async () => {
      const schema = {
        properties: { a: { $ref: '#/$defs/A' } },
        $defs: { A: { type: 'string' } },
        'x-meta': { $id: '#/$defs/A', type: 'number' },
      };
      expect(await validate(schema, { a: 1 })).toEqual(
        refused('$id', '/x-meta/$id', 'is below the root; nested IDs and anchors are not supported'),
      );
    });

    it('refuses a value JSON cannot carry before checking it', async () => {
      const message = 'The value holds something JSON cannot carry, so it is refused.';
      const open = { type: 'object', properties: { at: {} } };
      expect(await validate(open, { at: new Date(0) })).toEqual({ issues: [{ message, path: ['at'] }] });
      expect(await validate(open, { at: [1, Infinity] })).toEqual({ issues: [{ message, path: ['at', 1] }] });
      expect(await validate(open, Object.create({ at: 1 }))).toEqual({ issues: [{ message, path: [] }] });
      // Ajv's equality calls an own valueOf, so a value compared by const or enum may not hold one.
      expect(await validate({ properties: { a: { const: { p: 1 } } } }, JSON.parse('{"a":{"valueOf":1}}'))).toEqual({
        issues: [{ message, path: ['a', 'valueOf'] }],
      });
      expect(await validate(open, { at: new Proxy({}, {}) })).toEqual({ issues: [{ message, path: ['at'] }] });
      expect(await validate(open, { at: Object.defineProperty({}, 'b', { get: () => 1, enumerable: true }) })).toEqual({
        issues: [{ message, path: ['at'] }],
      });
      // Ajv's equality compared constructors: a null-prototype object or an array subclass is refused only
      // where a value is compared, and accepted elsewhere.
      const compared = { properties: { a: { enum: [{ b: 1 }, [1]] } } };
      expect(await validate(compared, { a: Object.assign(Object.create(null), { b: 1 }) })).toEqual({
        issues: [{ message, path: ['a'] }],
      });
      expect(await validate(compared, { a: Object.setPrototypeOf([1], class extends Array {}.prototype) })).toEqual({
        issues: [{ message, path: ['a'] }],
      });
      const nullPrototype = { at: Object.assign(Object.create(null), { b: 1 }) };
      expect(await validate(open, nullPrototype)).toEqual({ value: nullPrototype });
    });

    it('refuses a schema that Ajv and this validator would read differently', async () => {
      const notPlain = 'holds an object that is not a plain object or array';
      expect(await validate(new Proxy({ type: 'string' }, {}), 'a')).toEqual(refused('$schema', '', 'is a Proxy'));
      expect(await validate({ properties: { a: { [Symbol('s')]: 1 } } })).toEqual(
        refused('a', '/properties/a/Symbol(s)', 'has a key that is not enumerable or is a symbol'),
      );
      expect(await validate(Object.defineProperty({}, 'type', { value: 'string', enumerable: false }), 1)).toEqual(
        refused('$schema', '/type', 'has a key that is not enumerable or is a symbol'),
      );
      expect(
        await validate({ properties: Object.defineProperty({}, 'a', { get: () => ({}), enumerable: true }) }),
      ).toEqual(refused('a', '/properties/a', 'is read through a getter or setter'));
      expect(await validate({ const: new Date(0) })).toEqual(refused('const', '/const', notPlain));
      expect(await validate({ enum: [Object.create(null)] })).toEqual(refused('0', '/enum/0', notPlain));
      // A null-prototype object is a plain schema outside const and enum data.
      expect(await validate(Object.assign(Object.create(null), { type: 'string' }), 'a')).toEqual({ value: 'a' });
    });
  });

  it('reports an extra property once, and still reports a property declared false', async () => {
    const standardSchema = toStandardSchema({
      type: 'object',
      properties: { ok: { type: 'boolean' }, banned: false },
      additionalProperties: false,
    } as JSONSchema7);
    if (jsonSchemaValidatorLoading()) await jsonSchemaValidatorReady();

    expect(await standardSchema['~standard'].validate({ ok: true, extra: 1 })).toEqual({
      issues: [{ message: 'Property "extra" does not match additional properties schema.', path: [] }],
    });
    expect(await standardSchema['~standard'].validate({ banned: 1, extra: 1 })).toEqual({
      issues: [
        { message: 'False boolean schema.', path: ['banned'] },
        { message: 'Property "extra" does not match additional properties schema.', path: [] },
      ],
    });
  });

  describe('isStandardSchemaWithJSON', () => {
    it('should return true for JSON Schema wrapped schemas', () => {
      const jsonSchema: JSONSchema7 = { type: 'string' };
      const wrapper = toStandardSchema(jsonSchema);

      expect(isStandardSchemaWithJSON(wrapper)).toBe(true);
    });

    it('should return false for non-json-schema vendors', () => {
      // Mock a standard schema with different vendor
      const mockSchema = {
        '~standard': {
          version: 1,
          vendor: 'other-vendor',
        },
      };

      expect(isStandardSchemaWithJSON(mockSchema)).toBe(false);
    });

    it('should return false for non-schema values', () => {
      expect(isStandardSchemaWithJSON(null)).toBe(false);
      expect(isStandardSchemaWithJSON(undefined)).toBe(false);
      expect(isStandardSchemaWithJSON({})).toBe(false);
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
