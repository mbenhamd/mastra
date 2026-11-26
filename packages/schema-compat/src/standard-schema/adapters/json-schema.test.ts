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

    it('refuses an undeclared property set to undefined where the schema closes the object, as Ajv did', async () => {
      const standardSchema = toStandardSchema({
        type: 'object',
        properties: {
          ok: { type: 'boolean' },
          note: { type: 'string' },
          nested: { type: 'object', properties: { a: {} }, unevaluatedProperties: false },
        },
        additionalProperties: false,
      } as JSONSchema7);
      if (jsonSchemaValidatorLoading()) await jsonSchemaValidatorReady();

      expect(await standardSchema['~standard'].validate({ ok: true, extra: undefined })).toEqual({
        issues: [{ message: 'Property "extra" does not match additional properties schema.', path: [] }],
      });
      expect(await standardSchema['~standard'].validate({ nested: { a: 1, b: undefined } })).toEqual({
        issues: [{ message: 'Property "b" does not match unevaluated properties schema.', path: ['nested'] }],
      });
      const declared = { ok: true, note: undefined, nested: { a: undefined } };
      expect(await standardSchema['~standard'].validate(declared)).toEqual({ value: declared });
    });

    it('lets unevaluatedProperties in a subschema see only that subschema, as Ajv did', async () => {
      const standardSchema = toStandardSchema({
        $defs: { base: { properties: { tag: { type: 'string' } } } },
        type: 'object',
        properties: {
          // `tag` is evaluated by the sibling $ref, not by the allOf branch.
          inner: { $ref: '#/$defs/base', allOf: [{ unevaluatedProperties: false }] },
          // The branch's annotations reach an unevaluatedProperties beside it.
          outer: { allOf: [{ $ref: '#/$defs/base' }], unevaluatedProperties: false },
        },
      } as JSONSchema7);
      if (jsonSchemaValidatorLoading()) await jsonSchemaValidatorReady();

      expect(await standardSchema['~standard'].validate({ inner: { tag: 'x' } })).toEqual({
        issues: [{ message: 'Property "tag" does not match unevaluated properties schema.', path: ['inner'] }],
      });
      expect(await standardSchema['~standard'].validate({ outer: { tag: 'x' } })).toEqual({
        value: { outer: { tag: 'x' } },
      });
    });

    it('takes the branch Ajv took for an undefined property, and keeps a null beside it', async () => {
      // The key is absent for `required` in `if`, so the closed `else` applies.
      const conditional = toStandardSchema({
        if: { required: ['extra'] },
        then: {},
        else: { additionalProperties: false },
      } as JSONSchema7);
      // The second branch declares `x`, so `x: undefined` is absent there.
      const union = toStandardSchema({
        anyOf: [{ properties: { a: {} }, additionalProperties: false }, { properties: { x: { type: 'number' } } }],
      } as JSONSchema7);
      const nullable = toStandardSchema({
        type: 'object',
        properties: { site: { type: ['string', 'null'] }, note: { type: 'string' } },
      } as JSONSchema7);
      if (jsonSchemaValidatorLoading()) await jsonSchemaValidatorReady();

      expect(await conditional['~standard'].validate({ extra: undefined })).toEqual({
        issues: [
          { message: 'Instance does not match "else" schema.', path: [] },
          { message: 'Property "extra" does not match additional properties schema.', path: [] },
        ],
      });
      expect(await conditional['~standard'].validate({ extra: 1 })).toEqual({ value: { extra: 1 } });
      const unionValue = { a: 1, x: undefined };
      expect(await union['~standard'].validate(unionValue)).toEqual({ value: unionValue });
      const nullableValue = { site: null, note: undefined };
      expect(await nullable['~standard'].validate(nullableValue)).toEqual({ value: nullableValue });
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

  describe('draft 2020-12 schema support', () => {
    it('should validate correctly when $schema is draft 2020-12', async () => {
      const jsonSchema = {
        $schema: 'https://json-schema.org/draft/2020-12/schema',
        type: 'object' as const,
        properties: {
          url: { type: 'string' as const },
          formats: {
            type: 'array' as const,
            items: { type: 'string' as const },
          },
        },
        required: ['url'],
      };

      const standardSchema = toStandardSchema(jsonSchema);

      // Valid input should pass
      const validResult = await standardSchema['~standard'].validate({
        url: 'https://example.com',
        formats: ['markdown'],
      });
      expect('value' in validResult).toBe(true);

      // Invalid input should fail
      const invalidResult = await standardSchema['~standard'].validate({
        formats: ['markdown'],
        // missing required 'url'
      });
      expect('issues' in invalidResult).toBe(true);
      if ('issues' in invalidResult && invalidResult.issues) {
        expect(invalidResult.issues.length).toBeGreaterThan(0);
      }
    });

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

    it('reads an OpenAPI 3.0 boolean exclusive bound as excluding its minimum or maximum', async () => {
      const standardSchema = toStandardSchema({
        type: 'object',
        properties: {
          above: { type: 'number', minimum: 5, exclusiveMinimum: true },
          below: { type: 'number', maximum: 0, exclusiveMaximum: true },
          open: { type: 'number', minimum: 1, exclusiveMinimum: false },
        },
      } as unknown as JSONSchema7);
      if (jsonSchemaValidatorLoading()) await jsonSchemaValidatorReady();

      expect(await standardSchema['~standard'].validate({ above: 5.5, below: -0.5, open: 1 })).toEqual({
        value: { above: 5.5, below: -0.5, open: 1 },
      });
      expect(await standardSchema['~standard'].validate({ above: 5, below: 0, open: 0 })).toEqual({
        issues: [
          // cfworker's wording for an exclusive minimum.
          { message: '5 is less than 5.', path: ['above'] },
          { message: '0 is greater than or equal to 0.', path: ['below'] },
          { message: '0 is less than 1.', path: ['open'] },
        ],
      });

      // A name map that a `$ref` also reads as a schema: there `exclusiveMaximum: true` is both a
      // property's schema and a bound, so the schema is refused rather than compared with 1.
      const mapAsSchema = toStandardSchema({
        type: 'object',
        properties: { exclusiveMaximum: true },
        $defs: { bound: { $ref: '#/properties' } },
        allOf: [{ $ref: '#/$defs/bound' }],
      } as unknown as JSONSchema7);
      for (const value of [0, {}]) {
        const result = await mapAsSchema['~standard'].validate(value);
        expect('issues' in result && result.issues?.[0]?.message).toContain('a $ref target that the schema also uses');
      }
    });

    it('keeps a property named id inside a component named like a keyword', async () => {
      const standardSchema = toStandardSchema({
        $ref: '#/components/schemas/properties',
        components: { schemas: { properties: { type: 'object', properties: { id: { type: 'string' } } } } },
      } as JSONSchema7);
      if (jsonSchemaValidatorLoading()) await jsonSchemaValidatorReady();

      expect(await standardSchema['~standard'].validate({ id: 'a' })).toEqual({ value: { id: 'a' } });
      expect('issues' in (await standardSchema['~standard'].validate({ id: 5 }))).toBe(true);
    });

    it('applies nullable and ignores format inside prefixItems, as for properties', async () => {
      const standardSchema = toStandardSchema({
        $schema: 'https://json-schema.org/draft/2020-12/schema',
        type: 'array',
        prefixItems: [
          { type: 'string', format: 'email' },
          { type: 'string', nullable: true },
        ],
      } as JSONSchema7);

      expect(await standardSchema['~standard'].validate(['not an email', null])).toEqual({
        value: ['not an email', null],
      });
      expect('issues' in (await standardSchema['~standard'].validate([1, null]))).toBe(true);
      expect('issues' in (await standardSchema['~standard'].validate(['a', 2]))).toBe(true);
    });

    it('resolves a $ref in a draft-07 dependency named like a keyword, and keeps property dependencies', async () => {
      const standardSchema = toStandardSchema({
        type: 'object',
        properties: {
          type: { type: 'string' },
          detail: { type: 'string' },
          a: {},
          b: {},
          // A pointer into `dependencies` still resolves.
          linked: { $ref: '#/dependencies/type' },
        },
        dependencies: { type: { $ref: '#/definitions/Details' }, a: ['b'], example: ['b'] },
        definitions: { Details: { required: ['detail'] } },
      } as JSONSchema7);
      if (jsonSchemaValidatorLoading()) await jsonSchemaValidatorReady();

      const valid = { type: 'note', detail: 'text', a: 1, b: 2, linked: { detail: 'x' } };
      expect(await standardSchema['~standard'].validate(valid)).toEqual({ value: valid });
      expect('issues' in (await standardSchema['~standard'].validate({ linked: {} }))).toBe(true);
      expect(await standardSchema['~standard'].validate({ type: 'note' })).toEqual({
        issues: [{ message: 'Instance does not have required property "detail".', path: [] }],
      });
      expect('issues' in (await standardSchema['~standard'].validate({ a: 1 }))).toBe(true);
      expect('issues' in (await standardSchema['~standard'].validate({ example: 1 }))).toBe(true);
    });

    it('applies nullable and ignores format on a subschema reached only by $ref', async () => {
      const standardSchema = toStandardSchema({
        type: 'object',
        properties: {
          site: { $ref: '#/components/schemas/Site' },
          // A target named like a keyword whose value maps names to schemas.
          other: { $ref: '#/components/schemas/properties' },
          // Literal data shaped like a reference is compared as written.
          tag: { const: { $ref: '#/properties/tag/const', id: 1 } },
          pct: { const: { $ref: '#/%' }, examples: [{ $ref: '#/a%' }] },
        },
        components: {
          schemas: {
            Site: { type: 'string', format: 'url', nullable: true },
            properties: { type: 'string', format: 'url', nullable: true },
          },
        },
      } as JSONSchema7);
      if (jsonSchemaValidatorLoading()) await jsonSchemaValidatorReady();

      const tag = { $ref: '#/properties/tag/const', id: 1 };
      const pct = { $ref: '#/%' };
      for (const site of [null, 'not a url']) {
        expect(await standardSchema['~standard'].validate({ site, other: site, tag, pct })).toEqual({
          value: { site, other: site, tag, pct },
        });
      }
      expect('issues' in (await standardSchema['~standard'].validate({ site: 5 }))).toBe(true);
      expect('issues' in (await standardSchema['~standard'].validate({ other: 5 }))).toBe(true);
    });

    it('ignores format behind a $ref, and accepts a schema holding a large literal array', async () => {
      const standardSchema = toStandardSchema({
        type: 'object',
        properties: {
          site: { $ref: '#/components/schemas/properties' },
          code: { enum: Array.from({ length: 200_000 }, (_, i) => `c${i}`) },
        },
        components: { schemas: { properties: { type: 'string', format: 'url' } } },
      } as JSONSchema7);
      if (jsonSchemaValidatorLoading()) await jsonSchemaValidatorReady();

      // cfworker's url pattern backtracks exponentially on this string.
      const site = 'http://' + 'a'.repeat(40) + '!';
      expect(await standardSchema['~standard'].validate({ site, code: 'c199999' })).toEqual({
        value: { site, code: 'c199999' },
      });
      expect('issues' in (await standardSchema['~standard'].validate({ site, code: 'd' }))).toBe(true);
    });

    it('keeps examples and a schema named id from changing what a $ref reaches', async () => {
      const valid = async (schema: JSONSchema7, value: unknown) => {
        const standardSchema = toStandardSchema(schema);
        if (jsonSchemaValidatorLoading()) await jsonSchemaValidatorReady();
        return !('issues' in (await standardSchema['~standard'].validate(value)));
      };
      // Two OpenAPI examples with the same id are literal values, not schemas.
      const examples = {
        type: 'object',
        properties: { user: { $ref: '#/components/schemas/User' } },
        components: {
          schemas: { User: { type: 'object', properties: { id: { type: 'integer' } } } },
          examples: { a: { value: { id: 1 } }, b: { value: { id: 1 } } },
        },
      } as JSONSchema7;
      expect(await valid(examples, { user: { id: 1 } })).toBe(true);
      expect(await valid(examples, { user: { id: 'x' } })).toBe(false);
      // A boolean schema named id stays the target of a $ref to it.
      const boolId = {
        type: 'object',
        properties: { a: { $ref: '#/definitions/id' } },
        definitions: { id: false },
      } as JSONSchema7;
      expect(await valid(boolId, {})).toBe(true);
      expect(await valid(boolId, { a: 1 })).toBe(false);
      // A boolean, object or array id in annotations is not a schema URI.
      const annotationIds = {
        type: 'object',
        properties: {
          a: { type: 'string', 'x-ui': { id: true }, example: { id: { $oid: '5f' } } },
          b: { type: 'string', 'x-ui': { id: true }, example: { id: { $oid: '60' } } },
          c: { type: 'string', example: { id: [] } },
        },
      } as unknown as JSONSchema7;
      expect(await valid(annotationIds, { a: 'x' })).toBe(true);
      expect(await valid(annotationIds, { a: 1 })).toBe(false);
      // A component named id does not move the base of references between other components.
      const idComponent = {
        type: 'object',
        properties: { u: { $ref: '#/components/schemas/User' } },
        components: {
          schemas: {
            id: { type: 'string' },
            User: { type: 'object', properties: { n: { $ref: '#/components/schemas/Name' } } },
            Name: { type: 'string' },
          },
        },
      } as unknown as JSONSchema7;
      expect(await valid(idComponent, { u: { n: 'x' } })).toBe(true);
      expect(await valid(idComponent, { u: { n: 1 } })).toBe(false);
    });

    it('leaves literal data alone when an annotation $ref points into it', async () => {
      const standardSchema = toStandardSchema({
        type: 'object',
        properties: {
          tag: { const: { id: 'expected' }, 'x-meta': { $ref: '#/properties/tag/const' } },
          // A property, or a component, named like a data keyword is still a schema.
          default: { type: 'string', nullable: true },
          other: { $ref: '#/properties/default' },
          qty: { $ref: '#/components/schemas/default' },
          // A forbidden property in a map an annotation refers to.
          id: false,
          // Subschemas inside a component named like a data keyword.
          q: { $ref: '#/components/schemas/examples' },
          // A pointer into a referenced component's literal data.
          tok: { $ref: '#/components/schemas/Tok' },
        },
        'x-meta': { $ref: '#/properties' },
        components: {
          schemas: {
            default: { type: 'number', minimum: 5, exclusiveMinimum: true },
            examples: {
              type: 'object',
              properties: {
                n: { type: 'number', minimum: 5, exclusiveMinimum: true },
                s: { type: 'string', nullable: true },
              },
            },
            Tok: { const: { id: 'k1', v: 1 }, 'x-meta': { $ref: '#/components/schemas/Tok/const' } },
          },
        },
      } as JSONSchema7);
      if (jsonSchemaValidatorLoading()) await jsonSchemaValidatorReady();

      const valid = { tag: { id: 'expected' }, other: null, qty: 6, q: { n: 6, s: null }, tok: { id: 'k1', v: 1 } };
      expect(await standardSchema['~standard'].validate(valid)).toEqual({ value: valid });
      expect('issues' in (await standardSchema['~standard'].validate({ tag: {} }))).toBe(true);
      expect('issues' in (await standardSchema['~standard'].validate({ qty: 5 }))).toBe(true);
      expect('issues' in (await standardSchema['~standard'].validate({ id: 1 }))).toBe(true);
      expect('issues' in (await standardSchema['~standard'].validate({ q: { n: 5 } }))).toBe(true);
      expect('issues' in (await standardSchema['~standard'].validate({ tok: { v: 1 } }))).toBe(true);
    });

    it('ignores id in objects that are not schemas, and resolves a $ref to a schema named example', async () => {
      const standardSchema = toStandardSchema({
        type: 'object',
        properties: {
          user: { type: 'object', example: { id: 1 }, 'x-meta': { id: 'u' }, externalDocs: { id: 7 } },
          owner: { type: 'object', examples: [{ id: 1 }], 'x-meta': { id: 'u' }, externalDocs: { id: 7 } },
          account: { type: 'object', example: { id: 1 } },
          sample: { $ref: '#/components/schemas/example' },
        },
        components: { schemas: { example: { type: 'string' } } },
      } as JSONSchema7);
      if (jsonSchemaValidatorLoading()) await jsonSchemaValidatorReady();

      expect(await standardSchema['~standard'].validate({ user: {}, sample: 's' })).toEqual({
        value: { user: {}, sample: 's' },
      });
      expect('issues' in (await standardSchema['~standard'].validate({ sample: 5 }))).toBe(true);
    });

    it('keeps a property named $id or $ref, and resolves # and #/... references', async () => {
      const standardSchema = toStandardSchema({
        type: 'object',
        properties: {
          $id: { type: 'string' },
          $ref: { type: 'integer' },
          self: { $ref: '#' },
          id: { $ref: '#/properties/$id' },
        },
      } as JSONSchema7);
      if (jsonSchemaValidatorLoading()) await jsonSchemaValidatorReady();

      const valid = { $id: 'a', $ref: 1, self: { $ref: 2 }, id: 'b' };
      expect(await standardSchema['~standard'].validate(valid)).toEqual({ value: valid });
      expect('issues' in (await standardSchema['~standard'].validate({ $id: 1 }))).toBe(true);
      expect('issues' in (await standardSchema['~standard'].validate({ self: { $ref: 'x' } }))).toBe(true);
      expect('issues' in (await standardSchema['~standard'].validate({ id: 1 }))).toBe(true);
    });

    it('checks values against a component named like a keyword that holds its own reference', async () => {
      const standardSchema = toStandardSchema({
        type: 'object',
        properties: { a: { $ref: '#/components/schemas/type' }, n: { type: 'number' } },
        components: {
          schemas: {
            type: { type: 'object', properties: { o: { $ref: '#/components/schemas/Other' } } },
            Other: { type: 'string' },
          },
        },
      } as JSONSchema7);
      if (jsonSchemaValidatorLoading()) await jsonSchemaValidatorReady();

      expect(await standardSchema['~standard'].validate({ n: 1 })).toEqual({ value: { n: 1 } });
      expect('issues' in (await standardSchema['~standard'].validate({ n: 'x' }))).toBe(true);
      expect('issues' in (await standardSchema['~standard'].validate({ a: { o: 'x' } }))).toBe(true);
    });

    it('refuses a schema that brings a key the validator writes while resolving references', async () => {
      const standardSchema = toStandardSchema({
        type: 'object',
        properties: {
          n: { $ref: '#/properties/m', __absolute_ref__: '#/properties/m/default' },
          m: { default: { type: 'number', minimum: 5 } },
        },
      } as JSONSchema7);
      if (jsonSchemaValidatorLoading()) await jsonSchemaValidatorReady();

      expect(await standardSchema['~standard'].validate({ n: 5 })).toEqual({
        issues: [
          {
            message:
              'Schema validation error: __absolute_ref__ at "/properties/n/__absolute_ref__" is reserved for the validator, so the schema is refused',
          },
        ],
      });
    });

    const dynamicNode = {
      $dynamicAnchor: 'node',
      type: 'object',
      properties: {
        value: { type: 'string' },
        children: { type: 'array', items: { $dynamicRef: '#node' } },
      },
    };
    const draft2020 = 'https://json-schema.org/draft/2020-12/schema';
    it.each([
      [
        '$dynamicAnchor in a keyword position',
        { $schema: draft2020, ...dynamicNode },
        '$dynamicAnchor at "/$dynamicAnchor"',
      ],
      [
        '$dynamicRef reached only through a $ref pointer',
        {
          $schema: draft2020,
          $ref: '#/components/schemas/Node',
          components: { schemas: { Node: dynamicNode.properties } },
        },
        '$dynamicRef at "/components/schemas/Node/children/items/$dynamicRef"',
      ],
      [
        '$dynamicRef under a dependencies entry named like a keyword',
        { $schema: draft2020, dependencies: { type: { properties: { children: dynamicNode.properties.children } } } },
        '$dynamicRef at "/dependencies/type/properties/children/items/$dynamicRef"',
      ],
      [
        '$dynamicRef in a property named like a data keyword',
        { $schema: draft2020, properties: { default: dynamicNode.properties.children.items } },
        '$dynamicRef at "/properties/default/$dynamicRef"',
      ],
      ['$id at the root', { $id: 'http://x.test/root', type: 'object' }, '$id at "/$id"'],
      [
        '$id with a JSON Pointer fragment',
        { type: 'object', definitions: { z: { $id: '#/properties/a' } } },
        '$id at "/definitions/z/$id"',
      ],
      ['$anchor', { $defs: { lim: { $anchor: 'lim', minimum: 5 } } }, '$anchor at "/$defs/lim/$anchor"'],
      ['$recursiveAnchor', { $recursiveAnchor: true, type: 'object' }, '$recursiveAnchor at "/$recursiveAnchor"'],
      [
        '$recursiveRef',
        { type: 'object', properties: { child: { $recursiveRef: '#' } } },
        '$recursiveRef at "/properties/child/$recursiveRef"',
      ],
      [
        'an absolute $ref',
        { properties: { v: { $ref: 'http://x.test/root#/definitions' } } },
        '$ref "http://x.test/root#/definitions" at "/properties/v/$ref"',
      ],
      [
        'a relative $ref',
        { properties: { v: { $ref: 'root#/definitions' } } },
        '$ref "root#/definitions" at "/properties/v/$ref"',
      ],
      [
        'an anchor $ref',
        { properties: { n: { $ref: '#lim' } }, examples: { a: { minimum: 5 } } },
        '$ref "#lim" at "/properties/n/$ref"',
      ],
      [
        'a keyword under a name map that a $ref reads as a schema',
        { $defs: { $recursiveAnchor: true }, $ref: '#/$defs' },
        '$recursiveAnchor at "/$defs/$recursiveAnchor"',
      ],
      ['$id inside an annotation', { type: 'string', example: { $id: { k: 1 } } }, '$id at "/example/$id"'],
      ['a string $id in a name map', { properties: { $id: 'http://x.test/a' } }, '$id at "/properties/$id"'],
      [
        'a keyword that a $ref inside an array-valued properties reaches',
        (() => {
          const entry = { $ref: '#/default' };
          return { properties: [entry], 'x-entry': entry, default: { $recursiveAnchor: true, minimum: 5 } };
        })(),
        '$recursiveAnchor at "/default/$recursiveAnchor"',
      ],
      [
        'an object-valued $ref in data the dereference also walks',
        { $ref: '#/x/properties', x: { properties: { const: { example: { $ref: {} } } } } },
        '$ref that is not a string at "/x/properties/const/example/$ref"',
      ],
      [
        'a keyword that a $ref inside an object-valued prefixItems reaches',
        {
          type: 'array',
          prefixItems: { length: 1, 0: { $ref: '#/default' } },
          default: { $recursiveAnchor: true, type: 'object' },
        },
        '$recursiveAnchor at "/default/$recursiveAnchor"',
      ],
      [
        'a $ref that is not a string',
        {
          type: 'object',
          properties: {
            n: { $ref: ['#/properties/m/default'] },
            m: { default: { type: 'number', minimum: 5, exclusiveMinimum: true } },
          },
        },
        '$ref that is not a string at "/properties/n/$ref"',
      ],
    ])('refuses a schema with %s instead of resolving it', async (_case, schema, refused) => {
      const standardSchema = toStandardSchema(schema as JSONSchema7);
      if (jsonSchemaValidatorLoading()) await jsonSchemaValidatorReady();

      for (const value of [{ value: 'root', children: [{ value: 123 }] }, 'x']) {
        expect(await standardSchema['~standard'].validate(value)).toEqual({
          issues: [
            {
              message: `Schema validation error: ${refused} is not supported: only "#" and "#/..." references within the schema are resolved, so the schema is refused rather than checked loosely`,
            },
          ],
        });
      }
    });
  });

  it.each([
    [
      'its pointer names nothing (a malformed escape)',
      {
        type: 'array',
        prefixItems: { length: 1, 0: { $ref: '#/default/a%zz' } },
        default: { 'a%zz': { $recursiveAnchor: true, type: 'object' } },
      },
      '$ref "#/default/a%zz" at "/prefixItems/0/$ref" does not resolve within the schema, so the schema is refused',
    ],
    [
      'normalisation replaces its target',
      {
        type: 'array',
        minimum: 5,
        exclusiveMinimum: true,
        prefixItems: { length: 1, 0: { $ref: '#/exclusiveMinimum' } },
      },
      'a $ref resolves to a different target once the schema is prepared for validation, so the schema is refused',
    ],
    [
      'its target is also literal data with a boolean bound',
      {
        type: 'array',
        prefixItems: { length: 1, 0: { $ref: '#/default' } },
        default: { minimum: 5, exclusiveMinimum: true },
      },
      'a $ref target that the schema also uses as a name map or as literal data holds a boolean exclusiveMinimum or exclusiveMaximum, which cannot be read both ways, so the schema is refused',
    ],
  ])('refuses a $ref inside an object-valued prefixItems when %s', async (_case, schema, refused) => {
    const standardSchema = toStandardSchema(schema as unknown as JSONSchema7);
    if (jsonSchemaValidatorLoading()) await jsonSchemaValidatorReady();

    for (const value of [[5], [{}]]) {
      expect(await standardSchema['~standard'].validate(value)).toEqual({
        issues: [{ message: `Schema validation error: ${refused}` }],
      });
    }
  });

  it('refuses a schema whose reachable $ref points nowhere, and ignores one in an unused definition', async () => {
    const dangling = toStandardSchema({ properties: { x: { $ref: '#/missing' } } } as JSONSchema7);
    const unused = toStandardSchema({ type: 'object', $defs: { U: { $ref: '#/missing' } } } as JSONSchema7);
    if (jsonSchemaValidatorLoading()) await jsonSchemaValidatorReady();

    expect(await dangling['~standard'].validate({})).toEqual({
      issues: [
        {
          message:
            'Schema validation error: $ref "#/missing" at "/properties/x/$ref" does not resolve within the schema, so the schema is refused',
        },
      ],
    });
    expect(await unused['~standard'].validate({})).toEqual({ value: {} });
  });

  it('refuses a reachable $ref to a value that is not a schema', async () => {
    const standardSchema = toStandardSchema({
      properties: { x: { $ref: '#/x-n' } },
      'x-n': null,
    } as unknown as JSONSchema7);
    if (jsonSchemaValidatorLoading()) await jsonSchemaValidatorReady();

    expect(await standardSchema['~standard'].validate({})).toEqual({
      issues: [
        {
          message:
            'Schema validation error: $ref "#/x-n" at "/properties/x/$ref" points at a value that is not a schema, so the schema is refused',
        },
      ],
    });
  });

  it.each([
    ['then without if', { then: { $ref: '#/missing' } }, 5],
    ['else without if', { type: 'object', else: { $ref: '#/missing' } }, {}],
    ['additionalItems without items', { additionalItems: { $ref: '#/missing' } }, [1]],
    [
      'additionalItems beside an object items',
      { items: { type: 'number' }, additionalItems: { $ref: '#/missing' } },
      [1],
    ],
  ])('accepts a dangling $ref under %s, which the validator never reads', async (_label, schema, value) => {
    const standardSchema = toStandardSchema(schema as unknown as JSONSchema7);
    if (jsonSchemaValidatorLoading()) await jsonSchemaValidatorReady();

    expect(await standardSchema['~standard'].validate(value)).toEqual({ value });
  });

  it('refuses a $ref to "#/", which the validator reads as the root\'s "" member', async () => {
    const standardSchema = toStandardSchema({
      type: 'object',
      properties: { child: { $ref: '#/' } },
      '': true,
    } as unknown as JSONSchema7);
    if (jsonSchemaValidatorLoading()) await jsonSchemaValidatorReady();

    expect(await standardSchema['~standard'].validate({ child: 1 })).toEqual({
      issues: [
        {
          message:
            'Schema validation error: $ref "#/" at "/properties/child/$ref" is not supported: the validator reads it as the root\'s "" member, not as the root, so the schema is refused; write "#" for the root',
        },
      ],
    });
  });

  it.each([
    ['a property named $id', { properties: { $id: { type: 'string' } } }, { $id: 'abc' }],
    ['a property named $id with a boolean schema', { properties: { $id: false } }, {}],
    ['a dependency list under a property named $id', { dependencies: { $id: ['label'] } }, {}],
    [
      'a $ref under an object-valued prefixItems key the validator never reads',
      { type: 'array', prefixItems: { length: 0, note: { $ref: '#/default' } }, default: { exclusiveMinimum: true } },
      [],
    ],
    ['$id inside const', { const: { $id: 'k' } }, { $id: 'k' }],
    ['$anchor inside enum', { enum: ['a', { $anchor: 'k' }] }, { $anchor: 'k' }],
    ['a non-local $ref inside examples', { examples: [{ $ref: 'http://x.test/a~b' }] }, 'x'],
    [
      '$recursiveRef inside default, under a key with / and ~',
      { properties: { 'a/b~': { default: { $recursiveRef: '#' } } } },
      {},
    ],
    [
      'a property named $id in a component named properties',
      {
        $ref: '#/components/schemas/properties',
        components: { schemas: { properties: { type: 'object', properties: { $id: { type: 'string' } } } } },
      },
      { $id: 'abc' },
    ],
  ])('accepts a schema with %s, which only names a property or holds data', async (_case, schema, value) => {
    const standardSchema = toStandardSchema(schema as JSONSchema7);
    if (jsonSchemaValidatorLoading()) await jsonSchemaValidatorReady();

    expect(await standardSchema['~standard'].validate(value)).toEqual({ value });
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

  describe('uniqueItems', () => {
    const schema = toStandardSchema({
      type: 'object',
      properties: {
        tags: { $ref: '#/$defs/Tags' },
        nested: { type: 'object', properties: { ids: { type: 'array', uniqueItems: true } } },
      },
      $defs: { Tags: { type: 'array', items: { type: 'string' }, uniqueItems: true } },
    } as JSONSchema7);
    const validate = async (value: unknown) => {
      if (jsonSchemaValidatorLoading()) await jsonSchemaValidatorReady();
      return schema['~standard'].validate(value);
    };

    it('refuses a duplicate behind a $ref, naming the first value that repeats and its next index', async () => {
      expect(await validate({ tags: ['a', 'b', 'b', 'a'] })).toEqual({
        issues: [{ message: 'Duplicate items at indexes 0 and 3.', path: ['tags'] }],
      });
    });

    it('accepts distinct items, including values that differ only in type', async () => {
      expect(await validate({ tags: ['a', 'b'] })).toEqual({ value: { tags: ['a', 'b'] } });
      expect(await validate({ nested: { ids: ['1', 1, true, null] } })).toEqual({
        value: { nested: { ids: ['1', 1, true, null] } },
      });
    });

    it('refuses equal numbers, nulls and objects in a nested array', async () => {
      for (const ids of [
        [1, 1.0],
        [null, null],
        [{ a: [1] }, { a: [1] }],
        [
          { a: 1, b: [2] },
          { b: [2], a: 1 },
        ],
      ]) {
        expect(await validate({ nested: { ids } })).toEqual({
          issues: [{ message: 'Duplicate items at indexes 0 and 1.', path: ['nested', 'ids'] }],
        });
      }
    });

    it('finds a duplicate in an array that mixes strings and objects', async () => {
      expect(await validate({ nested: { ids: ['a', { x: 1 }, 'b', { x: 1 }] } })).toEqual({
        issues: [{ message: 'Duplicate items at indexes 1 and 3.', path: ['nested', 'ids'] }],
      });
      expect(await validate({ nested: { ids: ['a', { x: 1 }, 'a', { x: 1 }] } })).toEqual({
        issues: [{ message: 'Duplicate items at indexes 0 and 2.', path: ['nested', 'ids'] }],
      });
    });

    it('checks strings and objects in time that grows linearly with their count', async () => {
      // One array of 16,000 items, half strings and half objects, against 16 arrays
      // of 1,000. A linear check costs about the same for both; the pairwise check
      // costs 16 times more for the long one. The fastest of five interleaved rounds
      // keeps a loaded host from deciding.
      const item = (prefix: string, i: number) => (i % 2 === 0 ? `${prefix}${i}` : { id: `${prefix}${i}`, n: [i] });
      const long = { nested: { ids: Array.from({ length: 16_000 }, (_, i) => item('t', i)) } };
      const short = Array.from({ length: 16 }, (_, n) => ({
        nested: { ids: Array.from({ length: 1_000 }, (_, i) => item(`t${n}-`, i)) },
      }));
      let longBest = Infinity;
      let shortBest = Infinity;
      for (let round = 0; round < 5; round++) {
        let start = performance.now();
        expect('value' in (await validate(long))).toBe(true);
        longBest = Math.min(longBest, performance.now() - start);
        start = performance.now();
        for (const value of short) expect('value' in (await validate(value))).toBe(true);
        shortBest = Math.min(shortBest, performance.now() - start);
      }
      expect(longBest / shortBest).toBeLessThan(6);
    }, 120_000);
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
