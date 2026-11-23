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
      // The same union reached through $recursiveRef instead of $ref.
      const byRecursiveRef = {
        oneOf: [
          { type: 'object', required: ['text'], properties: { child: { $recursiveRef: '#' } } },
          { type: 'object', required: ['label'], properties: { child: { $recursiveRef: '#' } } },
        ],
      };
      if (jsonSchemaValidatorLoading()) await jsonSchemaValidatorReady();
      // Checking each branch's child again at every level took 2^depth steps: 13-25 s at depth 20.
      for (const [node, leaf] of [
        [byConst, { kind: 'b' }],
        [byRequired, { label: 'x' }],
        [byRecursiveRef, { label: 'x' }],
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

    it('ignores format behind an absolute $ref, and accepts a schema holding a large literal array', async () => {
      const standardSchema = toStandardSchema({
        $id: 'https://example.com/tool',
        type: 'object',
        properties: {
          site: { $ref: 'https://example.com/tool#/components/schemas/properties' },
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

    it('leaves literal data alone when an annotation $ref points into it', async () => {
      const standardSchema = toStandardSchema({
        type: 'object',
        properties: {
          tag: { const: { id: 'expected' }, 'x-meta': { $ref: '#/properties/tag/const' } },
          // A property, or a component, named like a data keyword is still a schema.
          default: { type: 'string', nullable: true },
          other: { $ref: '#/properties/default' },
          qty: { $ref: '#/components/schemas/default' },
        },
        components: { schemas: { default: { type: 'number', minimum: 5, exclusiveMinimum: true } } },
      } as JSONSchema7);
      if (jsonSchemaValidatorLoading()) await jsonSchemaValidatorReady();

      expect(await standardSchema['~standard'].validate({ tag: { id: 'expected' }, other: null, qty: 6 })).toEqual({
        value: { tag: { id: 'expected' }, other: null, qty: 6 },
      });
      expect('issues' in (await standardSchema['~standard'].validate({ tag: {} }))).toBe(true);
      expect('issues' in (await standardSchema['~standard'].validate({ qty: 5 }))).toBe(true);
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

    const dynamicNode = {
      $dynamicAnchor: 'node',
      type: 'object',
      properties: {
        value: { type: 'string' },
        children: { type: 'array', items: { $dynamicRef: '#node' } },
      },
    };
    it.each([
      ['in a keyword position', { $schema: 'https://json-schema.org/draft/2020-12/schema', ...dynamicNode }],
      [
        'reached only through a $ref pointer',
        {
          $schema: 'https://json-schema.org/draft/2020-12/schema',
          $ref: '#/components/schemas/Node',
          components: { schemas: { Node: dynamicNode } },
        },
      ],
      [
        'under a dependencies entry named like a keyword',
        {
          $schema: 'https://json-schema.org/draft/2020-12/schema',
          dependencies: { type: { properties: { children: dynamicNode.properties.children } } },
        },
      ],
      [
        'reached through a $ref to a member named like a name-map keyword',
        {
          $schema: 'https://json-schema.org/draft/2020-12/schema',
          ...dynamicNode,
          properties: {
            ...dynamicNode.properties,
            children: { type: 'array', items: { $ref: '#/components/schemas/dependencies' } },
          },
          components: { schemas: { dependencies: { $dynamicRef: '#node' } } },
        },
      ],
      [
        'in a property named like a data keyword',
        {
          $schema: 'https://json-schema.org/draft/2020-12/schema',
          properties: { default: dynamicNode.properties.children.items },
        },
      ],
    ])('refuses a schema with $dynamicRef %s instead of letting it pass unchecked', async (_case, schema) => {
      const standardSchema = toStandardSchema(schema as JSONSchema7);

      const result = await standardSchema['~standard'].validate({ value: 'root', children: [{ value: 123 }] });

      expect(result).toEqual({
        issues: [
          {
            message:
              'Schema validation error: $dynamicRef is not supported: this validator cannot resolve it, so the schema is refused rather than checked loosely',
          },
        ],
      });
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
