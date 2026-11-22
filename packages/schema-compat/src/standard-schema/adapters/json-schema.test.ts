import type { JSONSchema7 } from 'json-schema';
import { describe, it, expect, vi } from 'vitest';
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

    it('still rejects an own __proto__ key that the schema does not allow', async () => {
      const standardSchema = toStandardSchema({
        type: 'object',
        properties: { a: { type: 'string' } },
        additionalProperties: false,
      });

      // With an undefined property beside it, the value is copied before validation.
      const withUndefined = JSON.parse('{"a":"x","__proto__":{}}');
      withUndefined.b = undefined;

      const plain = await standardSchema['~standard'].validate(JSON.parse('{"a":"x","__proto__":{}}'));
      const copied = await standardSchema['~standard'].validate(withUndefined);

      expect('issues' in plain).toBe(true);
      expect('issues' in copied).toBe(true);
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

  describe('where code generation from strings is disallowed', () => {
    // Composio's GMAIL_FETCH_EMAILS input schema as its API served it on 2026-11-22,
    // with descriptions and examples left out, and `additionalProperties: false` as
    // applyCompatLayer adds it. Note the OpenAPI `nullable` fields.
    const gmailFetchEmails: JSONSchema7 = {
      type: 'object',
      title: 'FetchEmailsRequest',
      additionalProperties: false,
      properties: {
        query: { type: 'string', title: 'Query', default: null, nullable: true } as JSONSchema7,
        user_id: { type: 'string', title: 'User Id', default: 'me' },
        verbose: { type: 'boolean', title: 'Verbose', default: true },
        ids_only: { type: 'boolean', title: 'Ids Only', default: false },
        label_ids: { type: 'array', items: { type: 'string', properties: {} }, title: 'Label Ids' },
        page_token: { type: 'string', title: 'Page Token', default: null, nullable: true } as JSONSchema7,
        max_results: { type: 'integer', title: 'Max Results', default: 1, maximum: 500, minimum: 1 },
        include_payload: { type: 'boolean', title: 'Include Payload', default: true },
        include_spam_trash: { type: 'boolean', title: 'Include Spam Trash', default: false },
      },
    };

    it('accepts and rejects a real tool input the way it did before, with readable issues', async () => {
      // As under `node --disallow-code-generation-from-strings`: building a function
      // from a string throws.
      vi.stubGlobal('Function', function () {
        throw new EvalError('Code generation from strings disallowed for this context');
      });
      try {
        const schema = toStandardSchema(gmailFetchEmails);

        const input = { query: 'is:unread', page_token: null, max_results: 10, label_ids: ['INBOX'] };
        expect(await schema['~standard'].validate(input)).toEqual({ value: input });

        expect(await schema['~standard'].validate({ max_results: 5000, label_ids: [3] })).toEqual({
          issues: [
            { message: 'Instance type "number" is invalid. Expected "string".', path: ['label_ids', 0] },
            { message: '5000 is greater than 500.', path: ['max_results'] },
          ],
        });
      } finally {
        vi.unstubAllGlobals();
      }
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
