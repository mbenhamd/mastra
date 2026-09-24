import type { JSONSchema7, Schema } from '@internal/ai-sdk-v5';
import {
  AnthropicSchemaCompatLayer,
  applyCompatLayer,
  DeepSeekSchemaCompatLayer,
  GoogleSchemaCompatLayer,
  MetaSchemaCompatLayer,
  OpenAIReasoningSchemaCompatLayer,
  OpenAISchemaCompatLayer,
} from '@mastra/schema-compat';
import type { z as z3 } from 'zod/v3';
import type { z as z4 } from 'zod/v4';
import type { PublicSchema, StandardSchemaWithJSON } from '../../schema';
import { isStandardSchemaWithJSON, standardSchemaToJSONSchema } from '../../schema';

export type PartialSchemaOutput<OUTPUT = undefined> = OUTPUT extends undefined ? undefined : Partial<OUTPUT>;

/**
 * @deprecated Use StandardSchemaWithJSON from '../../schema' instead
 */
export type OutputSchema<OBJECT = any> =
  | z4.ZodType<OBJECT, any>
  | z3.Schema<OBJECT, z3.ZodTypeDef, any>
  | Schema<OBJECT>
  | JSONSchema7
  | undefined;

/**
 * @deprecated Use StandardSchemaWithJSON from '../../schema' instead
 * Legacy type for schema validation.
 */
export type SchemaWithValidation<T = any> = z4.ZodType<T, any> | z3.Schema<T, z3.ZodTypeDef, any>;

/**
 * @deprecated Use InferPublicSchema or InferStandardSchemaOutput from '../../schema' instead
 * Infer the output type from a schema
 */
export type InferSchemaOutput<T> =
  T extends z4.ZodType<infer O, any>
    ? O
    : T extends z3.Schema<infer O, z3.ZodTypeDef, any>
      ? O
      : T extends Schema<infer O>
        ? O
        : unknown;

/**
 * @deprecated Use PublicSchema from '../../schema' instead
 */
export type InferZodLikeSchema<T> =
  T extends z4.ZodType<infer O, any> ? O : T extends z3.Schema<infer O, z3.ZodTypeDef, any> ? O : unknown;

export type ZodLikePartialSchema<T = any> =
  | (z4.core.$ZodType<Partial<T>, any> & {
      safeParse(value: unknown): { success: boolean; data?: Partial<T>; error?: any };
    })
  | (z3.ZodType<Partial<T>, z3.ZodTypeDef, any> & {
      safeParse(value: unknown): { success: boolean; data?: Partial<T>; error?: any };
    });

export function asJsonSchema(schema: StandardSchemaWithJSON | undefined): JSONSchema7 | undefined {
  if (!schema) {
    return undefined;
  }

  // Handle StandardSchemaWithJSON
  if (isStandardSchemaWithJSON(schema)) {
    // Use 'input' IO mode to get the schema BEFORE transforms are applied
    // This is critical for OpenAI compat transforms that add .transform()
    // which can't be properly represented in JSON Schema
    //
    // Use 'draft-07' target for maximum compatibility with LLM providers
    const jsonSchema = standardSchemaToJSONSchema(schema, { io: 'input', target: 'draft-07' });

    return jsonSchema;
  }

  return schema;
}

export type SchemaModelInfo = {
  provider: string;
  modelId: string;
  supportsStructuredOutputs: boolean;
};

function createSchemaCompatLayers(model: SchemaModelInfo) {
  return [
    new OpenAIReasoningSchemaCompatLayer(model),
    new OpenAISchemaCompatLayer(model),
    new GoogleSchemaCompatLayer(model),
    new AnthropicSchemaCompatLayer(model),
    new DeepSeekSchemaCompatLayer(model),
    new MetaSchemaCompatLayer(model),
  ];
}

/**
 * Returns the matched provider compatibility layer's validating schema, or
 * undefined when no layer applies. Provider responses generated against a
 * compat-transformed wire schema (e.g. OpenAI strict mode emits null for
 * optional fields) must be validated through this schema so compat post-
 * processing maps values back to the original schema's expectations.
 */
export function getCompatValidationSchema<OUTPUT = undefined>(
  schema: StandardSchemaWithJSON<OUTPUT>,
  model: SchemaModelInfo,
): StandardSchemaWithJSON<OUTPUT> | undefined {
  for (const layer of createSchemaCompatLayers(model)) {
    if (layer.shouldApply()) {
      return layer.processToCompatSchema(schema) as StandardSchemaWithJSON<OUTPUT>;
    }
  }
  return undefined;
}

export function getTransformedSchema<OUTPUT = undefined>(
  schema?: StandardSchemaWithJSON<OUTPUT>,
  options?: { model?: SchemaModelInfo },
) {
  if (!schema) {
    return undefined;
  }

  const jsonSchema = options?.model
    ? (applyCompatLayer({
        schema: schema as PublicSchema<OUTPUT>,
        compatLayers: createSchemaCompatLayers(options.model),
        mode: 'jsonSchema',
      }) as JSONSchema7)
    : asJsonSchema(schema);

  if (!jsonSchema) {
    return undefined;
  }

  const { $schema, ...itemSchema } = jsonSchema;
  if (itemSchema.type === 'array') {
    const innerElement = itemSchema.items;
    const arrayOutputSchema: JSONSchema7 = {
      $schema: $schema,
      type: 'object',
      properties: {
        elements: { type: 'array', items: innerElement },
      },
      required: ['elements'],
      additionalProperties: false,
    };

    return {
      jsonSchema: arrayOutputSchema,
      outputFormat: 'array',
    };
  }

  // Handle enum schemas - wrap in object like AI SDK does
  if (itemSchema.enum && Array.isArray(itemSchema.enum)) {
    const enumOutputSchema: JSONSchema7 = {
      $schema: $schema,
      type: 'object',
      properties: {
        result: { type: itemSchema.type || 'string', enum: itemSchema.enum },
      },
      required: ['result'],
      additionalProperties: false,
    };

    return {
      jsonSchema: enumOutputSchema,
      outputFormat: 'enum',
    };
  }

  return {
    jsonSchema: jsonSchema,
    outputFormat: jsonSchema.type, // 'object'
  };
}

export function getResponseFormat(
  schema?: StandardSchemaWithJSON,
  options?: { model?: SchemaModelInfo },
):
  | {
      type: 'text';
    }
  | {
      type: 'json';
      /**
       * JSON schema that the generated output should conform to.
       */
      schema?: JSONSchema7;
    } {
  if (schema) {
    const transformedSchema = getTransformedSchema(schema, options);
    return {
      type: 'json',
      schema: transformedSchema?.jsonSchema,
    };
  }

  // response format 'text' for everything else
  return {
    type: 'text',
  };
}
