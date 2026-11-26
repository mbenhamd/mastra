import { jsonSchema as aiSdkJsonSchema } from '@internal/ai-v6';
import { AnthropicSchemaCompatLayer, isStandardSchemaWithJSON } from '@mastra/schema-compat';
import { describe, expect, it, vi } from 'vitest';
import { z as z3 } from 'zod/v3';
import { z } from 'zod/v4';
import { RequestContext } from '../../request-context';
import type { StandardSchemaWithJSON } from '../../schema';
import { createTool } from '../tool';
import type { ToolAction } from '../types';
import { resolveToolOutputValidationSchema, validateToolInput } from '../validation';
import { CoreToolBuilder } from './builder';

const haikuModelConfig = {
  provider: 'anthropic',
  modelId: 'claude-3.5-haiku-20241022',
  specificationVersion: 'v4' as const,
  supportsStructuredOutputs: false,
} as const;

function buildCoreTool(
  tool: ToolAction<any, any>,
  name: string,
  model: typeof haikuModelConfig | Record<string, unknown>,
) {
  return new CoreToolBuilder({
    originalTool: tool,
    options: {
      name,
      model: model as any,
      requestContext: new RequestContext(),
    },
  }).build();
}

describe('CoreToolBuilder - Schema Compatibility in Validation', () => {
  it('does not enable edited approvals for an AI SDK JSON schema without a validator after resume augmentation', async () => {
    const execute = vi.fn(async (input: { note: string }) => input);
    const tool = createTool({
      id: 'raw-json-schema-tool',
      description: 'Raw JSON schema tool',
      inputSchema: aiSdkJsonSchema<{ note: string }>({
        type: 'object',
        properties: { note: { type: 'string' } },
        required: ['note'],
        additionalProperties: false,
      }),
      execute,
    });

    const build = () =>
      new CoreToolBuilder({
        originalTool: tool,
        options: { name: 'raw-json-schema-tool', requestContext: new RequestContext(), requireApproval: true },
        autoResumeSuspendedTools: true,
      }).build();

    const firstBuilt = build();
    const secondBuilt = build();

    expect(firstBuilt.approvalInputEditing).toBeUndefined();
    expect(secondBuilt.approvalInputEditing).toBeUndefined();

    await expect(
      firstBuilt.execute?.(
        { note: 'ok' },
        { abortSignal: new AbortController().signal, toolCallId: 'raw-json-schema-valid', messages: [] },
      ),
    ).resolves.toEqual({ note: 'ok' });
    await expect(
      firstBuilt.execute?.(
        {},
        { abortSignal: new AbortController().signal, toolCallId: 'raw-json-schema-missing', messages: [] },
      ),
    ).resolves.toMatchObject({ error: true });
    await expect(
      secondBuilt.execute?.(
        { note: 42 },
        { abortSignal: new AbortController().signal, toolCallId: 'raw-json-schema-wrong-type', messages: [] },
      ),
    ).resolves.toMatchObject({ error: true });
    expect(execute).toHaveBeenCalledTimes(1);
  });

  it('preserves async AI SDK validation through resume augmentation', async () => {
    const execute = vi.fn(async (input: { note: string }) => input);
    const inputSchema = aiSdkJsonSchema<{ note: string }>(
      {
        type: 'object',
        properties: { note: { type: 'string' } },
        required: ['note'],
        additionalProperties: false,
      },
      {
        validate: (async (value: unknown) => {
          const note = (value as { note?: unknown })?.note;
          if (typeof note !== 'string' || note === 'reject') {
            return { success: false, error: new Error('Rejected note') };
          }
          return { success: true, value: { note: note.trim().toUpperCase() } };
        }) as any,
      },
    );
    const tool: ToolAction<any, any> = {
      id: 'raw-async-validator-tool',
      description: 'Raw AI SDK schema with async validation',
      inputSchema,
    };

    const builder = new CoreToolBuilder({
      originalTool: tool,
      options: { name: 'raw-async-validator-tool', requestContext: new RequestContext() },
      autoResumeSuspendedTools: true,
    });
    tool.execute = execute;
    const built = builder.build();

    await expect(
      built.execute?.(
        { note: ' accepted ' },
        { abortSignal: new AbortController().signal, toolCallId: 'async-native-valid', messages: [] },
      ),
    ).resolves.toEqual({ note: 'ACCEPTED' });
    await expect(
      built.execute?.(
        { note: 'reject' },
        { abortSignal: new AbortController().signal, toolCallId: 'async-native-invalid', messages: [] },
      ),
    ).resolves.toMatchObject({ error: true });
    expect(execute).toHaveBeenCalledTimes(1);
    expect(execute).toHaveBeenCalledWith({ note: 'ACCEPTED' }, expect.any(Object));
  });

  it('preserves raw Zod v3 transforms and refinements through resume augmentation', async () => {
    const execute = vi.fn(async (input: { note: string }) => input);
    const inputSchema = z3
      .object({ note: z3.string().transform(value => value.trim().toUpperCase()) })
      .refine(value => value.note !== 'REJECT', 'Rejected note');
    const tool: ToolAction<any, any> = {
      id: 'raw-zod-v3-validator-tool',
      description: 'Raw Zod v3 schema with transform and refinement',
      inputSchema,
    };

    const createBuilder = () =>
      new CoreToolBuilder({
        originalTool: tool,
        options: { name: 'raw-zod-v3-validator-tool', requestContext: new RequestContext() },
        autoResumeSuspendedTools: true,
      });
    const firstBuilder = createBuilder();
    tool.execute = execute;
    const firstBuilt = firstBuilder.build();
    tool.execute = undefined;
    const secondBuilder = createBuilder();
    tool.execute = execute;
    const secondBuilt = secondBuilder.build();

    await expect(
      firstBuilt.execute?.(
        { note: ' accepted ' },
        { abortSignal: new AbortController().signal, toolCallId: 'zod-v3-valid', messages: [] },
      ),
    ).resolves.toEqual({ note: 'ACCEPTED' });
    await expect(
      firstBuilt.execute?.(
        { note: ' reject ' },
        { abortSignal: new AbortController().signal, toolCallId: 'zod-v3-invalid', messages: [] },
      ),
    ).resolves.toMatchObject({ error: true });
    await expect(
      secondBuilt.execute?.(
        { note: ' accepted ' },
        { abortSignal: new AbortController().signal, toolCallId: 'zod-v3-repeat-valid', messages: [] },
      ),
    ).resolves.toEqual({ note: 'ACCEPTED' });
    await expect(
      secondBuilt.execute?.(
        { note: ' reject ' },
        { abortSignal: new AbortController().signal, toolCallId: 'zod-v3-repeat-invalid', messages: [] },
      ),
    ).resolves.toMatchObject({ error: true });
    expect(execute).toHaveBeenCalledTimes(2);
    expect(execute).toHaveBeenNthCalledWith(1, { note: 'ACCEPTED' }, expect.any(Object));
    expect(execute).toHaveBeenNthCalledWith(2, { note: 'ACCEPTED' }, expect.any(Object));
  });

  it('preserves async validation results in the native tool input schema', async () => {
    const inputSchema: StandardSchemaWithJSON = {
      '~standard': {
        version: 1,
        vendor: 'async-validation-test',
        validate: async value =>
          typeof value === 'string' ? { value: value.trim() } : { issues: [{ message: 'Expected a string' }] },
        jsonSchema: {
          input: () => ({ type: 'string' }),
          output: () => ({ type: 'string' }),
        },
      },
    };
    const schema = new CoreToolBuilder({
      originalTool: { id: 'async-validator', description: 'Validates asynchronously', inputSchema },
      options: { name: 'async-validator' },
    }).buildV5().inputSchema;

    if (typeof schema !== 'object' || schema === null || !('validate' in schema) || !schema.validate) {
      throw new Error('Expected a native schema with validation');
    }
    expect(await schema.validate(' value ')).toEqual({ success: true, value: 'value' });
    // Issues surface as a path-aware error (upstream #25794): one line per field plus `errors`.
    expect(await schema.validate(42)).toEqual({
      success: false,
      error: Object.assign(new Error('- root: Expected a string'), {
        errors: [{ path: [], message: 'Expected a string' }],
      }),
    });
  });

  it('preserves asynchronous validation through the native v6 schema wrapper', async () => {
    const execute = vi.fn(async (input: { token: string }) => input);
    const tool = {
      id: 'async-validation-tool',
      description: 'Tool with an asynchronous Zod 4 refinement',
      inputSchema: z.object({ token: z.string() }).refine(async input => input.token === 'accepted'),
      execute,
    } as ToolAction<any, any>;
    const built = buildCoreTool(tool, 'async-validation-tool', {
      provider: 'test-provider',
      modelId: 'test-model',
      specificationVersion: 'v4',
      supportsStructuredOutputs: false,
    });

    const nativeValidation = (built.parameters as { validate: (input: unknown) => Promise<unknown> }).validate;
    await expect(nativeValidation({ token: 'accepted' })).resolves.toMatchObject({
      success: true,
      value: { token: 'accepted' },
    });
    await expect(nativeValidation({ token: 'rejected' })).resolves.toMatchObject({ success: false });
    await expect(built.validateInput?.({ token: 'accepted' })).resolves.toMatchObject({
      data: { token: 'accepted' },
    });
    await expect(built.validateInput?.({ token: 'rejected' })).resolves.toMatchObject({ error: { error: true } });
    await expect(
      built.execute?.(
        { token: 'accepted' },
        { abortSignal: new AbortController().signal, toolCallId: 'async-call', messages: [] },
      ),
    ).resolves.toEqual({ token: 'accepted' });
    expect(execute).toHaveBeenCalledWith({ token: 'accepted' }, expect.any(Object));
  });

  it('createTool execute path skips author-schema re-validation after CoreToolBuilder compat validation', async () => {
    const execute = vi.fn(async ({ text }: { text: string }) => ({ success: true, text }));
    const shortTextTool = createTool({
      id: 'short-text-tool',
      description: 'Tool created via createTool',
      inputSchema: z.object({
        text: z.string().min(20),
      }),
      execute,
    });

    const coreTool = buildCoreTool(shortTextTool, 'shortTextTool', haikuModelConfig);
    await expect(coreTool.validateInput?.({ text: 'Short text' })).resolves.toMatchObject({
      error: { error: true },
    });
    const executeResult = await coreTool.execute?.(
      { text: 'Short text' },
      {
        abortSignal: new AbortController().signal,
        toolCallId: 'call-create-tool',
        messages: [],
      },
    );

    expect(executeResult).not.toHaveProperty('error');
    expect(executeResult).toEqual({ success: true, text: 'Short text' });
    expect(execute).toHaveBeenCalledTimes(1);
  });

  it('does not invoke class-based Standard Schemas as lazy factories (#27265)', async () => {
    const jsonSchema = {
      type: 'object',
      properties: { query: { type: 'string' } },
      required: ['query'],
      additionalProperties: false,
    };
    // Mimics Effect `Schema.Class(...).pipe(Schema.toStandardSchemaV1, Schema.toStandardJSONSchemaV1)`:
    // an ES class constructor carrying Standard Schema metadata.
    class Input {
      static '~standard' = {
        version: 1 as const,
        vendor: 'effect',
        validate: (value: unknown) =>
          typeof (value as any)?.query === 'string'
            ? { value }
            : { issues: [{ message: 'query must be a string', path: ['query'] }] },
        jsonSchema: { input: () => jsonSchema, output: () => jsonSchema },
      };
    }
    expect(isStandardSchemaWithJSON(Input)).toBe(true);
    expect(resolveToolOutputValidationSchema({ outputSchema: Input })).toBe(Input);

    const tool: ToolAction<any, any> = {
      id: 'class_probe',
      description: 'Return the supplied query',
      inputSchema: Input as any,
      outputSchema: Input as any,
      execute: async ({ query }) => ({ query }),
    };

    for (const model of [haikuModelConfig, { provider: 'openai', modelId: 'gpt-4.1', specificationVersion: 'v2' }]) {
      const coreTool = buildCoreTool(tool, 'class_probe', model);
      const options = { abortSignal: new AbortController().signal, toolCallId: 'call-class', messages: [] };
      expect(await coreTool.execute?.({ query: 'test' }, options)).toEqual({ query: 'test' });
      expect(await coreTool.execute?.({ query: 1 }, options)).toHaveProperty('error', true);
    }
  });

  it('processToCompatSchema accepts short strings on Haiku after stripping min/max', () => {
    const inputSchema = z.object({ text: z.string().min(20) });
    expect(isStandardSchemaWithJSON(inputSchema)).toBe(true);

    const layer = new AnthropicSchemaCompatLayer(haikuModelConfig as any);
    const shortInput = { text: 'Short text' };

    const viaCompatSchema = layer.processToCompatSchema(inputSchema);
    expect(validateToolInput(viaCompatSchema, shortInput, 'regression').error).toBeUndefined();
  });

  it('preserves cross-field refine rejection on Haiku compat validation', async () => {
    const inputSchema = z
      .object({
        start: z.number(),
        end: z.number(),
      })
      .refine(value => value.end > value.start);

    const tool: ToolAction<any, any> = {
      id: 'range-tool',
      description: 'Validates start/end range',
      inputSchema,
      execute: async ({ start, end }) => ({ start, end }),
    };

    const coreTool = buildCoreTool(tool, 'range-tool', haikuModelConfig);
    const executeResult = await coreTool.execute?.(
      { start: 10, end: 1 },
      {
        abortSignal: new AbortController().signal,
        toolCallId: 'test-call-id',
        messages: [],
      },
    );

    expect(executeResult).toHaveProperty('error', true);
  });

  it('keeps default fields optional in LLM-facing required array on Haiku', () => {
    const inputSchema = z.object({
      req: z.string(),
      def: z.string().default('hi'),
      opt: z.number().optional(),
    });

    const coreTool = buildCoreTool(
      {
        id: 'defaults-tool',
        description: 'Defaults tool',
        inputSchema,
        execute: async () => ({}),
      },
      'defaults-tool',
      haikuModelConfig,
    );

    const llmJsonSchema = (coreTool.parameters as { jsonSchema?: { required?: string[] } }).jsonSchema;
    expect(llmJsonSchema?.required).toEqual(['req']);
  });

  it('accepts omitted optional fields on o4-mini via processToCompatSchema validation', async () => {
    const execute = vi.fn(async (input: { a: string; b?: string }) => input);
    const tool = createTool({
      id: 'optional-o4-tool',
      description: 'Optional field tool',
      inputSchema: z.object({
        a: z.string(),
        b: z.string().optional(),
      }),
      execute,
    });

    const coreTool = buildCoreTool(tool, 'optional-o4-tool', {
      provider: 'openai',
      modelId: 'o4-mini',
      specificationVersion: 'v4',
      supportsStructuredOutputs: true,
    });

    await expect(coreTool.validateInput?.({ a: 'x', b: null })).resolves.toEqual({ data: { a: 'x' } });

    const executeResult = await coreTool.execute?.(
      { a: 'x' },
      {
        abortSignal: new AbortController().signal,
        toolCallId: 'optional-o4',
        messages: [],
      },
    );

    expect(executeResult).not.toHaveProperty('error');
    expect(execute).toHaveBeenCalledWith({ a: 'x', b: undefined }, expect.any(Object));
  });

  it('accepts omitted default fields on o4-mini and applies defaults via compat validation', async () => {
    const execute = vi.fn(async (input: { a: string; c: string }) => input);
    const tool = createTool({
      id: 'default-o4-tool',
      description: 'Default field tool',
      inputSchema: z.object({
        a: z.string(),
        c: z.string().default('fallback'),
      }),
      execute,
    });

    const coreTool = buildCoreTool(tool, 'default-o4-tool', {
      provider: 'openai',
      modelId: 'o4-mini',
      specificationVersion: 'v4',
      supportsStructuredOutputs: true,
    });

    const executeResult = await coreTool.execute?.(
      { a: 'x' },
      {
        abortSignal: new AbortController().signal,
        toolCallId: 'default-o4',
        messages: [],
      },
    );

    expect(executeResult).not.toHaveProperty('error');
    expect(execute).toHaveBeenCalledWith({ a: 'x', c: 'fallback' }, expect.any(Object));
  });

  it('coerces string input to number on o4-mini without rejecting at validation', async () => {
    const execute = vi.fn(async ({ a }: { a: number }) => ({ a }));
    const tool = createTool({
      id: 'coerce-o4-tool',
      description: 'Coerce number field',
      inputSchema: z.object({
        a: z.coerce.number(),
      }),
      execute,
    });

    const coreTool = buildCoreTool(tool, 'coerce-o4-tool', {
      provider: 'openai',
      modelId: 'o4-mini',
      specificationVersion: 'v4',
      supportsStructuredOutputs: true,
    });

    const executeResult = await coreTool.execute?.(
      { a: '42' },
      {
        abortSignal: new AbortController().signal,
        toolCallId: 'coerce-o4',
        messages: [],
      },
    );

    expect(executeResult).not.toHaveProperty('error');
    expect(executeResult).toEqual({ a: 42 });
    expect(execute).toHaveBeenCalledWith({ a: 42 }, expect.any(Object));
  });

  it('coerces string input to number on gemini-2.0-flash-lite without rejecting at validation', async () => {
    const execute = vi.fn(async ({ a }: { a: number }) => ({ a }));
    const tool = createTool({
      id: 'coerce-gemini-tool',
      description: 'Coerce number field',
      inputSchema: z.object({
        a: z.coerce.number(),
      }),
      execute,
    });

    const coreTool = buildCoreTool(tool, 'coerce-gemini-tool', {
      provider: 'google',
      modelId: 'gemini-2.0-flash-lite',
      specificationVersion: 'v4',
      supportsStructuredOutputs: false,
    });

    const executeResult = await coreTool.execute?.(
      { a: '42' },
      {
        abortSignal: new AbortController().signal,
        toolCallId: 'coerce-gemini',
        messages: [],
      },
    );

    expect(executeResult).not.toHaveProperty('error');
    expect(executeResult).toEqual({ a: 42 });
    expect(execute).toHaveBeenCalledWith({ a: 42 }, expect.any(Object));
  });

  it('builds Haiku tools with z.tuple input schemas without throwing', () => {
    const tool = createTool({
      id: 'tuple-tool',
      description: 'Tuple input tool',
      inputSchema: z.tuple([z.string(), z.number()]),
      execute: async () => ({ ok: true }),
    });

    expect(() => buildCoreTool(tool, 'tuple-tool', haikuModelConfig)).not.toThrow();
  });

  it('strips string minLength from LLM-facing parameters and execute validation on Haiku', async () => {
    const inputSchema = z.object({
      message: z.string().min(10).describe('A message with minimum 10 characters'),
    });

    const tool: ToolAction<any, any> = {
      id: 'test-tool',
      description: 'A test tool with string constraints',
      inputSchema,
      execute: async ({ message }) => ({ result: `Received: ${message}` }),
    };

    const coreTool = buildCoreTool(tool, 'test-tool', haikuModelConfig);
    const llmJsonSchema = (coreTool.parameters as { jsonSchema?: { properties?: Record<string, unknown> } }).jsonSchema;
    const messageProp = llmJsonSchema?.properties?.message as { minLength?: number } | undefined;

    expect(messageProp?.minLength).toBeUndefined();

    const executeResult = await coreTool.execute?.(
      { message: 'Hi there' },
      {
        abortSignal: new AbortController().signal,
        toolCallId: 'test-call-id',
        messages: [],
      },
    );

    expect(executeResult).not.toHaveProperty('error');
    expect(executeResult).toEqual({ result: 'Received: Hi there' });
  });

  it('still rejects invalid tool input on Haiku after compat transformation', async () => {
    const inputSchema = z.object({
      message: z.string().min(10),
    });

    const tool: ToolAction<any, any> = {
      id: 'test-tool',
      description: 'Reject invalid shapes',
      inputSchema,
      execute: async ({ message }) => ({ message }),
    };

    const coreTool = buildCoreTool(tool, 'test-tool', haikuModelConfig);

    const wrongType = await coreTool.execute?.(
      { message: 123 },
      {
        abortSignal: new AbortController().signal,
        toolCallId: 'test-call-id',
        messages: [],
      },
    );
    expect(wrongType).toHaveProperty('error', true);

    const missingField = await coreTool.execute?.(
      {},
      {
        abortSignal: new AbortController().signal,
        toolCallId: 'test-call-id',
        messages: [],
      },
    );
    expect(missingField).toHaveProperty('error', true);
  });

  it('preserves original constraints when no schema compat layer applies', async () => {
    const inputSchema = z.object({
      message: z.string().min(10),
    });

    const tool: ToolAction<any, any> = {
      id: 'test-tool',
      description: 'No compat layers',
      inputSchema,
      execute: async ({ message }) => ({ message }),
    };

    const coreTool = buildCoreTool(tool, 'test-tool', {
      provider: 'local',
      modelId: 'local-test-model',
      specificationVersion: 'v4',
      supportsStructuredOutputs: false,
    });

    const executeResult = await coreTool.execute?.(
      { message: 'short' },
      {
        abortSignal: new AbortController().signal,
        toolCallId: 'test-call-id',
        messages: [],
      },
    );

    expect(executeResult).toHaveProperty('error', true);
    expect((executeResult as { message?: string }).message).toMatch(/validation failed/i);
  });

  it('should use schema-compat transformed schema for BOTH LLM parameters AND validation', async () => {
    // Create a tool with string min constraint
    const inputSchema = z.object({
      message: z.string().min(10).describe('A message with minimum 10 characters'),
    });

    const toolWithConstraints: ToolAction<any, any> = {
      id: 'test-tool',
      description: 'A test tool with string constraints',
      inputSchema,
      execute: async (input: z.infer<typeof inputSchema>) => {
        const { message } = input;
        return { result: `Received: ${message}` };
      },
    };

    // Create a model config that requires schema transformation (Claude 3.5 Haiku strips min/max from strings)
    const modelConfig = haikuModelConfig;

    const coreTool = new CoreToolBuilder({
      originalTool: toolWithConstraints,
      options: {
        name: 'test-tool',
        model: modelConfig as any,
        requestContext: new RequestContext(),
      },
    }).build();

    // ASSERTION 1: The parameters sent to the LLM should be transformed
    // (Claude 3.5 Haiku compatibility layer removes min/max constraints from strings)
    const anthropicLayer = new AnthropicSchemaCompatLayer(modelConfig as any);

    // The original schema has a min constraint
    const originalSchema = inputSchema;
    const messageField = originalSchema.shape.message as z.ZodString;
    // Zod v4 uses class instances for checks instead of plain objects
    // Check by looking for the MinLength check class
    const hasMinCheck =
      messageField._def.checks?.some(
        (check: any) =>
          check.constructor?.name === '$ZodCheckMinLength' || (check.kind === 'min' && check.value === 10),
      ) ?? false;
    expect(hasMinCheck).toBe(true);

    // The transformed schema should NOT have the min constraint (for Claude 3.5 Haiku)
    // This is what the LLM sees
    const transformedSchema = anthropicLayer.processZodType(originalSchema);
    const transformedMessageField = (transformedSchema as any).shape.message as z.ZodString;

    // Zod v4 uses class instances for checks instead of plain objects
    const hasMinCheckAfterTransform =
      transformedMessageField._def.checks?.some(
        (check: any) => check.constructor?.name === '$ZodCheckMinLength' || check.kind === 'min',
      ) ?? false;
    expect(hasMinCheckAfterTransform).toBe(false);

    // ASSERTION 2: Validation must use the same transformed schema the LLM saw

    // Simulate what happens when the LLM calls the tool with a short string (< 10 chars)
    // The LLM was told there's no minimum, so it might send a short string
    const shortMessage = 'Hi there'; // Only 8 characters, less than the original min of 10

    const executeResult = await coreTool.execute?.(
      { message: shortMessage },
      {
        abortSignal: new AbortController().signal,
        toolCallId: 'test-call-id',
        messages: [],
      },
    );

    expect(executeResult).not.toHaveProperty('error');
    expect(executeResult).toHaveProperty('result');
    expect(executeResult.result).toBe('Received: Hi there');
  });

  it('should validate against transformed schema for number constraints with Anthropic', async () => {
    // Create a tool with number constraints
    const inputSchema2 = z.object({
      age: z.number().min(18).max(100).describe('Age between 18 and 100'),
    });

    const toolWithNumberConstraints: ToolAction<any, any> = {
      id: 'number-tool',
      description: 'A test tool with number constraints',
      inputSchema: inputSchema2,
      execute: async (input: z.infer<typeof inputSchema2>) => {
        const { age } = input;
        return { result: `Age: ${age}` };
      },
    };

    const modelConfig = {
      provider: 'anthropic',
      modelId: 'claude-3-opus-20240229',
      specificationVersion: 'v2' as const,
      supportsStructuredOutputs: false,
    };

    const builder = new CoreToolBuilder({
      originalTool: toolWithNumberConstraints,
      options: {
        name: 'number-tool',
        model: modelConfig as any,
        requestContext: new RequestContext(),
      },
    });

    const coreTool = builder.build();

    // Anthropic's schema compat layer transforms number constraints
    // The LLM receives a schema without strict min/max enforcement

    const executeResult = await coreTool.execute?.(
      { age: 25 },
      {
        abortSignal: new AbortController().signal,
        toolCallId: 'test-call-id',
        messages: [],
      },
    );

    expect(executeResult).not.toHaveProperty('error');
    expect(executeResult).toHaveProperty('result');
  });

  it('validates short strings when Anthropic Haiku strips string min constraints from the LLM schema', async () => {
    const inputSchema4 = z.object({
      text: z.string().min(20).describe('Text with minimum 20 characters'),
    });

    const toolWithMinConstraint: ToolAction<any, any> = {
      id: 'bug-demo-tool',
      description: 'Demonstrates the validation bug',
      inputSchema: inputSchema4,
      execute: async (input: z.infer<typeof inputSchema4>) => {
        const { text } = input;
        return { success: true, text };
      },
    };

    const modelConfig = {
      provider: 'anthropic',
      modelId: 'claude-3.5-haiku-20241022',
      specificationVersion: 'v4' as const,
      supportsStructuredOutputs: false,
    };

    const builder = new CoreToolBuilder({
      originalTool: toolWithMinConstraint,
      options: {
        name: 'bug-demo-tool',
        model: modelConfig as any,
        requestContext: new RequestContext(),
      },
    });

    const coreTool = builder.build();

    // The LLM receives a schema WITHOUT the min(20) constraint
    // So it might send a string with only 10 characters
    const shortText = 'Short text'; // Only 10 characters

    const executeResult = await coreTool.execute?.(
      { text: shortText },
      {
        abortSignal: new AbortController().signal,
        toolCallId: 'test-call-id',
        messages: [],
      },
    );

    expect(executeResult).not.toHaveProperty('error');
    expect(executeResult).toEqual({
      success: true,
      text: shortText,
    });
  });

  it('keeps native validation for union constraints when Haiku rewrites the LLM schema', async () => {
    const inputSchema = z.union([z.object({ text: z.string().min(20) }), z.object({ count: z.number() })]);
    const execute = vi.fn(async (input: unknown) => ({ success: true, input }));
    const tool: ToolAction<any, any> = {
      id: 'union-min-tool',
      description: 'Validates union branches',
      inputSchema,
      execute,
    };

    const coreTool = buildCoreTool(tool, 'union-min-tool', haikuModelConfig);
    const shortInput = { text: 'Short text' };

    await expect(coreTool.validateInput?.(shortInput)).resolves.toMatchObject({
      error: { error: true },
    });

    const executeResult = await coreTool.execute?.(shortInput, {
      abortSignal: new AbortController().signal,
      toolCallId: 'union-min-call',
      messages: [],
    });
    expect(executeResult).toEqual({ success: true, input: shortInput });
    expect(execute).toHaveBeenCalledWith(shortInput, expect.any(Object));
  });

  it('should handle OpenAI o3 reasoning model converting optional to nullable (working memory bug)', async () => {
    // This reproduces the exact bug reported by the user with updateWorkingMemory tool
    // OpenAI o3 converts .optional() to .nullable(), then sends null, but validation
    // was checking against the original schema which expects string | undefined
    const inputSchema5 = z.object({
      newMemory: z.string().describe('New memory to add'),
      searchString: z.string().optional().describe('Optional search string'),
      updateReason: z.string().describe('Reason for update'),
    });

    const updateWorkingMemoryTool: ToolAction<any, any> = {
      id: 'updateWorkingMemory',
      description: 'Update working memory',
      inputSchema: inputSchema5,
      execute: async (input: z.infer<typeof inputSchema5>) => {
        const { newMemory, searchString, updateReason } = input;
        return { success: true, newMemory, searchString, updateReason };
      },
    };

    const modelConfig = {
      provider: 'openai',
      modelId: 'o3-mini',
      specificationVersion: 'v4' as const,
      supportsStructuredOutputs: true,
    };

    const builder = new CoreToolBuilder({
      originalTool: updateWorkingMemoryTool,
      options: {
        name: 'updateWorkingMemory',
        model: modelConfig as any,
        requestContext: new RequestContext(),
      },
    });

    const coreTool = builder.build();

    // OpenAI o3 converts .optional() to .nullable() via OpenAIReasoningSchemaCompatLayer
    // So the LLM sends null instead of undefined for optional fields
    const newMemoryValue = '#User\n- First Name: Randy\n- Last Name: Lynn';
    const executeResult = await coreTool.execute?.(
      {
        newMemory: newMemoryValue,
        searchString: null, // LLM sends null because schema was converted to nullable
        updateReason: 'append-new-memory',
      },
      {
        abortSignal: new AbortController().signal,
        toolCallId: 'call_W84bP8Lo2qCwIYe03QDLCgTJ',
        messages: [],
      },
    );

    // Compat validation coerces null back to undefined for optional fields
    expect(executeResult).not.toHaveProperty('error');
    expect(executeResult).toEqual({
      success: true,
      newMemory: newMemoryValue,
      searchString: undefined,
      updateReason: 'append-new-memory',
    });
  });

  it.skip('should respect structured outputs for v2 models and preserve enums/constraints', async () => {
    const category = z.enum(['book', 'device']).describe('Inventory category');

    const inputSchema = z
      .object({
        category: category.optional().describe('Inventory category'),
        price: z.number().min(1).describe('Unit price'),
        label: z
          .string()
          .trim()
          .optional()
          .transform(v => v?.toLowerCase())
          .describe('Optional label'),
      })
      .describe('Inventory search filters');

    const toolWithEnum: ToolAction<any, any> = {
      id: 'v2-structured-tool',
      description: 'Tool with enum and min constraint',
      inputSchema,
      execute: async (input: z.infer<typeof inputSchema>) => {
        return { ok: true, received: input };
      },
    };

    const v2ModelConfig = {
      provider: 'openai',
      modelId: 'gpt-4o-mini',
      specificationVersion: 'v2' as const,
      supportsStructuredOutputs: true,
    };

    const builder = new CoreToolBuilder({
      originalTool: toolWithEnum,
      options: {
        name: 'v2-structured-tool',
        model: v2ModelConfig as any,
        requestContext: new RequestContext(),
      },
    });

    const coreTool = builder.build();

    // Ensure the parameters we send to the LLM are not empty objects
    const params = coreTool.parameters as any;
    const props = params.properties || {};

    expect(Object.keys(props)).toEqual(['category', 'price', 'label']);
    const requiredFields = params?.required || [];
    // Zod v4: .optional().transform() chains lose optional info during JSON Schema conversion
    // The transform wrapper becomes the outer type, so 'label' appears as required
    // This is a known limitation in Zod v4's toJSONSchema() implementation
    expect(requiredFields).toEqual(['price', 'label']);

    // Ensure the enum/type details survive schema compat
    const categoryProp = props.category;
    const priceProp = props.price;
    const labelProp = props.label;

    expect(categoryProp).toBeDefined();
    expect(categoryProp.type).toBe('string');
    expect(categoryProp.enum).toEqual(['book', 'device']);

    expect(priceProp).toBeDefined();
    expect(priceProp.type).toBe('number');
    expect(priceProp.minimum).toBe(1);

    expect(labelProp).toBeDefined();
    // Zod v4: transforms may not preserve type information in JSON Schema
    // The label field with .trim().optional().transform() loses type info
    // and only preserves the description

    // Execution should accept valid enum and numeric inputs and apply transform
    const executeResult = await coreTool.execute?.(
      { category: 'book', price: 25, label: '  BLUE ' },
      {
        abortSignal: new AbortController().signal,
        toolCallId: 'test-call-id',
        messages: [],
      },
    );

    expect(executeResult).not.toHaveProperty('error');
    expect(executeResult).toEqual({
      ok: true,
      received: { category: 'book', price: 25, label: 'blue' },
    });
  });

  describe('explicit nulls on optional fields without the OpenAI compat layer', () => {
    // Models without the OpenAI compat layer (Anthropic, OpenRouter Gemini) see
    // the author's .optional() fields, and some send null for them. Agent tool
    // calls validate asynchronously; the refusal must name the real fault only.
    const nonOpenAiModels = [
      ['anthropic', haikuModelConfig],
      [
        'openrouter gemini',
        {
          provider: 'openrouter',
          modelId: 'google/gemini-2.5-flash-lite',
          specificationVersion: 'v4' as const,
          supportsStructuredOutputs: false,
        },
      ],
    ] as const;
    const slideTool = (): ToolAction<any, any> => ({
      id: 'propose-slide',
      description: 'Propose a slide',
      inputSchema: z.object({
        title: z.string(),
        subtitle: z.string().optional(),
        content: z.object({ body: z.string() }),
        position: z.number().optional(),
      }),
      execute: async input => ({ ok: true, input }),
    });
    const callOptions = { abortSignal: new AbortController().signal, toolCallId: 'slide-call', messages: [] };

    it.each(nonOpenAiModels)('lists only the real fault when it comes with nulls (%s)', async (_, model) => {
      const result = await buildCoreTool(slideTool(), 'propose-slide', model).execute?.(
        { title: 'Results', subtitle: null, content: { body: 42 }, position: null },
        callOptions,
      );
      expect(result).toHaveProperty('error', true);
      const listed = (result as { message: string }).message
        .split('\n')
        .filter(line => line.startsWith('- '))
        .map(line => line.slice(2, line.indexOf(':')));
      expect(listed).toEqual(['content.body']);
    });

    it.each(nonOpenAiModels)('accepts nulls on optional fields alone (%s)', async (_, model) => {
      const result = await buildCoreTool(slideTool(), 'propose-slide', model).execute?.(
        { title: 'Results', subtitle: null, content: { body: 'Survival improved.' }, position: null },
        callOptions,
      );
      expect(result).toEqual({ ok: true, input: { title: 'Results', content: { body: 'Survival improved.' } } });
    });
  });

  describe('an approved tool call is not refused at execute for its input', () => {
    // The approval check runs before a user is asked; execute() later validates
    // the same raw arguments against the compat schema. On a compat-layer model
    // the check must refuse whatever execute() would refuse.
    const geminiModel = {
      provider: 'openrouter',
      modelId: 'google/gemini-2.5-flash-lite',
      specificationVersion: 'v4' as const,
      supportsStructuredOutputs: false,
    };
    const schema = z.object({
      title: z.string(),
      subtitle: z.string().optional(),
      note: z.string().nullable().optional(),
      tags: z.array(z.string()).optional(),
      layout: z.object({ columns: z.number() }).optional(),
      content: z.object({ body: z.string(), caption: z.string().optional() }),
      either: z.union([z.string(), z.array(z.number())]).optional(),
    });
    const promptSchema = z.object({ prompt: z.string(), limit: z.number().optional() });
    const gatedTool = (inputSchema: typeof schema | typeof promptSchema): ToolAction<any, any> => ({
      id: 'gated-tool',
      description: 'A tool that needs approval',
      inputSchema,
      requireApproval: true,
      execute: async input => ({ ok: true, input }),
    });
    const callOptions = { abortSignal: new AbortController().signal, toolCallId: 'gated-call', messages: [] };
    const listed = (error: unknown) =>
      String((error as { message?: unknown }).message)
        .split('\n')
        .filter(line => line.startsWith('- '))
        .map(line => line.slice(2, line.indexOf(':')));

    it('refuses a stringified value with a null before approval, as execute would', async () => {
      const input = { title: 'A', tags: '["x"]', subtitle: null, content: { body: 'b' } };
      const preflight = await buildCoreTool(gatedTool(schema), 'gated-tool', geminiModel).validateInput?.(input);
      const executed = await buildCoreTool(gatedTool(schema), 'gated-tool', geminiModel).execute?.(input, callOptions);
      expect(executed).toHaveProperty('error', true);
      expect(listed(preflight?.error)).toEqual(listed(executed));
    });

    it('lets a valid call through approval and execute', async () => {
      const input = { title: 'A', subtitle: null, tags: ['x'], content: { body: 'b' } };
      const coreTool = buildCoreTool(gatedTool(schema), 'gated-tool', geminiModel);
      await expect(coreTool.validateInput?.(input)).resolves.toEqual({
        data: { title: 'A', tags: ['x'], content: { body: 'b' } },
      });
      await expect(coreTool.execute?.(input, callOptions)).resolves.toEqual({
        ok: true,
        input: { title: 'A', tags: ['x'], content: { body: 'b' } },
      });
    });

    const cases: [string, typeof schema | typeof promptSchema, unknown][] = [
      ['valid', schema, { title: 'A', content: { body: 'b' } }],
      ['nulls on optional fields', schema, { title: 'A', subtitle: null, content: { body: 'b', caption: null } }],
      ['null on a nullable field', schema, { title: 'A', note: null, content: { body: 'b' } }],
      ['fault plus nulls', schema, { title: 'A', subtitle: null, content: { body: 42, caption: null } }],
      ['fault only', schema, { title: 'A', content: { body: 42 } }],
      ['missing required', schema, { content: { body: 'b' } }],
      ['stringified array', schema, { title: 'A', tags: '["x","y"]', content: { body: 'b' } }],
      [
        'stringified object plus nulls',
        schema,
        { title: 'A', layout: '{"columns":2}', subtitle: null, content: { body: 'b' } },
      ],
      ['stringified array plus fault', schema, { title: 'A', tags: '["x"]', content: { body: 1 } }],
      ['malformed stringified array', schema, { title: 'A', tags: '[x', content: { body: 'b' } }],
      ['JSON-looking string in a string union', schema, { title: 'A', either: '[1,2]', content: { body: 'b' } }],
      [
        'invalid JSON-looking string in a string union plus nulls',
        schema,
        { title: 'A', either: '["a"]', subtitle: null, content: { body: 'b' } },
      ],
      ['top-level null', promptSchema, null],
      ['prompt alias', promptSchema, { query: 'find it' }],
      ['prompt alias plus null', promptSchema, { query: 'find it', limit: null }],
      ['prompt alias plus fault', promptSchema, { query: 'find it', limit: 'many' }],
    ];

    it.each(cases)('%s: refused before approval exactly when execute refuses it', async (_, inputSchema, input) => {
      const coreTool = buildCoreTool(gatedTool(inputSchema), 'gated-tool', geminiModel);
      const preflight = await coreTool.validateInput?.(structuredClone(input));
      const executed = await coreTool.execute?.(structuredClone(input), callOptions);
      if (preflight?.error === undefined) {
        expect(executed).toMatchObject({ ok: true });
      } else {
        expect(executed).toHaveProperty('error', true);
      }
    });

    it("runs the author's refinement once per pre-approval check pair, as the native check alone did", async () => {
      // The loops check the same arguments object twice before approval. Fork
      // main ran only the native check there: the refinement ran twice.
      let refinements = 0;
      const counted = schema.refine(() => {
        refinements += 1;
        return true;
      });
      const coreTool = buildCoreTool(gatedTool(counted as unknown as typeof schema), 'gated-tool', geminiModel);
      const args = { title: 'A', content: { body: 'b' } };

      expect((await coreTool.validateInput?.(args))?.error).toBeUndefined();
      expect((await coreTool.validateInput?.(args))?.error).toBeUndefined();

      expect(refinements).toBe(2);
    });

    // Each loop removes a top-level `_background` before execute() validates, by
    // its own rule: the standard loop when the value is truthy, the durable loop
    // whenever the key is present. The compat check must see what execute() sees.
    const backgroundSeen = () => {
      const seen: boolean[] = [];
      const foreground = z
        .object({
          title: z.string(),
          _background: z.union([z.literal(false), z.object({ enabled: z.boolean() })]).optional(),
        })
        .superRefine(value => {
          seen.push(Object.hasOwn(value, '_background'));
        });
      const coreTool = buildCoreTool(gatedTool(foreground as unknown as typeof schema), 'gated-tool', geminiModel);
      return { seen, coreTool };
    };

    it('checks a stringified `_background` as the standard loop does, which removes it before execute', async () => {
      const foreground = z.object({ title: z.string(), _background: z.object({ enabled: z.boolean() }).optional() });
      const coreTool = buildCoreTool(gatedTool(foreground as unknown as typeof schema), 'gated-tool', geminiModel);

      const preflight = await coreTool.validateInput?.(
        { title: 'A', _background: '{"enabled":false}' },
        { backgroundRemoval: 'truthy' },
      );

      expect(preflight?.error).toBeUndefined();
    });

    it('checks `_background` as the standard loop does: removed when truthy, kept when falsy', async () => {
      const { seen, coreTool } = backgroundSeen();

      const truthy = await coreTool.validateInput?.(
        { title: 'A', _background: { enabled: false } },
        { backgroundRemoval: 'truthy' },
      );
      expect(truthy?.error).toBeUndefined();
      // compat check without it, native check with it
      expect(seen).toEqual([false, true]);

      seen.length = 0;
      const falsy = await coreTool.validateInput?.({ title: 'A', _background: false }, { backgroundRemoval: 'truthy' });
      expect(falsy?.error).toBeUndefined();
      expect(seen).toEqual([true, true]);
    });

    it('checks `_background` as the durable loop does: removed whenever present, falsy included', async () => {
      const { seen, coreTool } = backgroundSeen();

      const falsy = await coreTool.validateInput?.(
        { title: 'A', _background: false },
        { backgroundRemoval: 'present' },
      );

      expect(falsy?.error).toBeUndefined();
      expect(seen).toEqual([false, true]);
    });
  });

  describe('strict: false opts out of strict-mode schema rewriting', () => {
    const openaiModelConfig = {
      provider: 'openai',
      modelId: 'gpt-4o',
      specificationVersion: 'v4' as const,
      supportsStructuredOutputs: true,
    } as const;

    const requiredKeysFor = (strict?: boolean) => {
      const tool = createTool({
        id: 'partial-update-tool',
        description: 'Accepts partial updates',
        inputSchema: z.object({
          name: z.string().optional(),
          city: z.string().optional(),
        }),
        ...(strict === undefined ? {} : { strict }),
        execute: async () => ({ ok: true }),
      });

      const coreTool = buildCoreTool(tool, 'partialUpdateTool', openaiModelConfig);
      const jsonSchema = (coreTool.parameters as any).jsonSchema ?? coreTool.parameters;
      return (jsonSchema.required as string[] | undefined) ?? [];
    };

    it('promotes optional properties to required by default on OpenAI strict mode', () => {
      expect(requiredKeysFor(undefined)).toEqual(expect.arrayContaining(['name', 'city']));
    });

    it('keeps optional properties optional when the tool sets strict: false', () => {
      expect(requiredKeysFor(false)).toEqual([]);
    });
  });
});
