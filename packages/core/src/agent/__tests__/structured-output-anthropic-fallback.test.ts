/**
 * PF-7309: Anthropic's native output format is strict. `@ai-sdk/anthropic` rewrites every object to
 * `additionalProperties: false`, so a free-form record (explicit, or an object that is open by JSON
 * Schema default) can only come back as `{}`, and a schema over Anthropic's complexity limits is
 * rejected. Those schemas are injected into the prompt instead, as the caller wrote them.
 *
 * Every row drives `Agent.generate()` through the real provider adapter against a local HTTP server
 * and inspects the request body the adapter sends, so the assertions see exactly what would reach
 * the provider API.
 */
import { once } from 'node:events';
import { createServer } from 'node:http';
import type { AddressInfo } from 'node:net';
import { createAnthropic } from '@ai-sdk/anthropic-v6';
import { createOpenAI } from '@ai-sdk/openai-v6';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { z } from 'zod/v4';
import { Agent } from '../agent';

let baseURL = '';
let reply: unknown;
const requestBodies: any[] = [];

function anthropicResponse(body: any) {
  const usesJsonTool = Array.isArray(body.tools) && body.tools.some((tool: any) => tool.name === 'json');
  return {
    id: 'msg_pf7309',
    type: 'message',
    role: 'assistant',
    model: body.model,
    content: usesJsonTool
      ? [{ type: 'tool_use', id: 'toolu_pf7309', name: 'json', input: reply }]
      : [{ type: 'text', text: JSON.stringify(reply) }],
    stop_reason: usesJsonTool ? 'tool_use' : 'end_turn',
    stop_sequence: null,
    usage: { input_tokens: 10, output_tokens: 20 },
  };
}

function openAIResponse(body: any) {
  return {
    id: 'chatcmpl_pf7309',
    object: 'chat.completion',
    created: 0,
    model: body.model,
    choices: [{ index: 0, message: { role: 'assistant', content: JSON.stringify(reply) }, finish_reason: 'stop' }],
    usage: { prompt_tokens: 10, completion_tokens: 20, total_tokens: 30 },
  };
}

const server = createServer((request, response) => {
  const chunks: Buffer[] = [];
  request.on('data', chunk => chunks.push(chunk));
  request.on('end', () => {
    const body = JSON.parse(Buffer.concat(chunks).toString('utf8'));
    requestBodies.push(body);
    const payload = request.url?.endsWith('/chat/completions') ? openAIResponse(body) : anthropicResponse(body);
    response.writeHead(200, { 'content-type': 'application/json' });
    response.end(JSON.stringify(payload));
  });
});

beforeAll(async () => {
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  baseURL = `http://127.0.0.1:${(server.address() as AddressInfo).port}/v1`;
});

afterAll(async () => {
  server.close();
  await once(server, 'close');
});

/** The JSON schema Mastra wrote into the prompt after "matching this schema:", if any. */
function injectedSchema(body: any): any {
  const text = (body.messages ?? [])
    .flatMap((message: any) =>
      typeof message.content === 'string' ? [message.content] : message.content.map((part: any) => part.text ?? ''),
    )
    .find((part: string) => part.includes('matching this schema'));
  return text === undefined ? undefined : JSON.parse(text.slice(text.indexOf('{'), text.lastIndexOf('}') + 1));
}

const anthropic = () => createAnthropic({ apiKey: 'test-key', baseURL })('claude-haiku-5-5');
const openai = () => createOpenAI({ apiKey: 'test-key', baseURL }).chat('gpt-4o');

const recordSchema = z.object({ studyId: z.string(), fields: z.record(z.string(), z.string()) });
const populatedRecord = { studyId: 's1', fields: { population: 'adults', outcome: 'mortality' } };
const rawRecordSchema = (fields: Record<string, unknown>) => ({
  type: 'object',
  properties: { studyId: { type: 'string' }, fields },
  required: ['studyId', 'fields'],
});
const manyFields = (count: number, field: () => z.ZodType) =>
  z.object(Object.fromEntries(Array.from({ length: count }, (_, i) => [`field${i}`, field()])));
const allFields = (count: number, value: unknown) =>
  Object.fromEntries(Array.from({ length: count }, (_, i) => [`field${i}`, value]));

type Row = {
  name: string;
  model: () => any;
  schema: any;
  populated: unknown;
  providerOptions?: Record<string, any>;
  /** The record node inside the injected schema, when the row has a record. */
  record?: (schema: any) => any;
  expected: 'prompt' | 'anthropic-strict' | 'anthropic-json-tool' | 'openai-non-strict';
};

const rows: Row[] = [
  {
    name: 'z.record property',
    model: anthropic,
    schema: recordSchema,
    populated: populatedRecord,
    record: schema => schema.properties.fields,
    expected: 'prompt',
  },
  {
    name: 'z.record inside a root union',
    model: anthropic,
    schema: z.union([z.record(z.string(), z.string()), z.object({ error: z.boolean() })]),
    populated: { population: 'adults', outcome: 'mortality' },
    record: schema => schema.anyOf[0],
    expected: 'prompt',
  },
  {
    name: "raw JSON Schema { type: 'object' } record",
    model: anthropic,
    schema: rawRecordSchema({ type: 'object' }),
    populated: populatedRecord,
    record: schema => schema.properties.fields,
    expected: 'prompt',
  },
  {
    name: "raw JSON Schema { type: 'object', properties: {} } record",
    model: anthropic,
    schema: rawRecordSchema({ type: 'object', properties: {} }),
    populated: populatedRecord,
    record: schema => schema.properties.fields,
    expected: 'prompt',
  },
  {
    name: 'raw JSON Schema additionalProperties: true record',
    model: anthropic,
    schema: rawRecordSchema({ type: 'object', additionalProperties: true }),
    populated: populatedRecord,
    record: schema => schema.properties.fields,
    expected: 'prompt',
  },
  {
    name: 'raw JSON Schema additionalProperties: <schema> record',
    model: anthropic,
    schema: rawRecordSchema({ type: 'object', additionalProperties: { type: 'string' } }),
    populated: populatedRecord,
    record: schema => schema.properties.fields,
    expected: 'prompt',
  },
  {
    name: "record with structuredOutputMode 'jsonTool'",
    model: anthropic,
    schema: recordSchema,
    populated: populatedRecord,
    providerOptions: { anthropic: { structuredOutputMode: 'jsonTool' } },
    expected: 'anthropic-json-tool',
  },
  {
    name: "record with structuredOutputMode 'outputFormat'",
    model: anthropic,
    schema: recordSchema,
    populated: populatedRecord,
    providerOptions: { anthropic: { structuredOutputMode: 'outputFormat' } },
    expected: 'anthropic-strict',
  },
  {
    name: 'OpenAI record (unchanged: native, non-strict)',
    model: openai,
    schema: recordSchema,
    populated: populatedRecord,
    expected: 'openai-non-strict',
  },
  {
    name: 'more than 24 optional parameters',
    model: anthropic,
    schema: manyFields(25, () => z.string().optional()),
    populated: { field0: 'x' },
    expected: 'prompt',
  },
  {
    name: 'more than 16 union-typed parameters',
    model: anthropic,
    schema: manyFields(17, () => z.union([z.string(), z.number()])),
    populated: allFields(17, 'x'),
    expected: 'prompt',
  },
  {
    name: 'within limits, no record (native strict)',
    model: anthropic,
    schema: z.object({ summary: z.string(), tags: z.array(z.string()) }),
    populated: { summary: 's', tags: ['a'] },
    expected: 'anthropic-strict',
  },
];

describe('structured output through the real provider adapters (PF-7309)', () => {
  it.each(rows)('$name', async ({ model, schema, populated, providerOptions, record, expected }) => {
    requestBodies.length = 0;
    reply = populated;
    const agent = new Agent({ id: 'test', name: 'test', instructions: 'test', model: model() });

    const result = await agent.generate('extract', { structuredOutput: { schema }, providerOptions });

    expect(requestBodies).toHaveLength(1);
    const [body] = requestBodies;
    const strictFormat = body.output_config?.format ?? body.output_format;
    const jsonTool = (body.tools ?? []).find((tool: any) => tool.name === 'json');
    const injected = injectedSchema(body);

    switch (expected) {
      case 'prompt':
        expect(strictFormat).toBeUndefined();
        expect(jsonTool).toBeUndefined();
        expect(injected).toBeDefined();
        if (record) expect(record(injected).additionalProperties).not.toBe(false);
        break;
      case 'anthropic-strict':
        expect(strictFormat?.type).toBe('json_schema');
        expect(injected).toBeUndefined();
        break;
      case 'anthropic-json-tool':
        expect(strictFormat).toBeUndefined();
        expect(jsonTool).toBeDefined();
        expect(injected).toBeUndefined();
        break;
      case 'openai-non-strict':
        expect(body.response_format?.type).toBe('json_schema');
        expect(body.response_format.json_schema.strict).toBe(false);
        expect(body.response_format.json_schema.schema.properties.fields.additionalProperties).toEqual({
          type: 'string',
        });
        expect(injected).toBeUndefined();
        break;
    }
    expect(result.object).toEqual(populated);
  });
});
