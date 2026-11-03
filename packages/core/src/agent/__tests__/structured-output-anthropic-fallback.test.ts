/**
 * PF-7309: Anthropic's native output format is strict. `@ai-sdk/anthropic` rewrites every object to
 * `additionalProperties: false`, so any schema that is not provably carriable (an open object, a
 * tuple, an intersection, ...) or that is over Anthropic's complexity limits goes through the prompt
 * instead, as the caller wrote it. A schema strict output provably carries stays native strict.
 *
 * Every row drives the real entry point (Agent, durable Agent, recovered durable run, or Harness)
 * through the real provider adapter against a local HTTP server and inspects the request body the
 * adapter sends, so the assertions see exactly what would reach the provider API.
 */
import { once } from 'node:events';
import { createServer } from 'node:http';
import type { AddressInfo } from 'node:net';
import { createAnthropic as createAnthropicV2 } from '@ai-sdk/anthropic-v5';
import { createAnthropic } from '@ai-sdk/anthropic-v6';
import { createOpenAI } from '@ai-sdk/openai-v6';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { z as z3 } from 'zod/v3';
import { z } from 'zod/v4';
import { EventEmitterPubSub } from '../../events/event-emitter';
import { Harness } from '../../harness/v1/harness';
import { Mastra } from '../../mastra';
import { toStandardSchema } from '../../schema';
import { InMemoryStore } from '../../storage';
import { InMemoryHarness } from '../../storage/domains/harness/inmemory';
import { InMemoryDB } from '../../storage/domains/inmemory-db';
import { MockStore } from '../../storage/mock';
import { createOutputHandler } from '../../stream/base/output-format-handlers';
import { createTool } from '../../tools';
import { Agent } from '../agent';
import type { RestartResult } from '../durable/__tests__/restart-harness';
import { createRestartScenario, DEFAULT_TIMEOUT_MS, findRow } from '../durable/__tests__/restart-harness';
import { DurableStepIds } from '../durable/constants';
import { createDurableAgent } from '../durable/create-durable-agent';
import { globalRunRegistry } from '../durable/run-registry';

let baseURL = '';
let reply: unknown;
/** Replaces the default Anthropic reply for one test (used by the approval resume test). */
let anthropicReplyOverride: ((body: any) => unknown[] | undefined) | undefined;
const requestBodies: any[] = [];
/** While set, the server leaves Anthropic requests unanswered (a model call in flight at a crash). */
let holdRequests = false;

function anthropicResponse(body: any) {
  const usesJsonTool = Array.isArray(body.tools) && body.tools.some((tool: any) => tool.name === 'json');
  const override = anthropicReplyOverride?.(body);
  const content: any[] =
    override ??
    (usesJsonTool
      ? [{ type: 'tool_use', id: 'toolu_pf7309', name: 'json', input: reply }]
      : [{ type: 'text', text: JSON.stringify(reply) }]);
  return {
    id: 'msg_pf7309',
    type: 'message',
    role: 'assistant',
    model: body.model,
    content,
    stop_reason: content.some((part: any) => part.type === 'tool_use') ? 'tool_use' : 'end_turn',
    stop_sequence: null,
    usage: { input_tokens: 10, output_tokens: 20 },
  };
}

/** The same Anthropic message as server-sent events, for adapters that stream (the durable Agent). */
function anthropicEvents(message: ReturnType<typeof anthropicResponse>): string {
  const events: Array<Record<string, unknown>> = [
    {
      type: 'message_start',
      message: { ...message, content: [], stop_reason: null, usage: { input_tokens: 10, output_tokens: 0 } },
    },
  ];
  message.content.forEach((part: any, index: number) => {
    if (part.type === 'tool_use') {
      events.push({ type: 'content_block_start', index, content_block: { ...part, input: {} } });
      events.push({
        type: 'content_block_delta',
        index,
        delta: { type: 'input_json_delta', partial_json: JSON.stringify(part.input) },
      });
    } else {
      events.push({ type: 'content_block_start', index, content_block: { type: 'text', text: '' } });
      events.push({ type: 'content_block_delta', index, delta: { type: 'text_delta', text: part.text } });
    }
    events.push({ type: 'content_block_stop', index });
  });
  events.push({
    type: 'message_delta',
    delta: { stop_reason: message.stop_reason, stop_sequence: null },
    usage: { output_tokens: 20 },
  });
  events.push({ type: 'message_stop' });
  return events.map(event => `event: ${event.type}\ndata: ${JSON.stringify(event)}\n\n`).join('');
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
    if (holdRequests) return;
    if (request.url?.endsWith('/chat/completions')) {
      response.writeHead(200, { 'content-type': 'application/json' });
      response.end(JSON.stringify(openAIResponse(body)));
    } else if (body.stream) {
      response.writeHead(200, { 'content-type': 'text/event-stream' });
      response.end(anthropicEvents(anthropicResponse(body)));
    } else {
      response.writeHead(200, { 'content-type': 'application/json' });
      response.end(JSON.stringify(anthropicResponse(body)));
    }
  });
});

let pubsub: EventEmitterPubSub;

beforeAll(async () => {
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  baseURL = `http://127.0.0.1:${(server.address() as AddressInfo).port}/v1`;
});

afterAll(async () => {
  server.closeAllConnections();
  server.close();
  await once(server, 'close');
});

beforeEach(() => {
  pubsub = new EventEmitterPubSub();
  requestBodies.length = 0;
  anthropicReplyOverride = undefined;
  holdRequests = false;
  // Router models (a cold structuring pass resolves `anthropic/...`) reach the local server too.
  vi.stubEnv('ANTHROPIC_BASE_URL', baseURL);
  vi.stubEnv('ANTHROPIC_API_KEY', 'test-key');
});

afterEach(async () => {
  vi.unstubAllEnvs();
  globalRunRegistry.clear();
  await pubsub.close();
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

/** What the adapter sent for structured output. */
function sentFormat(body: any) {
  const strictFormat = body.output_config?.format ?? body.output_format;
  const jsonTool = (body.tools ?? []).find((tool: any) => tool.name === 'json');
  return { strictFormat, jsonTool, injected: injectedSchema(body) };
}

const anthropic = () => createAnthropic({ apiKey: 'test-key', baseURL })('claude-haiku-5-5');
/** The same Anthropic adapter under another provider name. */
const renamedAnthropic = () => createAnthropic({ apiKey: 'test-key', baseURL, name: 'proxy' })('claude-haiku-5-5');
/** A model the pinned adapter sends `jsonTool` responses natively for (it rejects forced tool use). */
const anthropicOpus = () => createAnthropic({ apiKey: 'test-key', baseURL })('claude-opus-5-5');
/** A V2 (AI SDK v5) Anthropic model, whose tools lose per-tool `strict` before dispatch. */
const anthropicV2 = () => createAnthropicV2({ apiKey: 'test-key', baseURL })('claude-sonnet-5-5');
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
const withinLimitsSchema = z.object({ summary: z.string(), tags: z.array(z.string()) });
/** An input processor that swaps the step's structured-output schema before the model call. */
const replaceSchema = (schema: unknown) => ({
  id: 'replace-structured-output-schema',
  processInputStep: ({ structuredOutput }: any) => ({ structuredOutput: { ...structuredOutput, schema } }),
});
/** An input processor that assigns a new schema on the step's own structured-output object. */
const mutateSchema = (schema: unknown) => ({
  id: 'mutate-structured-output-schema',
  processInputStep: ({ structuredOutput }: any) => {
    structuredOutput.schema = schema;
    return { structuredOutput };
  },
});
const when = '2026-11-03T00:00:00.000Z';

const lookupTool =
  (strict: boolean, optionalArguments = 1) =>
  () => ({
    lookup: createTool({
      id: 'lookup',
      description: 'Look something up',
      inputSchema: z.object(
        Object.fromEntries(Array.from({ length: optionalArguments }, (_, i) => [`query${i}`, z.string().optional()])),
      ),
      ...(strict ? { strict: true } : {}),
      execute: async () => 'ok',
    }),
  });
const thirteenFields = (prefix: string) =>
  z.object(Object.fromEntries(Array.from({ length: 13 }, (_, i) => [`${prefix}${i}`, z.string()])));

/**
 * A standard schema from a library Mastra has not verified: its JSON Schema looks closed, but
 * Mastra cannot tell whether the library's own objects keep unknown keys, so strict is not provable.
 */
const unverifiedVendorSchema = {
  '~standard': {
    version: 1,
    vendor: 'acme-schema',
    validate: (value: unknown) =>
      value && typeof value === 'object' && typeof (value as { summary?: unknown }).summary === 'string'
        ? { value }
        : { issues: [{ message: 'expected { summary: string }' }] },
    jsonSchema: {
      input: () => ({
        type: 'object',
        properties: { summary: { type: 'string' } },
        required: ['summary'],
        additionalProperties: false,
      }),
      output: () => ({
        type: 'object',
        properties: { summary: { type: 'string' } },
        required: ['summary'],
        additionalProperties: false,
      }),
    },
  },
};

type Expected = 'prompt' | 'anthropic-strict' | 'anthropic-json-tool' | 'openai-non-strict';
type Via = 'agent' | 'durable' | 'recovered' | 'harness';
type Row = {
  name: string;
  model: () => any;
  schema: any;
  populated: unknown;
  /** The object the run returns, when it differs from the reply (e.g. dates). */
  object?: unknown;
  providerOptions?: Record<string, any>;
  inputProcessors?: any[];
  tools?: () => Record<string, any>;
  toolChoice?: 'none';
  /** The request carries no tool definitions (they were stripped). */
  noRequestTools?: boolean;
  /** Only the request is asserted: Mastra cannot validate this reply on any path. */
  requestOnly?: boolean;
  /** A separate structuring model (`structuredOutput.model`). */
  structuringModel?: string;
  /** The record node inside the injected schema, when the row has a record. */
  record?: (schema: any) => any;
  expected: Expected;
  via?: Via;
};

const fallbackRows: Row[] = [
  {
    name: 'plain z.object with a z.record field',
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
    name: 'raw JSON Schema typeless { properties: {} } record',
    model: anthropic,
    schema: rawRecordSchema({ properties: {} }),
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
    name: 'z.object().passthrough()',
    model: anthropic,
    schema: z.object({ studyId: z.string() }).passthrough(),
    populated: { studyId: 's1', outcome: 'mortality' },
    record: schema => schema,
    expected: 'prompt',
  },
  {
    name: 'z.object().catchall(z.string())',
    model: anthropic,
    schema: z.object({ studyId: z.string() }).catchall(z.string()),
    populated: { studyId: 's1', outcome: 'mortality' },
    record: schema => schema,
    expected: 'prompt',
  },
  {
    name: 'z.looseObject()',
    model: anthropic,
    schema: z.looseObject({ studyId: z.string() }),
    populated: { studyId: 's1', outcome: 'mortality' },
    record: schema => schema,
    expected: 'prompt',
  },
  {
    name: 'implicitly open intersection (looseObject.and(object))',
    model: anthropic,
    schema: z.looseObject({ id: z.string() }).and(z.object({ known: z.string() })),
    populated: { id: '1', known: 'k', outcome: 'mortality' },
    expected: 'prompt',
  },
  {
    name: 'tuple .rest(z.record)',
    model: anthropic,
    schema: z.object({ parts: z.tuple([z.string()]).rest(z.record(z.string(), z.string())) }),
    populated: { parts: ['a', { outcome: 'mortality' }] },
    expected: 'prompt',
  },
  {
    name: 'raw JSON Schema root tuple with additionalItems record',
    model: anthropic,
    schema: {
      type: 'array',
      items: [{ type: 'string' }],
      additionalItems: { type: 'object', additionalProperties: { type: 'string' } },
    },
    populated: { elements: ['a', { outcome: 'mortality' }] },
    object: ['a', { outcome: 'mortality' }],
    record: schema => schema.properties.elements.additionalItems,
    expected: 'prompt',
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
];

/** Schemas strict output provably carries: they stay native strict on every path. */
const converseRows: Row[] = [
  {
    name: 'converse: plain z.object within limits',
    model: anthropic,
    schema: withinLimitsSchema,
    populated: { summary: 's', tags: ['a'] },
    expected: 'anthropic-strict',
  },
  {
    name: 'converse: nested plain z.objects',
    model: anthropic,
    schema: z.object({
      study: z.object({ id: z.string(), arm: z.object({ name: z.string() }) }),
      outcomes: z.array(z.object({ label: z.string(), value: z.number().optional() })),
    }),
    populated: { study: { id: 's1', arm: { name: 'a' } }, outcomes: [{ label: 'x', value: 1 }] },
    expected: 'anthropic-strict',
  },
  {
    name: 'converse: raw JSON Schema closed everywhere',
    model: anthropic,
    schema: {
      type: 'object',
      properties: {
        studyId: { type: 'string' },
        arm: {
          type: 'object',
          properties: { name: { type: 'string' } },
          required: ['name'],
          additionalProperties: false,
        },
      },
      required: ['studyId', 'arm'],
      additionalProperties: false,
    },
    populated: { studyId: 's1', arm: { name: 'a' } },
    expected: 'anthropic-strict',
  },
  {
    name: 'converse: z.strictObject',
    model: anthropic,
    schema: z.strictObject({ summary: z.string() }),
    populated: { summary: 's' },
    expected: 'anthropic-strict',
  },
];

const jsonToolRow: Row = {
  name: "record with structuredOutputMode 'jsonTool'",
  model: anthropic,
  schema: recordSchema,
  populated: populatedRecord,
  providerOptions: { anthropic: { structuredOutputMode: 'jsonTool' } },
  expected: 'anthropic-json-tool',
};

const rawOpenRecord = rawRecordSchema({ type: 'object', properties: {} });

const rows: Row[] = [
  ...fallbackRows,
  ...converseRows,
  jsonToolRow,
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
    // The decision follows the schema the step actually sends, not the one passed to generate().
    name: 'input processor replaces a within-limits schema with a record schema',
    model: anthropic,
    schema: withinLimitsSchema,
    inputProcessors: [replaceSchema(recordSchema)],
    populated: populatedRecord,
    record: schema => schema.properties.fields,
    expected: 'prompt',
  },
  {
    name: 'input processor replaces a record schema with a within-limits schema',
    model: anthropic,
    schema: recordSchema,
    inputProcessors: [replaceSchema(withinLimitsSchema)],
    populated: { summary: 's', tags: ['a'] },
    expected: 'anthropic-strict',
  },
  {
    name: 'input processor assigns a record schema in place',
    model: anthropic,
    schema: withinLimitsSchema,
    inputProcessors: [mutateSchema(recordSchema)],
    populated: populatedRecord,
    record: schema => schema.properties.fields,
    expected: 'prompt',
  },
  // Mastra's conversion represents dates and custom types; the decision must keep that.
  {
    name: 'z.date() with a record',
    model: anthropic,
    schema: z.object({ when: z.date(), fields: z.record(z.string(), z.string()) }),
    populated: { when, fields: populatedRecord.fields },
    object: { when: new Date(when), fields: populatedRecord.fields },
    record: schema => schema.properties.fields,
    expected: 'prompt',
  },
  {
    name: 'z.coerce.date() with a record',
    model: anthropic,
    schema: z.object({ when: z.coerce.date(), fields: z.record(z.string(), z.string()) }),
    populated: { when, fields: populatedRecord.fields },
    object: { when: new Date(when), fields: populatedRecord.fields },
    record: schema => schema.properties.fields,
    expected: 'prompt',
  },
  {
    name: 'z.coerce.date() within limits, no record (native strict)',
    model: anthropic,
    schema: z.object({ when: z.coerce.date(), summary: z.string() }),
    populated: { when, summary: 's' },
    object: { when: new Date(when), summary: 's' },
    expected: 'anthropic-strict',
  },
  {
    name: 'unverified standard-schema vendor (closed-looking object)',
    model: anthropic,
    schema: unverifiedVendorSchema,
    populated: { summary: 's' },
    expected: 'prompt',
  },
  {
    name: 'array without items',
    model: anthropic,
    schema: {
      type: 'object',
      properties: { values: { type: 'array' } },
      required: ['values'],
      additionalProperties: false,
    },
    // An empty list: Mastra's Anthropic compat validation cannot traverse items of an array
    // without an `items` schema (a separate, pre-existing limitation).
    populated: { values: [] },
    expected: 'prompt',
  },
  {
    name: 'recursive $ref',
    model: anthropic,
    schema: {
      type: 'object',
      properties: { name: { type: 'string' }, children: { type: 'array', items: { $ref: '#' } } },
      required: ['name', 'children'],
      additionalProperties: false,
    },
    populated: { name: 'a', children: [{ name: 'b', children: [] }] },
    expected: 'prompt',
  },
  {
    // A root union is not provably accepted (Anthropic documents object roots only); merged into
    // one object it would also have 26 optional properties.
    name: 'root union of 2 closed objects with 13 required properties each',
    model: anthropic,
    schema: z.union([thirteenFields('a'), thirteenFields('b')]),
    populated: Object.fromEntries(Array.from({ length: 13 }, (_, i) => [`a${i}`, 'x'])),
    expected: 'prompt',
  },
  {
    // Anthropic's limits are request-wide: the strict tool's optional argument makes 25.
    name: '24 optional parameters plus a strict tool with 1 optional parameter',
    model: anthropic,
    schema: manyFields(24, () => z.string().optional()),
    tools: lookupTool(true),
    populated: { field0: 'x' },
    expected: 'prompt',
  },
  {
    name: 'converse: 24 optional parameters plus a non-strict tool',
    model: anthropic,
    schema: manyFields(24, () => z.string().optional()),
    tools: lookupTool(false),
    populated: { field0: 'x' },
    expected: 'anthropic-strict',
  },
  {
    // Processor A swaps in an unverified vendor; processor B re-wraps its identical JSON as plain
    // JSON Schema. A's downgrade must survive B.
    name: 'processor chain: unverified vendor, then the same JSON as plain JSON Schema',
    model: anthropic,
    schema: z.object({ summary: z.string() }),
    inputProcessors: [
      replaceSchema(unverifiedVendorSchema),
      {
        id: 'rewrap-as-json-schema',
        processInputStep: ({ structuredOutput }: any) => ({
          structuredOutput: {
            ...structuredOutput,
            schema: toStandardSchema(unverifiedVendorSchema['~standard'].jsonSchema.input() as any),
          },
        }),
      },
    ],
    populated: { summary: 's' },
    expected: 'prompt',
  },
  {
    name: "renamed Anthropic provider (createAnthropic({ name: 'proxy' })) with a record",
    model: renamedAnthropic,
    schema: recordSchema,
    populated: populatedRecord,
    record: schema => schema.properties.fields,
    expected: 'prompt',
  },
  {
    // The pinned adapter turns jsonTool into native output for this model, so the decision applies.
    name: "claude-opus-5-5 with structuredOutputMode 'jsonTool' and a record",
    model: anthropicOpus,
    schema: recordSchema,
    providerOptions: { anthropic: { structuredOutputMode: 'jsonTool' } },
    populated: populatedRecord,
    record: schema => schema.properties.fields,
    expected: 'prompt',
  },
  {
    // The output-format wrapping types a root enum's `result` as a string; numeric members can't
    // match. Mastra's enum handler also accepts only string results, so no path validates the reply.
    name: 'root numeric enum',
    model: anthropic,
    schema: { enum: [1, 2] },
    populated: { result: 1 },
    expected: 'prompt',
    requestOnly: true,
  },
  {
    // AI SDK v5 models lose per-tool `strict` before dispatch, so the tool isn't budgeted.
    name: 'V2 model: 24 optional parameters plus a strict tool with 1 optional parameter stays native',
    model: anthropicV2,
    schema: manyFields(24, () => z.string().optional()),
    tools: lookupTool(true),
    populated: { field0: 'x' },
    expected: 'anthropic-strict',
  },
  {
    // Equal JSON from an unverified vendor: the processor's live schema still falls back.
    name: 'input processor swaps a verified schema for an unverified vendor with equal JSON',
    model: anthropic,
    schema: z.object({ summary: z.string() }),
    inputProcessors: [replaceSchema(unverifiedVendorSchema)],
    populated: { summary: 's' },
    expected: 'prompt',
  },
  {
    // The adapter drops `additionalProperties` from an object without properties.
    name: 'nullable object without properties',
    model: anthropic,
    schema: {
      type: 'object',
      properties: { meta: { type: ['object', 'null'], additionalProperties: false } },
      required: ['meta'],
      additionalProperties: false,
    },
    populated: { meta: null },
    expected: 'prompt',
  },
  {
    name: 'object-valued enum',
    model: anthropic,
    schema: {
      type: 'object',
      properties: { value: { enum: [{ kind: 'a' }, { kind: 'b' }] } },
      required: ['value'],
      additionalProperties: false,
    },
    populated: { value: { kind: 'a' } },
    expected: 'prompt',
  },
  {
    name: 'converse: primitive enum',
    model: anthropic,
    schema: z.object({ status: z.enum(['open', 'closed']), score: z.number() }),
    populated: { status: 'open', score: 1 },
    expected: 'anthropic-strict',
  },
  {
    // Native output strips the tools under toolChoice 'none', so the strict tool is not budgeted.
    name: "toolChoice 'none' with a strict tool of 25 optional arguments stays native and drops the tool",
    model: anthropic,
    schema: withinLimitsSchema,
    tools: lookupTool(true, 25),
    toolChoice: 'none',
    populated: { summary: 's', tags: ['a'] },
    expected: 'anthropic-strict',
    noRequestTools: true,
  },
  {
    name: 'durable: unverified standard-schema vendor (closed-looking object)',
    model: anthropic,
    schema: unverifiedVendorSchema,
    populated: { summary: 's' },
    expected: 'prompt',
    via: 'durable',
  },
  {
    // A cold worker rebuilds the structuring pass from the serialized, closed-looking JSON.
    name: 'recovered durable run: structuredOutput.model structuring pass with an unverified vendor',
    model: anthropic,
    schema: unverifiedVendorSchema,
    structuringModel: 'anthropic/claude-haiku-5-5',
    populated: { summary: 's' },
    expected: 'prompt',
    via: 'recovered',
  },
  {
    name: 'Zod v3: plain z.object with a z.record field',
    model: anthropic,
    schema: z3.object({ studyId: z3.string(), fields: z3.record(z3.string(), z3.string()) }),
    populated: populatedRecord,
    record: schema => schema.properties.fields,
    expected: 'prompt',
  },
  {
    name: 'Zod v3: converse plain z.object within limits',
    model: anthropic,
    schema: z3.object({ summary: z3.string(), tags: z3.array(z3.string()) }),
    populated: { summary: 's', tags: ['a'] },
    expected: 'anthropic-strict',
  },
  {
    name: 'z.custom() with a record',
    model: anthropic,
    schema: z.object({
      id: z.custom<string>(value => typeof value === 'string'),
      fields: z.record(z.string(), z.string()),
    }),
    populated: { id: 'study-1', fields: populatedRecord.fields },
    record: schema => schema.properties.fields,
    expected: 'prompt',
  },
  // The durable Agent serializes the schema between steps; the decision recorded before
  // serialization, and the persisted schema, must keep the caller's openness.
  ...[...fallbackRows, ...converseRows, jsonToolRow].map(row => ({
    ...row,
    name: `durable: ${row.name}`,
    via: 'durable' as const,
  })),
  // A worker restart: the run is recovered from its persisted snapshot with no live registry.
  {
    name: "recovered durable run: raw { type: 'object', properties: {} } record",
    model: anthropic,
    schema: rawOpenRecord,
    populated: populatedRecord,
    record: schema => schema.properties.fields,
    expected: 'prompt',
    via: 'recovered',
  },
  {
    name: 'recovered durable run: converse plain z.object',
    model: anthropic,
    schema: withinLimitsSchema,
    populated: { summary: 's', tags: ['a'] },
    expected: 'anthropic-strict',
    via: 'recovered',
  },
  {
    // The finish-time structuring pass resolves its model from the persisted config.
    name: 'recovered durable run: structuredOutput.model structuring pass with a raw { properties: {} } record',
    model: anthropic,
    schema: rawOpenRecord,
    structuringModel: 'anthropic/claude-haiku-5-5',
    populated: populatedRecord,
    record: schema => schema.properties.fields,
    expected: 'prompt',
    via: 'recovered',
  },
  {
    name: 'Harness message({ output, sync: true }): plain z.object with a z.record field',
    model: anthropic,
    schema: recordSchema,
    populated: populatedRecord,
    record: schema => schema.properties.fields,
    expected: 'prompt',
    via: 'harness',
  },
  {
    name: 'Harness message({ output, sync: true }): converse plain z.object',
    model: anthropic,
    schema: withinLimitsSchema,
    populated: { summary: 's', tags: ['a'] },
    expected: 'anthropic-strict',
    via: 'harness',
  },
];

function makeAgent(row: Row, id = 'pf7309-agent') {
  return new Agent({
    id,
    name: id,
    instructions: 'test',
    model: row.model(),
    inputProcessors: row.inputProcessors,
    tools: row.tools?.(),
  });
}

function rowOptions(row: Row) {
  return {
    structuredOutput: {
      schema: row.schema,
      ...(row.structuringModel ? { model: row.structuringModel } : {}),
      ...(row.requestOnly ? { errorStrategy: 'warn' } : {}),
    },
    providerOptions: row.providerOptions,
    ...(row.toolChoice ? { toolChoice: row.toolChoice } : {}),
  } as any;
}

let recoveredRuns = 0;

/**
 * A worker restart: graph 1 (its own module graph, store and run registry) starts the durable run
 * and crashes while the model call is in flight; graph 2 recovers the run from the persisted rows
 * alone. The restart harness reports the recovered stream's chunks rather than the object.
 */
async function runRecovered(row: Row) {
  const runId = `pf7309-recovered-${++recoveredRuns}`;
  const scenario = createRestartScenario({
    kind: 'durable',
    runId,
    build: ({ core }) =>
      new core.Agent({
        id: 'pf7309-recovered',
        name: 'pf7309-recovered',
        instructions: 'test',
        model: row.model(),
        tools: row.tools?.(),
      }),
  });
  try {
    holdRequests = true;
    const original = await scenario.start(({ agent }) => agent.stream('extract', { runId, ...rowOptions(row) }));
    await vi.waitFor(() => expect(requestBodies).toHaveLength(1), { timeout: DEFAULT_TIMEOUT_MS });
    const checkpoint = await original.checkpoint();
    holdRequests = false;
    const recovered = await scenario.restart(checkpoint);
    const root = findRow(checkpoint, DurableStepIds.AGENTIC_LOOP, runId);
    return {
      object: undefined,
      persisted: (root?.snapshot.context as any)?.input?.options?.structuredOutput,
      recovered,
    };
  } finally {
    await scenario.stop();
  }
}

async function runRow(row: Row): Promise<{ object: unknown; persisted?: any; recovered?: RestartResult }> {
  switch (row.via ?? 'agent') {
    case 'agent':
      return { object: (await makeAgent(row).generate('extract', rowOptions(row))).object };
    case 'durable': {
      const durableAgent = createDurableAgent({ agent: makeAgent(row), pubsub });
      const prepared = await durableAgent.prepare('extract', rowOptions(row));
      globalRunRegistry.clear();
      requestBodies.length = 0;
      const run = await durableAgent.stream('extract', rowOptions(row));
      const full = await run.output.getFullOutput();
      run.cleanup();
      return { object: full.object, persisted: prepared.workflowInput.options.structuredOutput };
    }
    case 'recovered':
      return runRecovered(row);
    case 'harness': {
      const harness = new Harness({
        agents: { default: makeAgent(row, 'default') } as any,
        storage: new InMemoryStore(),
        modes: [{ id: 'default', agentId: 'default' }],
        defaultModeId: 'default',
        sessions: { storage: new InMemoryHarness({ db: new InMemoryDB() }) },
      });
      try {
        const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
        return { object: await session.message({ content: 'extract', output: row.schema, sync: true }) };
      } finally {
        await harness.shutdown();
      }
    }
  }
}

describe('structured output through the real provider adapters (PF-7309)', () => {
  it.each(rows)('$name', async row => {
    reply = row.populated;

    const { object, persisted, recovered } = await runRow(row);

    // A structuring pass adds a request; a recovered run repeats the call the crash interrupted.
    expect(requestBodies).toHaveLength(1 + (row.structuringModel ? 1 : 0) + (recovered ? 1 : 0));
    const body = requestBodies.at(-1);
    const { strictFormat, jsonTool, injected } = sentFormat(body);
    switch (row.expected) {
      case 'prompt':
        expect(strictFormat).toBeUndefined();
        expect(jsonTool).toBeUndefined();
        expect(injected).toBeDefined();
        if (row.record) expect(row.record(injected).additionalProperties).not.toBe(false);
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
    if (row.noRequestTools) expect(body.tools).toBeUndefined();
    if (persisted) {
      // The durable payload records the decision and persists a schema that keeps the caller's
      // openness, so a reloaded run (recovery, cold structuring) validates the populated reply.
      expect(persisted.sourceDecision?.strictOutputCannotCarry).toBe(row.expected !== 'anthropic-strict');
      if (row.record) expect(row.record(persisted.sourceDecision.schema).additionalProperties).not.toBe(false);
      const reloaded = await createOutputHandler({
        schema: toStandardSchema(persisted.schema),
      }).validateAndTransformFinal(JSON.stringify(row.populated));
      expect(reloaded.success).toBe(true);
    }
    if (recovered) {
      // The recovered run validated the populated reply against the reloaded schema.
      expect(recovered.executionError).toBeUndefined();
      expect(recovered.streamErrors).toEqual([]);
      expect(recovered.chunkTypes).toContain('object-result');
    } else if (!row.requestOnly) {
      expect(object).toEqual(row.object ?? row.populated);
    }
  });

  // A resumed durable step rebuilds its options from the serialized run, so it must still use the
  // recorded prompt injection for the open `{ properties: {} }` record.
  it('durable: a step resumed after a tool approval uses the recorded decision', async () => {
    reply = populatedRecord;
    const approvalTool = createTool({
      id: 'approvalTool',
      description: 'approval-gated tool',
      inputSchema: z.object({ value: z.string() }),
      requireApproval: true,
      execute: async () => 'ok',
    });
    anthropicReplyOverride = body =>
      body.messages.some(
        (message: any) =>
          Array.isArray(message.content) && message.content.some((part: any) => part.type === 'tool_result'),
      )
        ? undefined
        : [{ type: 'tool_use', id: 'toolu_approval', name: 'approvalTool', input: { value: 'x' } }];
    const agent = new Agent({
      id: 'pf7309-resume-agent',
      name: 'pf7309-resume-agent',
      instructions: 'Use the approval tool, then answer.',
      model: anthropic(),
      tools: { approvalTool },
    });
    const durableAgent = createDurableAgent({ agent, pubsub });
    new Mastra({ logger: false, storage: new MockStore(), agents: { pf7309ResumeAgent: durableAgent } });

    let suspended: unknown;
    const initial = await durableAgent.stream('extract', {
      structuredOutput: { schema: rawOpenRecord } as any,
      onSuspended: data => {
        suspended = data;
      },
    });
    await vi.waitFor(() => expect(suspended).toMatchObject({ type: 'approval' }));
    let finished: any;
    const resumed = await durableAgent.resume(
      initial.runId,
      { approved: true },
      {
        onFinish: data => {
          finished = data;
        },
      },
    );
    await vi.waitFor(() => expect(finished).toBeDefined());
    resumed.cleanup();
    initial.cleanup();

    expect(requestBodies).toHaveLength(2);
    for (const body of requestBodies) {
      const { strictFormat, jsonTool, injected } = sentFormat(body);
      expect(strictFormat).toBeUndefined();
      expect(jsonTool).toBeUndefined();
      expect(injected.properties.fields.additionalProperties).not.toBe(false);
    }
    expect(finished.object).toEqual(populatedRecord);
  });
});
