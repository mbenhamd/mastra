/**
 * PF-7309: Anthropic's native output format is strict. `@ai-sdk/anthropic` rewrites every object to
 * `additionalProperties: false`, so a free-form record (explicit, or an object that is open by JSON
 * Schema default) can only come back as `{}`, and a schema over Anthropic's complexity limits is
 * rejected. Those schemas are injected into the prompt instead, as the caller wrote them.
 *
 * Every row drives an Agent (regular or durable) through the real provider adapter against a local
 * HTTP server and inspects the request body the adapter sends, so the assertions see exactly what
 * would reach the provider API.
 */
import { once } from 'node:events';
import { createServer } from 'node:http';
import type { AddressInfo } from 'node:net';
import { createAnthropic } from '@ai-sdk/anthropic-v6';
import { createOpenAI } from '@ai-sdk/openai-v6';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { z } from 'zod/v4';
import { EventEmitterPubSub } from '../../events/event-emitter';
import { Mastra } from '../../mastra';
import { MockStore } from '../../storage/mock';
import { createTool } from '../../tools';
import { Agent } from '../agent';
import { createDurableAgent } from '../durable/create-durable-agent';
import { globalRunRegistry } from '../durable/run-registry';

let baseURL = '';
let reply: unknown;
/** Replaces the default Anthropic reply for one test (used by the approval resume test). */
let anthropicReplyOverride: ((body: any) => unknown[] | undefined) | undefined;
const requestBodies: any[] = [];

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
  server.close();
  await once(server, 'close');
});

beforeEach(() => {
  pubsub = new EventEmitterPubSub();
  requestBodies.length = 0;
  anthropicReplyOverride = undefined;
});

afterEach(async () => {
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
const when = '2026-11-03T00:00:00.000Z';

type Expected = 'prompt' | 'anthropic-strict' | 'anthropic-json-tool' | 'openai-non-strict';
type Row = {
  name: string;
  model: () => any;
  schema: any;
  populated: unknown;
  /** The object the run returns, when it differs from the reply (e.g. dates). */
  object?: unknown;
  providerOptions?: Record<string, any>;
  inputProcessors?: any[];
  /** The record node inside the injected schema, when the row has a record. */
  record?: (schema: any) => any;
  expected: Expected;
  durable?: boolean;
};

const fallbackRows: Row[] = [
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

const jsonToolRow: Row = {
  name: "record with structuredOutputMode 'jsonTool'",
  model: anthropic,
  schema: recordSchema,
  populated: populatedRecord,
  providerOptions: { anthropic: { structuredOutputMode: 'jsonTool' } },
  expected: 'anthropic-json-tool',
};

const rows: Row[] = [
  ...fallbackRows,
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
    name: 'within limits, no record (native strict)',
    model: anthropic,
    schema: withinLimitsSchema,
    populated: { summary: 's', tags: ['a'] },
    expected: 'anthropic-strict',
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
  // Mastra's conversion represents dates and custom types; the source read must keep that.
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
  // serialization must still apply.
  ...fallbackRows.map(row => ({ ...row, name: `durable: ${row.name}`, durable: true })),
  { ...jsonToolRow, name: `durable: ${jsonToolRow.name}`, durable: true },
];

async function runRow(row: Row) {
  const agent = new Agent({
    id: 'pf7309-agent',
    name: 'pf7309-agent',
    instructions: 'test',
    model: row.model(),
    inputProcessors: row.inputProcessors,
  });
  const options = { structuredOutput: { schema: row.schema }, providerOptions: row.providerOptions } as any;
  if (!row.durable) return { object: (await agent.generate('extract', options)).object };

  const durableAgent = createDurableAgent({ agent, pubsub });
  const prepared = await durableAgent.prepare('extract', options);
  const run = await durableAgent.stream('extract', options);
  const full = await run.output.getFullOutput();
  run.cleanup();
  return {
    object: full.object,
    recorded: (prepared.workflowInput.options.structuredOutput as any)?.sourceDecision,
  };
}

describe('structured output through the real provider adapters (PF-7309)', () => {
  it.each(rows)('$name', async row => {
    reply = row.populated;

    const { object, recorded } = await runRow(row);

    expect(requestBodies).toHaveLength(1);
    const [body] = requestBodies;
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
    if (row.durable) {
      // The serialized run carries the decision recorded from the caller's schema. For the
      // jsonTool row it says the strict format cannot carry the schema; the explicit provider
      // mode then keeps the JSON tool.
      expect(recorded?.strictOutputCannotCarry).toBe(true);
      if (row.record) expect(row.record(recorded.schema).additionalProperties).not.toBe(false);
    }
    expect(object).toEqual(row.object ?? row.populated);
  });

  // A resumed durable step rebuilds its options from the serialized run. The serialized
  // `{ properties: {} }` record is closed, so re-deriving the decision there would choose the
  // strict format; the resumed request must still use the recorded prompt injection.
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
    const schema = rawRecordSchema({ type: 'object', properties: {} });

    let suspended: unknown;
    const initial = await durableAgent.stream('extract', {
      structuredOutput: { schema } as any,
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
