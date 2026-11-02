/**
 * PF-7309: Anthropic's strict native output format rewrites free-form objects (`z.record()`) to
 * `additionalProperties: false`, so the model can only return `{}` for them. Record-bearing schemas
 * are injected into the prompt instead, and the populated reply is validated by Mastra.
 *
 * Drives the real `@ai-sdk/anthropic` provider against a local HTTP server and inspects the request
 * body it sends, so the test sees exactly what would reach the Anthropic API.
 */
import { once } from 'node:events';
import { createServer } from 'node:http';
import type { AddressInfo } from 'node:net';
import { createAnthropic } from '@ai-sdk/anthropic-v6';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { z } from 'zod/v4';
import { Agent } from '../agent';

let baseURL = '';
let reply: unknown;
const requestBodies: any[] = [];

const server = createServer((request, response) => {
  const chunks: Buffer[] = [];
  request.on('data', chunk => chunks.push(chunk));
  request.on('end', () => {
    requestBodies.push(JSON.parse(Buffer.concat(chunks).toString('utf8')));
    response.writeHead(200, { 'content-type': 'application/json' });
    response.end(
      JSON.stringify({
        id: 'msg_pf7309',
        type: 'message',
        role: 'assistant',
        model: 'claude-haiku-5-5',
        content: [{ type: 'text', text: JSON.stringify(reply) }],
        stop_reason: 'end_turn',
        stop_sequence: null,
        usage: { input_tokens: 10, output_tokens: 20 },
      }),
    );
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

/** The JSON schema Mastra wrote into the prompt after "matching this schema:". */
function injectedSchema(body: any): any {
  const text = body.messages
    .flatMap((message: any) =>
      typeof message.content === 'string' ? [message.content] : message.content.map((part: any) => part.text ?? ''),
    )
    .find((part: string) => part.includes('matching this schema'));
  expect(text).toBeDefined();
  return JSON.parse(text.slice(text.indexOf('{'), text.lastIndexOf('}') + 1));
}

describe('Anthropic structured output with free-form records (PF-7309)', () => {
  it.each([
    {
      name: 'a record property',
      schema: z.object({ studyId: z.string(), fields: z.record(z.string(), z.string()) }),
      populated: { studyId: 's1', fields: { population: 'adults', outcome: 'mortality' } },
      record: (schema: any) => schema.properties.fields,
    },
    {
      // The Anthropic compat layer merges a root union of objects into one closed object.
      name: 'a root union of a record and an object',
      schema: z.union([z.record(z.string(), z.string()), z.object({ error: z.boolean() })]),
      populated: { population: 'adults', outcome: 'mortality' },
      record: (schema: any) => schema.anyOf[0],
    },
  ])('keeps $name open in the prompt and returns the populated record', async ({ schema, populated, record }) => {
    requestBodies.length = 0;
    reply = populated;
    const model = createAnthropic({ apiKey: 'test-key', baseURL })('claude-haiku-5-5');
    const agent = new Agent({ id: 'test', name: 'test', instructions: 'test', model });

    const result = await agent.generate('extract', { structuredOutput: { schema } });

    expect(requestBodies).toHaveLength(1);
    const [body] = requestBodies;
    expect(body.output_config?.format).toBeUndefined();
    expect(body.output_format).toBeUndefined();
    expect(body.tools).toBeUndefined();
    const sentRecord = record(injectedSchema(body));
    expect(sentRecord.additionalProperties).toEqual({ type: 'string' });
    expect(result.object).toEqual(populated);
  });
});
