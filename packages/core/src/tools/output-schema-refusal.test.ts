import { describe, expect, it, vi } from 'vitest';
import { noopLogger } from '../logger';
import { RequestContext } from '../request-context';
import { JSON_SCHEMA_REFUSAL, toStandardSchema } from '../schema';
import { createTool } from './tool';
import { CoreToolBuilder } from './tool-builder/builder';

const REFUSAL = 'not at "/properties/status/not" is not a supported keyword, so the schema is refused';

/**
 * An output schema as the JSON Schema adapter wraps it where code generation from strings is
 * disallowed and the schema is refused. This process allows code generation, where the adapter
 * validates with Ajv and refuses no schema up front, so the refusal stands in for that path.
 */
function refusedOutputSchema() {
  const schema = toStandardSchema({ type: 'object', properties: { status: { not: { const: 'failed' } } } });
  Object.defineProperty(schema, JSON_SCHEMA_REFUSAL, { value: () => REFUSAL });
  return schema;
}

const inputSchema = toStandardSchema({ type: 'object', properties: { to: { type: 'string' } } });
const notRun = (toolId: string) =>
  `Tool output schema is refused for ${toolId}, so the tool was not run:\n- root: Schema validation error: ${REFUSAL}`;

describe('a tool whose output schema is refused', () => {
  it('does not run a Mastra tool', async () => {
    const execute = vi.fn(async () => ({ status: 'sent' }));
    const sendEmail = createTool({
      id: 'send_email',
      description: 'Send an email',
      inputSchema,
      outputSchema: refusedOutputSchema(),
      execute,
    });

    const result = await sendEmail.execute!({ to: 'a@example.test' }, {});

    expect(result).toMatchObject({ error: true, message: notRun('send_email') });
    expect(execute).not.toHaveBeenCalled();
  });

  it('does not run an AI SDK tool', async () => {
    const execute = vi.fn(async () => ({ status: 'sent' }));
    const coreTool = new CoreToolBuilder({
      originalTool: { description: 'Send an email', inputSchema, outputSchema: refusedOutputSchema(), execute },
      options: { name: 'send_email', logger: noopLogger, requestContext: new RequestContext() },
    }).build();

    const result = await coreTool.execute!({ to: 'a@example.test' }, { toolCallId: 'call-1', messages: [] });

    expect(result).toMatchObject({ error: true, message: notRun('send_email') });
    expect(execute).not.toHaveBeenCalled();
  });
});
