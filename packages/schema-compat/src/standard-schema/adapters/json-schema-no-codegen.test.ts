import type { JSONSchema7 } from 'json-schema';
import { describe, expect, it } from 'vitest';

import { jsonSchemaValidatorReady, toStandardSchema } from '../../index';

// vitest.config.ts runs this file in a fresh process started with
// `--disallow-code-generation-from-strings`, before the adapter is imported.
describe('JSON Schema adapter where code generation from strings is disallowed', () => {
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
    // eslint-disable-next-line no-new-func -- proves the process really disallows it
    expect(() => new Function('return 1')).toThrow(EvalError);
    // eslint-disable-next-line no-eval -- proves the process really disallows it
    expect(() => (0, eval)('1')).toThrow(EvalError);

    await jsonSchemaValidatorReady();
    const schema = toStandardSchema(gmailFetchEmails);

    const input = { query: 'is:unread', page_token: null, max_results: 10, label_ids: ['INBOX'] };
    expect(await schema['~standard'].validate(input)).toEqual({ value: input });

    expect(await schema['~standard'].validate({ max_results: 5000, label_ids: [3] })).toEqual({
      issues: [
        { message: 'Instance type "number" is invalid. Expected "string".', path: ['label_ids', 0] },
        { message: '5000 is greater than 500.', path: ['max_results'] },
      ],
    });
  });
});
