import { afterAll, describe, expect, it, vi } from 'vitest';

// A browser worker created from a blob URL reports its full URL as `location.pathname`.
// `@cfworker/json-schema` turns that into an invalid URL as soon as it is imported, so this must be
// in place before the adapter is loaded.
vi.hoisted(() => {
  const location = { origin: 'http://localhost:5173', pathname: 'http://localhost:5173/abc', search: '' };
  vi.stubGlobal('self', { location });
  vi.stubGlobal('location', location);
});

import { jsonSchemaValidatorReady, toStandardSchema } from '../../index';

afterAll(() => {
  vi.unstubAllGlobals();
});

// vitest.config.ts runs this file in a fresh process started with
// `--disallow-code-generation-from-strings`, so the adapter takes the interpreter path.
describe('JSON Schema adapter in a blob worker location (validator cannot load)', () => {
  it('imports without throwing and refuses every value instead of passing it unchecked', async () => {
    await jsonSchemaValidatorReady();
    const schema = toStandardSchema({ type: 'object', properties: { coin: { type: 'string' } } });

    for (const value of [{ coin: 'BTC' }, { coin: 42 }]) {
      expect(await schema['~standard'].validate(value)).toEqual({
        issues: [
          { message: expect.stringMatching(/^Schema validation error: JSON Schema validator unavailable \(.+\)$/) },
        ],
      });
    }
  });
});
