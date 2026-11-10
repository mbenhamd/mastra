import { afterEach, describe, expect, it, vi } from 'vitest';

import { ComposioToolProvider } from './composio';

// PF-8016: the real `@composio/core` client asks the npm registry for its
// latest version when it is constructed unless `disableVersionCheck` is set.
// This file runs the real SDK (no module mocks) and records every request.
describe('ComposioToolProvider — npm egress', () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('builds both real Composio clients without asking npm for the latest @composio/core', async () => {
    const requested: string[] = [];
    vi.stubGlobal(
      'fetch',
      vi.fn(async (input: string | URL | Request) => {
        requested.push(input instanceof Request ? input.url : String(input));
        return new Response('{}', { status: 503 });
      }),
    );
    const provider = new ComposioToolProvider({ apiKey: 'test-key' });

    // `resolveToolsVNext` builds the Mastra-provider client and
    // `listToolkitsVNext` the raw client. Both then fail on the stubbed API.
    await provider
      .resolveToolsVNext({ toolSlugs: ['warm'], toolMeta: {}, connectionId: 'ca_0' })
      .catch(() => undefined);
    await provider.listToolkitsVNext().catch(() => undefined);

    expect(requested.length).toBeGreaterThan(0);
    expect(requested.filter(url => url.startsWith('https://registry.npmjs.org/'))).toEqual([]);
  });
});
