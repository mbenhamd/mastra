import { afterEach, describe, expect, it, vi } from 'vitest';

// PF-8016: unlike `composio.test.ts`, this file does not mock `@composio/core`.
// The real SDK asks the npm registry for its latest version unless
// `disableVersionCheck` is set, so every request is recorded here and answered
// locally; nothing reaches the network.
//
// The package runs with `isolate: false`, so a worker can already hold a
// `./composio` bound to the mocks from `composio.test.ts`. The test therefore
// loads a fresh, unmocked provider, and resets the module cache afterwards so
// later files in the worker do not inherit the real SDK.
describe('ComposioToolProvider — npm registry traffic', () => {
  afterEach(() => {
    vi.unstubAllGlobals();
    vi.resetModules();
  });

  it('makes no npm registry request while building and using its real Composio clients', async () => {
    vi.resetModules();
    vi.doUnmock('@composio/core');
    vi.doUnmock('@composio/mastra');
    const { ComposioToolProvider } = await import('./composio');

    const requested: string[] = [];
    vi.stubGlobal(
      'fetch',
      vi.fn(async (input: string | URL | Request) => {
        requested.push(input instanceof Request ? input.url : String(input));
        return new Response('{}', { status: 401 });
      }),
    );
    // Tracking off so SDK telemetry cannot send after the stub is removed; the
    // version check does not depend on it.
    const provider = new ComposioToolProvider({ apiKey: 'test-key', allowTracking: false });

    // `resolveToolsVNext` builds the Mastra-provider client and
    // `listToolkitsVNext` the raw client; both then fail on the stubbed API.
    await provider
      .resolveToolsVNext({ toolSlugs: ['warm'], toolMeta: {}, connectionId: 'ca_0' })
      .catch(() => undefined);
    await provider.listToolkitsVNext().catch(() => undefined);

    expect(requested.length).toBeGreaterThan(0);
    expect(requested.filter(url => new URL(url).hostname === 'registry.npmjs.org')).toEqual([]);
    // The cold import of the real SDK alone can pass the 5 s default.
  }, 30_000);
});
