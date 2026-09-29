import { describe, it, expect, vi } from 'vitest';

import { connectClient, serveHTTP } from './__tests__/harness.mock';
import { MCPServer } from './server';

vi.setConfig({ testTimeout: 20_000, hookTimeout: 20_000 });

/**
 * Regression tests for cross-tenant resource leakage.
 *
 * MCPServer used to cache the result of the first `resources/list` call on the
 * shared, long-lived instance and replay it to every subsequent caller, ignoring
 * per-request auth (`extra.authInfo`). For dynamic providers that scope resources
 * per user/tenant this leaked one caller's resource index to the next caller.
 *
 * The provider must be invoked per request with the current `extra`, never served
 * from a shared cache. These cases key tenancy on `extra.authInfo`; the
 * requestContext-keyed variant lives in `__tests__/per-request-providers.test.ts`.
 *
 * @see https://github.com/mastra-ai/mastra/issues/17609
 */
describe('MCPServer dynamic resource provider does not leak across callers', () => {
  const tenantOf = (extra: { authInfo?: { clientId?: string } } | undefined) =>
    extra?.authInfo?.clientId ?? 'anonymous';

  /**
   * A tenant-scoped provider: the resource list and content depend entirely on
   * `extra.authInfo`, so each caller must see only their own resources.
   */
  const createTenantServer = () => {
    const listResources = vi.fn(async ({ extra }: { extra: any }) => [
      { uri: `app://${tenantOf(extra)}/doc`, name: `Doc for ${tenantOf(extra)}`, mimeType: 'text/plain' },
    ]);
    const getResourceContent = vi.fn(async ({ uri }: { uri: string }) => ({ text: uri }));
    const resourceTemplates = vi.fn(async ({ extra }: { extra: any }) => [
      { uriTemplate: `app://${tenantOf(extra)}/{id}`, name: `Template for ${tenantOf(extra)}` },
    ]);

    const server = new MCPServer({
      name: 'tenant-server',
      version: '1.0.0',
      tools: {},
      resources: { listResources, getResourceContent, resourceTemplates },
    });

    return { server, listResources, getResourceContent, resourceTemplates };
  };

  const withTenants = async (
    fixture: ReturnType<typeof createTenantServer>,
    run: (clients: {
      a: Awaited<ReturnType<typeof connectClient>>;
      b: Awaited<ReturnType<typeof connectClient>>;
    }) => Promise<void>,
  ) => {
    const served = await serveHTTP(fixture.server, {
      auth: req => ({ token: 't', clientId: String(req.headers['x-tenant']), scopes: [] }),
    });
    try {
      const a = await connectClient(served.url, {}, { 'x-tenant': 'tenant-A' });
      const b = await connectClient(served.url, {}, { 'x-tenant': 'tenant-B' });
      try {
        await run({ a, b });
      } finally {
        await a.close();
        await b.close();
      }
    } finally {
      await served.close();
    }
  };

  it('serves each caller their own resources from resources/list', async () => {
    const fixture = createTenantServer();
    await withTenants(fixture, async ({ a, b }) => {
      expect((await a.listResources()).resources[0]?.name).toBe('Doc for tenant-A');
      expect((await b.listResources()).resources[0]?.name).toBe('Doc for tenant-B');
    });
    // Provider must be re-evaluated for each caller, not served from a shared cache.
    expect(fixture.listResources).toHaveBeenCalledTimes(2);
  });

  it('resolves resources/read against the current caller, not a cached list', async () => {
    const fixture = createTenantServer();
    await withTenants(fixture, async ({ a, b }) => {
      // Tenant A populates any would-be cache via list, then tenant B reads its own resource.
      await a.listResources();
      const result = await b.readResource({ uri: 'app://tenant-B/doc' });
      expect(result.contents[0]?.uri).toBe('app://tenant-B/doc');
      // Tenant A's resource is not resolvable for tenant B.
      await expect(b.readResource({ uri: 'app://tenant-A/doc' })).rejects.toThrow('Resource not found');
    });
    expect(fixture.getResourceContent).toHaveBeenCalledWith(expect.objectContaining({ uri: 'app://tenant-B/doc' }));
    expect(fixture.getResourceContent).not.toHaveBeenCalledWith(expect.objectContaining({ uri: 'app://tenant-A/doc' }));
  });

  it('serves each caller their own resource templates from resources/templates/list', async () => {
    const fixture = createTenantServer();
    await withTenants(fixture, async ({ a, b }) => {
      expect((await a.listResourceTemplates()).resourceTemplates[0]?.name).toBe('Template for tenant-A');
      expect((await b.listResourceTemplates()).resourceTemplates[0]?.name).toBe('Template for tenant-B');
    });
    // Provider must be re-evaluated for each caller, not served from a shared cache.
    expect(fixture.resourceTemplates).toHaveBeenCalledTimes(2);
  });

  it('never serves the dynamic provider through the public listResources() method', async () => {
    // The public method has no caller identity, so it must not consult (or cache)
    // the tenant-scoped provider: application resources are only reachable through
    // an MCP request that carries the caller's auth.
    const { server, listResources } = createTenantServer();

    await expect(server.listResources()).resolves.toEqual({ resources: [] });
    await expect(server.listResources()).resolves.toEqual({ resources: [] });

    expect(listResources).not.toHaveBeenCalled();
  });
});
