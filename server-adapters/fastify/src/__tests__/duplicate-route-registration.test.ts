import { Mastra } from '@mastra/core';
import type { ServerRoute } from '@mastra/server/server-adapter';
import Fastify from 'fastify';
import type { FastifyInstance } from 'fastify';
import { afterEach, describe, expect, it } from 'vitest';
import { MastraServer } from '../index';

/**
 * The ALL-method expansion in registerRoute() tolerates duplicate route
 * registrations by matching Fastify's stable FST_ERR_DUPLICATED_ROUTE error
 * code (previously a fragile match on the English error message). The code is
 * present across the whole supported peer range (verified in fastify 5.8.4,
 * 5.8.5, 5.9.0, 5.10.0 and 5.11.0).
 */

function jsonRoute(method: string, path: string, payload: Record<string, unknown>): ServerRoute {
  return {
    method,
    path,
    responseType: 'json',
    handler: async () => payload,
  } as unknown as ServerRoute;
}

describe('Fastify Adapter — duplicate route detection via FST_ERR_DUPLICATED_ROUTE', () => {
  let app: FastifyInstance | null = null;

  afterEach(async () => {
    if (!app) return;
    await app.close();
    app = null;
  });

  it('fastify reports duplicate routes with the stable FST_ERR_DUPLICATED_ROUTE code', () => {
    // Pins the Fastify contract the adapter relies on: if Fastify ever stops
    // attaching this code to duplicate registrations, this fails loudly.
    const bare = Fastify();
    bare.route({ method: 'GET', url: '/dup', handler: async () => ({}) });

    let caught: unknown;
    try {
      bare.route({ method: 'GET', url: '/dup', handler: async () => ({}) });
    } catch (error) {
      caught = error;
    }

    expect(caught).toBeInstanceOf(Error);
    expect((caught as { code?: string }).code).toBe('FST_ERR_DUPLICATED_ROUTE');
  });

  it('skips already-declared methods when expanding an ALL route and registers the rest', async () => {
    app = Fastify();
    const mastra = new Mastra({ logger: false });
    const adapter = new MastraServer({ app, mastra });
    adapter.registerContextMiddleware();

    // GET is declared first; the ALL expansion must skip the GET duplicate but
    // still register POST/PUT/DELETE/PATCH.
    await adapter.registerRoute(app, jsonRoute('GET', '/all-probe', { from: 'get-route' }), { prefix: '' });
    await expect(
      adapter.registerRoute(app, jsonRoute('ALL', '/all-probe', { from: 'all-route' }), { prefix: '' }),
    ).resolves.toBeUndefined();
    await app.ready();

    const getResponse = await app.inject({ method: 'GET', url: '/all-probe' });
    const postResponse = await app.inject({ method: 'POST', url: '/all-probe' });

    expect(getResponse.statusCode).toBe(200);
    expect(getResponse.json()).toEqual({ from: 'get-route' });
    expect(postResponse.statusCode).toBe(200);
    expect(postResponse.json()).toEqual({ from: 'all-route' });
  });

  it('tolerates registering the same ALL route twice', async () => {
    app = Fastify();
    const mastra = new Mastra({ logger: false });
    const adapter = new MastraServer({ app, mastra });
    adapter.registerContextMiddleware();

    const route = jsonRoute('ALL', '/all-twice', { ok: true });
    await adapter.registerRoute(app, route, { prefix: '' });
    // Every expanded method is a duplicate on the second pass; none may throw.
    await expect(adapter.registerRoute(app, route, { prefix: '' })).resolves.toBeUndefined();
    await app.ready();

    const response = await app.inject({ method: 'PATCH', url: '/all-twice' });
    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual({ ok: true });
  });

  it('still propagates non-duplicate registration errors', async () => {
    app = Fastify();
    const mastra = new Mastra({ logger: false });
    const adapter = new MastraServer({ app, mastra });
    adapter.registerContextMiddleware();
    await app.ready();

    // Registering after the instance is ready fails with a DIFFERENT Fastify
    // error code (FST_ERR_INSTANCE_ALREADY_LISTENING on listen; booted
    // instances reject new routes) — the duplicate-skip must not swallow it.
    await expect(
      adapter.registerRoute(app, jsonRoute('ALL', '/too-late', { ok: false }), { prefix: '' }),
    ).rejects.toThrow();
  });
});
