import { Mastra } from '@mastra/core/mastra';
import { MastraServer as FullServerBase } from '@mastra/server/server-adapter';
import { HARNESS_ROUTES, HARNESS_SESSION_CONTROL_ROUTES } from '@mastra/server/server-adapter/routes/harness';
import type { ServerRoute } from '@mastra/server/server-adapter/selected';
import Fastify from 'fastify';
import { describe, expect, it, vi } from 'vitest';

import { MastraServer as FullFastifyServer } from '../index';
import { MastraServer } from '../selected';

const SELECTED_PERMISSIONS_PATH = '/api/harness/code/sessions/session-1/permissions';
const CANONICAL_SESSION_CONTROL_ROUTE_KEYS = [
  'GET /harness/:name/sessions/:sessionId/permissions',
  'PATCH /harness/:name/sessions/:sessionId/permissions',
  'POST /harness/:name/sessions/:sessionId/inbox/:itemId',
];

function routeKey(route: Pick<ServerRoute, 'method' | 'path'>): string {
  return `${route.method} ${route.path}`;
}

function createMastraWithAuth() {
  const mastra = new Mastra({ logger: false });
  const originalGetServer = mastra.getServer.bind(mastra);

  mastra.getServer = () =>
    ({
      ...originalGetServer(),
      auth: {
        authenticateToken: async (token: string) => (token === 'valid-token' ? { id: 'resource-1' } : null),
        authorize: async () => true,
      },
    }) as any;

  return mastra;
}

async function createSelectedRouteApp() {
  const app = Fastify();
  const adapter = new MastraServer({
    app,
    mastra: createMastraWithAuth(),
    routeRegistry: HARNESS_SESSION_CONTROL_ROUTES,
  });

  adapter.registerContextMiddleware();
  await adapter.registerRoutes();

  return app;
}

describe('selected Fastify routes', () => {
  it('registers only the canonical routes selected from the injected domain registry', async () => {
    const app = Fastify();
    try {
      const adapter = new MastraServer({
        app,
        mastra: new Mastra({ logger: false }),
        routeRegistry: HARNESS_ROUTES,
        routes: HARNESS_SESSION_CONTROL_ROUTES,
      });
      const registerRoute = vi.spyOn(adapter, 'registerRoute');

      await adapter.registerRoutes();

      expect(registerRoute).toHaveBeenCalledTimes(HARNESS_SESSION_CONTROL_ROUTES.length);
      expect(registerRoute.mock.calls.map(call => routeKey(call[1]))).toEqual(CANONICAL_SESSION_CONTROL_ROUTE_KEYS);
      expect(adapter.getServerRoutes().map(routeKey)).toEqual(CANONICAL_SESSION_CONTROL_ROUTE_KEYS);
      expect(Object.isFrozen(adapter.getServerRoutes())).toBe(true);
      expect(adapter).toBeInstanceOf(FullServerBase);
    } finally {
      await app.close();
    }
  });

  it('preserves the full adapter root relationship with the public server base', async () => {
    const app = Fastify();
    try {
      const adapter = new FullFastifyServer({
        app,
        mastra: new Mastra({ logger: false }),
        routes: HARNESS_SESSION_CONTROL_ROUTES,
      });

      expect(adapter).toBeInstanceOf(FullServerBase);
      expect(adapter.getServerRoutes().map(routeKey)).toEqual(CANONICAL_SESSION_CONTROL_ROUTE_KEYS);
    } finally {
      await app.close();
    }
  });

  it('snapshots and freezes a routeRegistry-only selection before caller mutation', async () => {
    const app = Fastify();
    const originalRoute: ServerRoute = {
      method: 'GET',
      path: '/registry/original',
      responseType: 'json',
      requiresAuth: false,
      handler: async () => ({ selected: true }),
    };
    const laterRoute: ServerRoute = {
      method: 'GET',
      path: '/registry/later',
      responseType: 'json',
      requiresAuth: false,
      handler: async () => ({ selected: false }),
    };
    const routeRegistry: ServerRoute[] = [originalRoute];

    try {
      const adapter = new MastraServer({
        app,
        mastra: new Mastra({ logger: false }),
        routeRegistry,
      });
      const selectedRoutes = adapter.getServerRoutes();

      routeRegistry.push(laterRoute);
      adapter.registerContextMiddleware();
      await adapter.registerRoutes();

      expect(selectedRoutes).toEqual([originalRoute]);
      expect(selectedRoutes[0]).toBe(originalRoute);
      expect(Object.isFrozen(selectedRoutes)).toBe(true);
      expect(adapter.getServerRoutes()).toBe(selectedRoutes);

      const originalResponse = await app.inject({ method: 'GET', url: '/api/registry/original' });
      expect(originalResponse.statusCode).toBe(200);
      expect(originalResponse.json()).toEqual({ selected: true });

      const laterResponse = await app.inject({ method: 'GET', url: '/api/registry/later' });
      expect(laterResponse.statusCode).toBe(404);
    } finally {
      await app.close();
    }
  });

  it('authenticates a selected Harness PATCH before validating its body', async () => {
    const app = await createSelectedRouteApp();
    try {
      const response = await app.inject({
        method: 'PATCH',
        url: SELECTED_PERMISSIONS_PATH,
        payload: { action: 'grantCategory' },
      });

      expect(response.statusCode).toBe(401);
    } finally {
      await app.close();
    }
  });

  it('returns the Harness validation envelope for an authenticated malformed selected PATCH', async () => {
    const app = await createSelectedRouteApp();
    try {
      const response = await app.inject({
        method: 'PATCH',
        url: SELECTED_PERMISSIONS_PATH,
        headers: { authorization: 'Bearer valid-token' },
        payload: { action: 'grantCategory' },
      });

      expect(response.statusCode).toBe(400);
      expect(response.json()).toMatchObject({
        code: 'harness.validation',
        message: 'Invalid body',
        details: { field: 'category' },
      });
      expect(String(response.json().details.reason)).toContain('expected string');
    } finally {
      await app.close();
    }
  });

  it('does not register Harness routes outside the injected registry', async () => {
    const app = await createSelectedRouteApp();
    try {
      const response = await app.inject({
        method: 'PATCH',
        url: '/api/harness/code/sessions/session-1/mode',
        headers: { authorization: 'Bearer valid-token' },
        payload: { mode: 'code' },
      });

      expect(response.statusCode).toBe(404);
    } finally {
      await app.close();
    }
  });
});
