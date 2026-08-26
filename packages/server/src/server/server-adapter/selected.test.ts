import type { Mastra } from '@mastra/core/mastra';
import { describe, expect, it, vi } from 'vitest';

import { HARNESS_ROUTES, HARNESS_SESSION_CONTROL_ROUTES } from './routes/harness';
import { MastraServer } from './selected';
import type { ServerRoute, ServerRouteSelector } from './selected';

class TestSelectedMastraServer extends MastraServer<Record<string, never>, unknown, unknown> {
  stream = vi.fn();
  getParams = vi.fn();
  sendResponse = vi.fn();
  registerRoute = vi.fn();
  registerContextMiddleware = vi.fn();
  registerAuthMiddleware = vi.fn();
  registerHttpLoggingMiddleware = vi.fn();
}

function createAdapter(options: { routeRegistry?: readonly ServerRoute[]; routes?: ServerRouteSelector } = {}) {
  return new TestSelectedMastraServer({
    app: {},
    mastra: {
      getServer: () => undefined,
      setMastraServer: vi.fn(),
    } as unknown as Mastra,
    routeRegistry: options.routeRegistry ?? HARNESS_ROUTES,
    routes: options.routes,
  });
}

function routeKey(route: Pick<ServerRoute, 'method' | 'path'>): string {
  return `${route.method} ${route.path}`;
}

describe('selected server routes', () => {
  it('exports a frozen canonical Harness session-control tuple', () => {
    expect(Object.isFrozen(HARNESS_SESSION_CONTROL_ROUTES)).toBe(true);
    expect(HARNESS_SESSION_CONTROL_ROUTES.map(routeKey)).toEqual([
      'POST /harness/:name/sessions/:sessionId/inbox/:itemId',
      'GET /harness/:name/sessions/:sessionId/permissions',
      'PATCH /harness/:name/sessions/:sessionId/permissions',
    ]);

    for (const route of HARNESS_SESSION_CONTROL_ROUTES) {
      expect(HARNESS_ROUTES.find(candidate => routeKey(candidate) === routeKey(route))).toBe(route);
      expect(route.requiresAuth).toBe(true);
      expect(route.harnessAuth).toEqual({ clientRoute: true });
    }
  });

  it('canonicalizes selected arrays and preserves the injected registry order', () => {
    const canonicalRoute = HARNESS_SESSION_CONTROL_ROUTES[0];
    const laterCanonicalRoute = HARNESS_SESSION_CONTROL_ROUTES[2];
    const spoofedRoute = {
      ...canonicalRoute,
      requiresAuth: false,
      handler: vi.fn(async () => ({ spoofed: true })),
    } as ServerRoute;
    const routeRegistry = [...HARNESS_SESSION_CONTROL_ROUTES];
    const adapter = createAdapter({ routeRegistry, routes: [laterCanonicalRoute, spoofedRoute] });

    routeRegistry.length = 0;
    const selectedRoutes = adapter.getServerRoutes();

    expect(selectedRoutes).toEqual([canonicalRoute, laterCanonicalRoute]);
    expect(selectedRoutes[0]).toBe(canonicalRoute);
    expect(selectedRoutes[0]).not.toBe(spoofedRoute);
    expect(selectedRoutes[1]).toBe(laterCanonicalRoute);
    expect(Object.isFrozen(selectedRoutes)).toBe(true);
    expect(adapter.getServerRoutes()).toBe(selectedRoutes);
  });

  it('evaluates predicates only against the injected registry and preserves its order', () => {
    const predicate = vi.fn((route: ServerRoute) => route.method !== 'PATCH');
    const adapter = createAdapter({ routeRegistry: HARNESS_SESSION_CONTROL_ROUTES, routes: predicate });

    expect(predicate.mock.calls.map(([route]) => route)).toEqual(HARNESS_SESSION_CONTROL_ROUTES);
    expect(adapter.getServerRoutes()).toEqual([HARNESS_SESSION_CONTROL_ROUTES[0], HARNESS_SESSION_CONTROL_ROUTES[1]]);
    expect(Object.isFrozen(adapter.getServerRoutes())).toBe(true);
  });

  it('rejects unknown or duplicate routes without loading another domain registry', () => {
    const canonicalRoute = HARNESS_SESSION_CONTROL_ROUTES[0];
    const unknownRoute = { ...canonicalRoute, path: '/not-in-the-selected-registry' } as ServerRoute;

    expect(() => createAdapter({ routeRegistry: HARNESS_SESSION_CONTROL_ROUTES, routes: [unknownRoute] })).toThrow(
      'routes selector can only include built-in Mastra server routes; unknown route: POST /not-in-the-selected-registry',
    );
    expect(() =>
      createAdapter({ routeRegistry: HARNESS_SESSION_CONTROL_ROUTES, routes: [canonicalRoute, canonicalRoute] }),
    ).toThrow(
      'routes selector contains duplicate built-in route: POST /harness/:name/sessions/:sessionId/inbox/:itemId',
    );
    expect(() => createAdapter({ routeRegistry: [canonicalRoute, canonicalRoute] })).toThrow(
      'route registry contains duplicate built-in route: POST /harness/:name/sessions/:sessionId/inbox/:itemId',
    );
  });
});
