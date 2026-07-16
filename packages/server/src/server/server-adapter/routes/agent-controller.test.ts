import { describe, expect, it } from 'vitest';

import { AGENT_CONTROLLER_ROUTES } from './agent-controller';
import { HARNESS_ROUTES } from './harness';
import { SERVER_ROUTES } from '.';

const routeKey = (route: { method: string; path: string }) => `${route.method} ${route.path}`;

describe('agent-controller routes', () => {
  it('serves every route under /agent-controller with AgentController tags and agent-controller permissions', () => {
    expect(AGENT_CONTROLLER_ROUTES.length).toBeGreaterThan(0);
    for (const route of AGENT_CONTROLLER_ROUTES) {
      expect(route.path === '/agent-controller' || route.path.startsWith('/agent-controller/')).toBe(true);
      expect(route.openapi?.tags ?? []).toContain('AgentController');
      expect(route.openapi?.tags ?? []).not.toContain('Harness');
      expect(route.requiresAuth).toBe(true);
      const perms = Array.isArray(route.requiresPermission)
        ? route.requiresPermission
        : route.requiresPermission
          ? [route.requiresPermission]
          : [];
      expect(perms.length).toBeGreaterThan(0);
      for (const perm of perms) {
        expect(typeof perm).toBe('string');
        expect(typeof perm === 'string' && perm.startsWith('agent-controller:')).toBe(true);
      }
    }
  });

  it('preserves the independent Harness v1 route contract', () => {
    expect(HARNESS_ROUTES.length).toBeGreaterThan(0);
    const publicInboundRouteKey = 'POST /harness/:name/channels/:channelId/inbound';
    expect(HARNESS_ROUTES.filter(route => route.requiresAuth === false).map(routeKey)).toEqual([publicInboundRouteKey]);

    for (const route of HARNESS_ROUTES) {
      expect(route.path === '/harness' || route.path.startsWith('/harness/')).toBe(true);
      expect(route.openapi?.tags ?? []).toContain('Harness');
      expect(route.openapi?.tags ?? []).not.toContain('AgentController');
      expect(route.requiresPermission).toBeUndefined();

      if (routeKey(route) === publicInboundRouteKey) {
        expect(route.requiresAuth).toBe(false);
        expect(route.harnessAuth).toBeUndefined();
      } else {
        expect(route.requiresAuth).toBe(true);
        expect(route.harnessAuth?.clientRoute).toBe(true);
      }
    }
  });

  it('registers the AgentController and Harness route families exactly once without aliases', () => {
    const registeredAgentControllerRoutes = SERVER_ROUTES.filter(
      route => route.path === '/agent-controller' || route.path.startsWith('/agent-controller/'),
    );
    const registeredHarnessRoutes = SERVER_ROUTES.filter(
      route => route.path === '/harness' || route.path.startsWith('/harness/'),
    );

    expect(registeredAgentControllerRoutes).toEqual(AGENT_CONTROLLER_ROUTES);
    expect(registeredHarnessRoutes).toEqual(HARNESS_ROUTES);

    const agentControllerRouteKeys = new Set(AGENT_CONTROLLER_ROUTES.map(routeKey));
    const harnessRouteKeys = new Set(HARNESS_ROUTES.map(routeKey));
    expect(agentControllerRouteKeys.size).toBe(AGENT_CONTROLLER_ROUTES.length);
    expect(harnessRouteKeys.size).toBe(HARNESS_ROUTES.length);
    expect([...agentControllerRouteKeys].filter(key => harnessRouteKeys.has(key))).toEqual([]);
  });
});
