import { Mastra } from '@mastra/core';
import { Hono } from 'hono';
import { describe, expect, it } from 'vitest';

import { MASTRA_FRAMEWORK_PUBLIC_KEY, MastraServer } from '../index';
import type { HonoVariables } from '../index';

describe('framework-public authentication boundary', () => {
  it('still runs general middleware on a framework-public route', async () => {
    const mastra = new Mastra({ logger: false });
    const app = new Hono();
    const adapter = new MastraServer({ app, mastra });
    const calls: string[] = [];

    adapter.registerContextMiddleware();
    app.use('*', async (_c, next) => {
      calls.push('audit');
      await next();
    });
    app.get('/api/auth/capabilities', c => {
      calls.push('handler');
      return c.json({ capabilities: ['sso', 'credentials'] });
    });

    const response = await app.request('http://localhost/api/auth/capabilities');

    expect(response.status).toBe(200);
    expect(calls).toEqual(['audit', 'handler']);
  });

  it('does not let public-route classification bypass host security middleware', async () => {
    const mastra = new Mastra({ logger: false });
    const app = new Hono();
    const adapter = new MastraServer({ app, mastra });

    adapter.registerContextMiddleware();
    app.use('*', async () => new Response('rate limited', { status: 429 }));
    app.get('/api/auth/capabilities', c => c.json({ ok: true }));

    const response = await app.request('http://localhost/api/auth/capabilities');

    expect(response.status).toBe(429);
    expect(await response.text()).toBe('rate limited');
  });

  it('allows only framework authentication to skip itself on public routes', async () => {
    const mastra = new Mastra({ logger: false });
    const app = new Hono<{ Variables: HonoVariables }>();
    const adapter = new MastraServer({ app, mastra });
    const audited: string[] = [];

    adapter.registerContextMiddleware();
    app.use('*', async (c, next) => {
      audited.push(c.req.path);
      await next();
    });
    app.use('*', async (c, next) => {
      if (c.get(MASTRA_FRAMEWORK_PUBLIC_KEY)) return next();
      return c.text('unauthorized', 401);
    });
    app.get('/api/auth/capabilities', c => c.text('public'));
    app.get('/api/agents', c => c.text('protected'));

    const publicResponse = await app.request('http://localhost/api/auth/capabilities');
    const protectedResponse = await app.request('http://localhost/api/agents');

    expect(publicResponse.status).toBe(200);
    expect(await publicResponse.text()).toBe('public');
    expect(protectedResponse.status).toBe(401);
    expect(audited).toEqual(['/api/auth/capabilities', '/api/agents']);
  });
});
