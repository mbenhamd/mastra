import type { Server, ServerResponse } from 'node:http';
import http2 from 'node:http2';
import net from 'node:net';
import { Mastra } from '@mastra/core';
import { registerApiRoute } from '@mastra/core/server';
import type { ServerRoute } from '@mastra/server/server-adapter';
import Fastify from 'fastify';
import type { FastifyInstance, FastifyServerOptions } from 'fastify';
import { afterEach, describe, expect, it } from 'vitest';
import { createAuthMiddleware, MastraServer } from '../index';

/**
 * URL-derivation tests for the trustProxy-consistent `request.host` adoption.
 *
 * The adapter derives absolute URLs in four places: `toWebRequest` (handler
 * `params.request` + auth), the MCP `startHTTP`/`startSSE` transport URLs, and
 * the custom-route dispatch URL. All four must:
 *  - honor `trustProxy` (x-forwarded-host / x-forwarded-proto) so scheme and
 *    authority come from the same trust domain,
 *  - fall back to the HTTP/2 `:authority` pseudo-header when there is no
 *    `host` header,
 *  - never contain `undefined` (or an empty authority) for fully host-less
 *    requests such as HTTP/1.0 without a Host header.
 */

interface ProbeSetup {
  app: FastifyInstance;
  adapter: MastraServer;
  mcpHttpUrls: string[];
  mcpSseUrls: string[];
}

/** Registers three synthetic probe routes that surface the derived URLs. */
async function setupProbeApp(fastifyOptions: FastifyServerOptions = {}): Promise<ProbeSetup> {
  const app = Fastify(fastifyOptions);
  const mastra = new Mastra({ logger: false });
  const adapter = new MastraServer({ app, mastra });
  adapter.registerContextMiddleware();

  // JSON probe: echoes the URL of the Web Request built by toWebRequest().
  await adapter.registerRoute(
    app,
    {
      method: 'GET',
      path: '/url-probe',
      responseType: 'json',
      handler: async (params: { request?: globalThis.Request }) => ({ url: params.request?.url ?? null }),
    } as unknown as ServerRoute,
    { prefix: '' },
  );

  // MCP HTTP probe: captures the URL the adapter passes to server.startHTTP().
  const mcpHttpUrls: string[] = [];
  await adapter.registerRoute(
    app,
    {
      method: 'GET',
      path: '/mcp-http-probe',
      responseType: 'mcp-http',
      handler: async () => ({
        server: {
          startHTTP: async ({ url, res }: { url: URL; res: ServerResponse }) => {
            mcpHttpUrls.push(url.href);
            res.writeHead(200, { 'content-type': 'application/json' });
            res.end(JSON.stringify({ ok: true }));
          },
        },
        httpPath: '/mcp-http-probe',
      }),
    } as unknown as ServerRoute,
    { prefix: '' },
  );

  // MCP SSE probe: captures the URL the adapter passes to server.startSSE().
  const mcpSseUrls: string[] = [];
  await adapter.registerRoute(
    app,
    {
      method: 'GET',
      path: '/mcp-sse-probe',
      responseType: 'mcp-sse',
      handler: async () => ({
        server: {
          startSSE: async ({ url, res }: { url: URL; res: ServerResponse }) => {
            mcpSseUrls.push(url.href);
            res.writeHead(200, { 'content-type': 'application/json' });
            res.end(JSON.stringify({ ok: true }));
          },
        },
        ssePath: '/mcp-sse-probe',
        messagePath: '/mcp-sse-probe/messages',
      }),
    } as unknown as ServerRoute,
    { prefix: '' },
  );

  return { app, adapter, mcpHttpUrls, mcpSseUrls };
}

async function listen(app: FastifyInstance): Promise<number> {
  await app.listen({ port: 0, host: '127.0.0.1' });
  const address = (app.server as Server).address();
  if (!address || typeof address === 'string') throw new Error('Failed to get server address');
  return address.port;
}

/**
 * Sends a raw HTTP/1.0 request WITHOUT a Host header. fetch/inject always add
 * a Host header, so a raw socket is the only faithful way to produce the
 * host-less case (Fastify's `request.host` is `''` for these requests).
 */
function rawHttp10Request(
  port: number,
  path: string,
  headers: Record<string, string> = {},
): Promise<{ status: number; body: string }> {
  return new Promise((resolve, reject) => {
    const socket = net.connect(port, '127.0.0.1', () => {
      const headerLines = Object.entries(headers)
        .map(([name, value]) => `${name}: ${value}\r\n`)
        .join('');
      socket.write(`GET ${path} HTTP/1.0\r\n${headerLines}\r\n`);
    });
    let data = '';
    socket.setEncoding('utf8');
    socket.on('data', chunk => {
      data += chunk;
    });
    socket.on('end', () => {
      const statusLine = data.split('\r\n', 1)[0] ?? '';
      const status = Number(statusLine.split(' ')[1]);
      const separator = data.indexOf('\r\n\r\n');
      resolve({ status, body: separator === -1 ? '' : data.slice(separator + 4) });
    });
    socket.on('error', reject);
  });
}

/** Sends a single HTTP/2 (h2c prior-knowledge) request; h2 has no host header. */
function http2Request(port: number, path: string): Promise<{ status: number; body: string }> {
  return new Promise((resolve, reject) => {
    const client = http2.connect(`http://127.0.0.1:${port}`);
    client.on('error', reject);
    const request = client.request({ ':method': 'GET', ':path': path });
    let status = 0;
    let data = '';
    request.on('response', responseHeaders => {
      status = Number(responseHeaders[':status']);
    });
    request.setEncoding('utf8');
    request.on('data', chunk => {
      data += chunk;
    });
    request.on('end', () => {
      client.close();
      resolve({ status, body: data });
    });
    request.on('error', reject);
    request.end();
  });
}

describe('Fastify Adapter — trustProxy-consistent URL derivation (request.host/request.protocol)', () => {
  let app: FastifyInstance | null = null;

  afterEach(async () => {
    if (!app) return;
    await app.close();
    app = null;
  });

  describe('host-less requests (HTTP/1.0 without a Host header)', () => {
    it('toWebRequest never derives an undefined or empty authority', async () => {
      const setup = await setupProbeApp();
      app = setup.app;
      const port = await listen(app);

      const response = await rawHttp10Request(port, '/url-probe');

      expect(response.status).toBe(200);
      const payload = JSON.parse(response.body) as { url: string };
      expect(payload.url).toBe('http://localhost/url-probe');
      expect(payload.url).not.toContain('undefined');
    });

    it('MCP startHTTP/startSSE URLs no longer degrade to http://undefined', async () => {
      const setup = await setupProbeApp();
      app = setup.app;
      const port = await listen(app);

      const httpResponse = await rawHttp10Request(port, '/mcp-http-probe');
      const sseResponse = await rawHttp10Request(port, '/mcp-sse-probe');

      expect(httpResponse.status).toBe(200);
      expect(sseResponse.status).toBe(200);
      expect(setup.mcpHttpUrls).toEqual(['http://localhost/mcp-http-probe']);
      expect(setup.mcpSseUrls).toEqual(['http://localhost/mcp-sse-probe']);
    });

    it('custom API route dispatch URL never contains undefined', async () => {
      app = Fastify();
      const mastra = new Mastra({ logger: false });
      const adapter = new MastraServer({
        app,
        mastra,
        customApiRoutes: [
          registerApiRoute('/custom-url-probe', {
            method: 'GET',
            requiresAuth: false,
            handler: async c => c.json({ url: c.req.url }),
          }),
        ],
      });
      adapter.registerContextMiddleware();
      await adapter.registerCustomApiRoutes();
      const port = await listen(app);

      const response = await rawHttp10Request(port, '/custom-url-probe');

      expect(response.status).toBe(200);
      const payload = JSON.parse(response.body) as { url: string };
      expect(payload.url).toBe('http://localhost/custom-url-probe');
      expect(payload.url).not.toContain('undefined');
    });
  });

  describe('HTTP/2 requests (authority from the :authority pseudo-header)', () => {
    it('derives URLs from :authority instead of yielding http://undefined', async () => {
      const setup = await setupProbeApp({ http2: true } as FastifyServerOptions);
      app = setup.app as unknown as FastifyInstance;
      const port = await listen(app);

      const jsonResponse = await http2Request(port, '/url-probe');
      const mcpResponse = await http2Request(port, '/mcp-http-probe');

      expect(jsonResponse.status).toBe(200);
      const payload = JSON.parse(jsonResponse.body) as { url: string };
      expect(payload.url).toBe(`http://127.0.0.1:${port}/url-probe`);
      expect(payload.url).not.toContain('undefined');

      expect(mcpResponse.status).toBe(200);
      expect(setup.mcpHttpUrls).toEqual([`http://127.0.0.1:${port}/mcp-http-probe`]);
    });
  });

  describe('trustProxy enabled (forwarded scheme and authority honored end-to-end)', () => {
    const forwardedHeaders = {
      'x-forwarded-host': 'forwarded.example.com',
      'x-forwarded-proto': 'https',
    };

    it('toWebRequest derives scheme AND authority from the forwarded trust domain', async () => {
      const setup = await setupProbeApp({ trustProxy: true });
      app = setup.app;
      const port = await listen(app);

      const response = await fetch(`http://127.0.0.1:${port}/url-probe`, { headers: forwardedHeaders });

      expect(response.status).toBe(200);
      await expect(response.json()).resolves.toEqual({ url: 'https://forwarded.example.com/url-probe' });
    });

    it('MCP startHTTP/startSSE URLs reflect the forwarded scheme and authority', async () => {
      const setup = await setupProbeApp({ trustProxy: true });
      app = setup.app;
      const port = await listen(app);

      const httpResponse = await fetch(`http://127.0.0.1:${port}/mcp-http-probe`, { headers: forwardedHeaders });
      const sseResponse = await fetch(`http://127.0.0.1:${port}/mcp-sse-probe`, { headers: forwardedHeaders });

      expect(httpResponse.status).toBe(200);
      expect(sseResponse.status).toBe(200);
      expect(setup.mcpHttpUrls).toEqual(['https://forwarded.example.com/mcp-http-probe']);
      expect(setup.mcpSseUrls).toEqual(['https://forwarded.example.com/mcp-sse-probe']);
    });

    it('custom API route dispatch URL reflects the forwarded scheme and authority', async () => {
      app = Fastify({ trustProxy: true });
      const mastra = new Mastra({ logger: false });
      const adapter = new MastraServer({
        app,
        mastra,
        customApiRoutes: [
          registerApiRoute('/custom-url-probe', {
            method: 'GET',
            requiresAuth: false,
            handler: async c => c.json({ url: c.req.url }),
          }),
        ],
      });
      adapter.registerContextMiddleware();
      await adapter.registerCustomApiRoutes();
      const port = await listen(app);

      const response = await fetch(`http://127.0.0.1:${port}/custom-url-probe`, { headers: forwardedHeaders });

      expect(response.status).toBe(200);
      await expect(response.json()).resolves.toEqual({ url: 'https://forwarded.example.com/custom-url-probe' });
    });

    it('auth middleware passes the forwarded URL to the auth provider', async () => {
      app = Fastify({ trustProxy: true });
      const capturedUrls: string[] = [];
      const mastra = new Mastra({ logger: false });
      const originalGetServer = mastra.getServer.bind(mastra);
      mastra.getServer = () =>
        ({
          ...originalGetServer(),
          auth: {
            authenticateToken: async (token: string, request: unknown) => {
              const raw = (request as { raw?: globalThis.Request }).raw;
              if (raw instanceof Request) capturedUrls.push(raw.url);
              return token === 'valid-token' ? { id: 'user-1' } : null;
            },
            authorize: async () => true,
          },
        }) as any;

      const adapter = new MastraServer({ app, mastra });
      adapter.registerContextMiddleware();
      app.get('/custom/protected', { preHandler: createAuthMiddleware({ mastra }) }, async () => ({ ok: true }));
      const port = await listen(app);

      const response = await fetch(`http://127.0.0.1:${port}/custom/protected`, {
        headers: { ...forwardedHeaders, authorization: 'Bearer valid-token' },
      });

      expect(response.status).toBe(200);
      expect(capturedUrls).toEqual(['https://forwarded.example.com/custom/protected']);
    });
  });

  describe('trustProxy disabled (forwarded headers must NOT be trusted)', () => {
    it('ignores x-forwarded-host/x-forwarded-proto in derived URLs', async () => {
      const setup = await setupProbeApp();
      app = setup.app;
      const port = await listen(app);

      const response = await fetch(`http://127.0.0.1:${port}/url-probe`, {
        headers: {
          'x-forwarded-host': 'attacker.example.com',
          'x-forwarded-proto': 'https',
        },
      });
      await fetch(`http://127.0.0.1:${port}/mcp-http-probe`, {
        headers: { 'x-forwarded-host': 'attacker.example.com' },
      });

      expect(response.status).toBe(200);
      const payload = (await response.json()) as { url: string };
      expect(payload.url).toBe(`http://127.0.0.1:${port}/url-probe`);
      expect(setup.mcpHttpUrls).toEqual([`http://127.0.0.1:${port}/mcp-http-probe`]);
    });
  });
});
