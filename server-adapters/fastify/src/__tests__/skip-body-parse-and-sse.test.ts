/**
 * Node/Fastify integration tests for two harness HTTP features that, until now,
 * were only exercised against the Hono server-adapter:
 *
 *  1. The channel-ingress webhook auth path — `route.skipBodyParse` + raw-body
 *     capture (for HMAC-over-exact-bytes verification) + a per-route `bodyLimit`
 *     (`route.maxBodySize`) that is decoupled from the adapter-wide bodyLimit.
 *     PapersFlow runs the harness under Fastify (not Hono), and Fastify auto-parses
 *     JSON via a content-type parser, so the raw bytes need explicit handling.
 *
 *  2. The SSE session-events route (`responseType: 'datastream-response'`) — a Web
 *     `ReadableStream` streamed to the Node response, plus a heartbeat interval and
 *     a subscribe/unsubscribe lifecycle that MUST be torn down when the client
 *     disconnects. Fastify's client-disconnect semantics (`req.raw 'close'` +
 *     the abort controller wired in `createContextMiddleware`) differ from Hono's
 *     `stream.onAbort`, so this verifies the cleanup actually fires under Node.
 *
 * These spin a real Fastify instance (`app.listen({ port: 0 })`) and drive it over
 * the loopback interface, mirroring the existing Fastify adapter test style. They
 * import the same minimal set as stream-disconnect.test.ts (fastify + @mastra/core
 * + @mastra/server/server-adapter) so they do not depend on @mastra/mcp being built.
 */
import { createHmac } from 'node:crypto';
import { Mastra } from '@mastra/core';
import type { ServerRoute } from '@mastra/server/server-adapter';
import Fastify from 'fastify';
import type { FastifyInstance } from 'fastify';
import { afterEach, describe, expect, it } from 'vitest';

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function waitFor(assertion: () => boolean, timeout = 1000): Promise<void> {
  const start = Date.now();
  while (!assertion()) {
    if (Date.now() - start > timeout) {
      throw new Error('Timed out waiting for assertion');
    }
    await sleep(2);
  }
}

let app: FastifyInstance | null = null;

afterEach(async () => {
  if (app) {
    app.server.closeAllConnections?.();
    await app.close();
    app = null;
  }
});

/**
 * Build a real Fastify adapter + app, register the context middleware (which
 * installs the JSON content-type parser + abort wiring) and the given routes.
 * `bodyLimitOptions` is intentionally NOT passed, which proves `route.maxBodySize`
 * is honored standalone (the production gap: PapersFlow constructs the adapter
 * without a global bodyLimit but relies on the webhook route's own 1 MiB cap).
 */
async function setupAdapter(routes: ServerRoute[]): Promise<{ app: FastifyInstance; address: string }> {
  const { MastraServer } = await import('../index');
  const fastifyApp = Fastify();
  const mastra = new Mastra({});
  const adapter = new MastraServer({
    app: fastifyApp,
    mastra,
    prefix: '',
    // NOTE: no bodyLimitOptions — proves route.maxBodySize is honored standalone.
  });

  adapter.registerContextMiddleware();
  for (const route of routes) {
    await adapter.registerRoute(fastifyApp, route, { prefix: '' });
  }
  const address = await fastifyApp.listen({ port: 0 });
  return { app: fastifyApp, address };
}

interface CapturedWebhook {
  rawBody?: Uint8Array | string;
  requestBody: unknown;
  contentType?: string;
  signatureValid?: boolean;
}

/**
 * A skipBodyParse webhook route that verifies an HMAC over the EXACT raw bytes,
 * exactly as the channel-ingress route does (verification owns parsing).
 */
function makeWebhookRoute(
  path: string,
  secret: string,
  captured: { value?: CapturedWebhook },
  opts: { maxBodySize?: number } = {},
): ServerRoute {
  return {
    method: 'POST',
    path,
    responseType: 'json',
    requiresAuth: false,
    skipBodyParse: true,
    ...(opts.maxBodySize !== undefined ? { maxBodySize: opts.maxBodySize } : {}),
    handler: (params: any) => {
      const rawBody = params.rawBody as Uint8Array | string | undefined;
      const providedSig = params.getHeader?.('x-signature') as string | undefined;
      let signatureValid = false;
      if (rawBody !== undefined && providedSig) {
        const bytes = typeof rawBody === 'string' ? Buffer.from(rawBody, 'utf8') : Buffer.from(rawBody);
        const expected = createHmac('sha256', secret).update(bytes).digest('hex');
        // Constant-time-ish compare is irrelevant for a test; equality suffices.
        signatureValid = expected === providedSig;
      }
      captured.value = {
        rawBody,
        requestBody: params.requestBody,
        contentType: params.getHeader?.('content-type'),
        signatureValid,
      };
      if (!signatureValid) {
        const err = new Error('invalid signature') as Error & { status?: number };
        err.status = 401;
        throw err;
      }
      return { ok: true };
    },
  } as unknown as ServerRoute;
}

describe('Fastify adapter — skipBodyParse raw-body capture + HMAC (channel webhook ingress)', () => {
  it('delivers the EXACT raw bytes to the handler so an HMAC signature verifies', async () => {
    const secret = 'webhook-secret';
    const captured: { value?: CapturedWebhook } = {};
    const setup = await setupAdapter([makeWebhookRoute('/webhook', secret, captured, { maxBodySize: 1024 * 1024 })]);
    app = setup.app;

    const signedBody = JSON.stringify({ event: 'message', token: 'abc' });
    const signature = createHmac('sha256', secret).update(Buffer.from(signedBody, 'utf8')).digest('hex');

    const res = await fetch(`${setup.address}/webhook`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'x-signature': signature },
      body: signedBody,
    });

    expect(res.status).toBe(200);
    expect(captured.value).toBeDefined();
    const rawBody = captured.value!.rawBody;
    // Raw bytes captured as a Uint8Array (Fastify parses the body as a Buffer).
    expect(rawBody).toBeInstanceOf(Uint8Array);
    expect(new TextDecoder().decode(rawBody as Uint8Array)).toBe(signedBody);
    // Verification owns parsing: the adapter did NOT pre-parse into requestBody.
    expect(captured.value!.requestBody).toBeUndefined();
    expect(captured.value!.signatureValid).toBe(true);
  });

  it('a tampered body fails HMAC verification (signature over the wrong bytes -> 401)', async () => {
    const secret = 'webhook-secret';
    const captured: { value?: CapturedWebhook } = {};
    const setup = await setupAdapter([makeWebhookRoute('/webhook', secret, captured, { maxBodySize: 1024 * 1024 })]);
    app = setup.app;

    const originalBody = JSON.stringify({ event: 'message', token: 'abc' });
    const signature = createHmac('sha256', secret).update(Buffer.from(originalBody, 'utf8')).digest('hex');
    // Attacker mutates the body in flight; the signature no longer matches.
    const tamperedBody = JSON.stringify({ event: 'message', token: 'EVIL' });

    const res = await fetch(`${setup.address}/webhook`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'x-signature': signature },
      body: tamperedBody,
    });

    expect(res.status).toBe(401);
    // The handler still received the exact (tampered) bytes; it just rejected them.
    expect(new TextDecoder().decode(captured.value!.rawBody as Uint8Array)).toBe(tamperedBody);
    expect(captured.value!.signatureValid).toBe(false);
  });

  it('forwards a signed-but-NOT-strict-JSON body without a 400 short-circuit (skipBodyParse)', async () => {
    const secret = 'webhook-secret';
    const captured: { value?: CapturedWebhook } = {};
    const setup = await setupAdapter([makeWebhookRoute('/webhook', secret, captured, { maxBodySize: 1024 * 1024 })]);
    app = setup.app;

    // A provider-signed payload that declares JSON but is NOT strict JSON. With the
    // default Fastify JSON parser this would 400 (or 500) before the handler runs.
    const signedBody = '{"event":"message","ts":not-json,*raw*';
    const signature = createHmac('sha256', secret).update(Buffer.from(signedBody, 'utf8')).digest('hex');

    const res = await fetch(`${setup.address}/webhook`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'x-signature': signature },
      body: signedBody,
    });

    // No malformed-JSON short-circuit: the handler ran and verified the raw bytes.
    expect(res.status).toBe(200);
    expect(new TextDecoder().decode(captured.value!.rawBody as Uint8Array)).toBe(signedBody);
    expect(captured.value!.requestBody).toBeUndefined();
    expect(captured.value!.signatureValid).toBe(true);
  });

  it('a NON-skipBodyParse route still rejects malformed JSON and parses valid JSON (unchanged)', async () => {
    const captured: { value?: unknown } = {};
    const normalRoute: ServerRoute = {
      method: 'POST',
      path: '/normal',
      responseType: 'json',
      requiresAuth: false,
      handler: (params: any) => {
        captured.value = params.requestBody;
        return { ok: true };
      },
    } as unknown as ServerRoute;
    const setup = await setupAdapter([normalRoute]);
    app = setup.app;

    const bad = await fetch(`${setup.address}/normal`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: '{"broken": ',
    });
    // A normal (non-skipBodyParse) route still rejects malformed JSON at the parser
    // and the handler never runs. (The content-type parser surfaces a raw SyntaxError,
    // which Fastify maps to 500 — pre-existing behavior; the point is that, unlike a
    // skipBodyParse route, the malformed body does NOT reach the handler.)
    expect(bad.status).toBeGreaterThanOrEqual(400);
    expect(captured.value).toBeUndefined();

    const good = await fetch(`${setup.address}/normal`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ hello: 'world' }),
    });
    expect(good.status).toBe(200);
    expect(captured.value).toEqual({ hello: 'world' });
  });
});

describe('Fastify adapter — per-route bodyLimit (route.maxBodySize) decoupled from global', () => {
  it('returns 413 on an oversized body from route.maxBodySize with NO global bodyLimitOptions', async () => {
    const secret = 'webhook-secret';
    const captured: { value?: CapturedWebhook } = {};
    const maxBodySize = 1024; // 1 KiB cap
    const setup = await setupAdapter([makeWebhookRoute('/webhook', secret, captured, { maxBodySize })]);
    app = setup.app;

    const oversized = 'x'.repeat(maxBodySize + 1);
    const res = await fetch(`${setup.address}/webhook`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'content-length': String(oversized.length) },
      body: oversized,
    });

    expect(res.status).toBe(413);
    // Fastify rejects before the body is buffered into the handler.
    expect(captured.value).toBeUndefined();
  });

  it('allows an under-cap body through the per-route bodyLimit (no false 413)', async () => {
    const secret = 'webhook-secret';
    const captured: { value?: CapturedWebhook } = {};
    const maxBodySize = 1024;
    const setup = await setupAdapter([makeWebhookRoute('/webhook', secret, captured, { maxBodySize })]);
    app = setup.app;

    const body = JSON.stringify({ small: true });
    const signature = createHmac('sha256', secret).update(Buffer.from(body, 'utf8')).digest('hex');
    const res = await fetch(`${setup.address}/webhook`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'x-signature': signature },
      body,
    });

    expect(res.status).toBe(200);
    expect(new TextDecoder().decode(captured.value!.rawBody as Uint8Array)).toBe(body);
  });
});

interface SseLifecycle {
  subscribed: number;
  unsubscribed: number;
  heartbeatCleared: boolean;
}

/**
 * An SSE route shaped like GET_HARNESS_SESSION_EVENTS_ROUTE: responseType
 * 'datastream-response', a Web ReadableStream body, a heartbeat interval, and a
 * subscribe/unsubscribe lifecycle whose cleanup MUST run on client disconnect.
 * It records the lifecycle so the test can assert nothing leaks.
 */
function makeSseRoute(path: string, lifecycle: SseLifecycle, heartbeatIntervalMs = 50): ServerRoute {
  return {
    method: 'GET',
    path,
    responseType: 'datastream-response',
    requiresAuth: false,
    handler: (params: any) => {
      const abortSignal = params.abortSignal as AbortSignal | undefined;
      let heartbeat: ReturnType<typeof setInterval> | undefined;
      let closed = false;
      // Simulate session.subscribe() returning an unsubscribe fn.
      lifecycle.subscribed += 1;
      const unsubscribe = () => {
        lifecycle.unsubscribed += 1;
      };
      const cleanup = () => {
        if (closed) return;
        closed = true;
        if (heartbeat !== undefined) {
          clearInterval(heartbeat);
          heartbeat = undefined;
          lifecycle.heartbeatCleared = true;
        }
        unsubscribe();
      };

      const stream = new ReadableStream<Uint8Array>({
        start(controller) {
          const abortCleanup = () => {
            cleanup();
            try {
              controller.close();
            } catch {}
          };
          abortSignal?.addEventListener('abort', abortCleanup, { once: true });
          if (abortSignal?.aborted) {
            abortCleanup();
            return;
          }
          // Emit one event so the client sees a live stream, then heartbeat.
          controller.enqueue(new TextEncoder().encode('data: {"type":"open"}\n\n'));
          heartbeat = setInterval(() => {
            if (closed) return;
            try {
              controller.enqueue(new TextEncoder().encode(': keep-alive\n\n'));
            } catch {
              cleanup();
            }
          }, heartbeatIntervalMs);
          (heartbeat as { unref?: () => void }).unref?.();
        },
        cancel() {
          cleanup();
        },
      });

      return new Response(stream, {
        status: 200,
        headers: {
          'content-type': 'text/event-stream; charset=utf-8',
          'cache-control': 'no-cache, no-transform',
          connection: 'keep-alive',
        },
      });
    },
  } as unknown as ServerRoute;
}

describe('Fastify adapter — SSE (datastream-response) streaming + client-disconnect cleanup', () => {
  it('streams events to the client and tears down subscription + heartbeat on client disconnect', async () => {
    const lifecycle: SseLifecycle = { subscribed: 0, unsubscribed: 0, heartbeatCleared: false };
    const setup = await setupAdapter([makeSseRoute('/events', lifecycle)]);
    app = setup.app;

    const controller = new AbortController();
    const res = await fetch(`${setup.address}/events`, {
      method: 'GET',
      headers: { accept: 'text/event-stream' },
      signal: controller.signal,
    });

    expect(res.status).toBe(200);
    expect(res.headers.get('content-type')).toContain('text/event-stream');
    expect(lifecycle.subscribed).toBe(1);

    // Read the first streamed chunk to confirm the stream is live.
    const reader = res.body!.getReader();
    const first = await reader.read();
    expect(first.done).toBe(false);
    expect(new TextDecoder().decode(first.value!)).toContain('data: {"type":"open"}');

    // Subscription is still live (not yet cleaned up).
    expect(lifecycle.unsubscribed).toBe(0);

    // Client disconnects.
    await reader.cancel().catch(() => {});
    controller.abort();

    // The handler's cleanup must run: subscription removed AND heartbeat cleared.
    await waitFor(() => lifecycle.unsubscribed === 1 && lifecycle.heartbeatCleared);
    expect(lifecycle.unsubscribed).toBe(1);
    expect(lifecycle.heartbeatCleared).toBe(true);
  });

  it('passes the handler abortSignal that fires on client disconnect (no leaked interval)', async () => {
    const lifecycle: SseLifecycle = { subscribed: 0, unsubscribed: 0, heartbeatCleared: false };
    // Long heartbeat: if cleanup did NOT clear it, the interval would leak.
    const setup = await setupAdapter([makeSseRoute('/events', lifecycle, 10_000)]);
    app = setup.app;

    const controller = new AbortController();
    const res = await fetch(`${setup.address}/events`, {
      method: 'GET',
      headers: { accept: 'text/event-stream' },
      signal: controller.signal,
    });
    const reader = res.body!.getReader();
    await reader.read(); // pull the open frame so start() has run + heartbeat scheduled

    await reader.cancel().catch(() => {});
    controller.abort();

    await waitFor(() => lifecycle.heartbeatCleared && lifecycle.unsubscribed === 1);
    expect(lifecycle.heartbeatCleared).toBe(true);
    expect(lifecycle.unsubscribed).toBe(1);
  });
});
