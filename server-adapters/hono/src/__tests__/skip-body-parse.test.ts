/**
 * Integration tests for the per-route `skipBodyParse` opt-out and per-route
 * `bodyLimit` decoupling on the Hono adapter.
 *
 * Context (harness-v1 §14.2 channel webhook ingress): a provider signs the EXACT
 * request bytes. The adapter must (1) capture the raw body without JSON/multipart
 * parsing and without the malformed-JSON 400 short-circuit, so a signed payload
 * that is not strict JSON still reaches the handler/adapter for HMAC verification;
 * (2) install a per-route pre-buffer byte cap (413) from `route.maxBodySize` even
 * when the adapter was constructed WITHOUT global bodyLimitOptions; and (3) leave
 * every other route's parse + body-limit behavior unchanged.
 *
 * These exercise the real adapter request pipeline (global context middleware ->
 * per-route middleware -> getParams -> handler), unlike the @mastra/server
 * handler unit tests which call `route.handler` directly.
 */
import { Mastra } from '@mastra/core';
import type { ServerRoute } from '@mastra/server/server-adapter';
import { Hono } from 'hono';
import { describe, it, expect, beforeEach } from 'vitest';
import { MastraServer } from '../index';

interface CapturedRequest {
  rawBody?: Uint8Array | string;
  requestBody: unknown;
  contentType?: string;
}

/**
 * Build a JSON ServerRoute whose handler records what the adapter forwarded.
 * `skipBodyParse` and `maxBodySize` are passed through unchanged.
 */
function makeRecorderRoute(
  path: string,
  captured: { value?: CapturedRequest },
  opts: { skipBodyParse?: boolean; maxBodySize?: number } = {},
): ServerRoute {
  return {
    method: 'POST',
    path,
    responseType: 'json',
    requiresAuth: false,
    ...(opts.skipBodyParse !== undefined ? { skipBodyParse: opts.skipBodyParse } : {}),
    ...(opts.maxBodySize !== undefined ? { maxBodySize: opts.maxBodySize } : {}),
    handler: (params: any) => {
      captured.value = {
        rawBody: params.rawBody,
        requestBody: params.requestBody,
        contentType: params.getHeader?.('content-type'),
      };
      return { ok: true };
    },
  } as unknown as ServerRoute;
}

/**
 * Register routes through the real adapter pipeline (context middleware first,
 * then each route) WITHOUT going through full init() so we don't pull in every
 * built-in SERVER_ROUTE. `bodyLimitOptions` is intentionally NOT passed, which
 * proves `route.maxBodySize` is honored standalone. `prefix: ''` so the routes
 * register at their literal paths.
 */
async function setupAdapter(routes: ServerRoute[]): Promise<Hono> {
  const app = new Hono();
  const mastra = new Mastra({});
  const adapter = new MastraServer({
    app: app as any,
    mastra,
    prefix: '',
    // NOTE: no bodyLimitOptions — proves route.maxBodySize is honored standalone.
  });

  adapter.registerContextMiddleware();
  for (const route of routes) {
    await adapter.registerRoute(app as any, route, { prefix: '' });
  }
  return app;
}

describe('Hono adapter — per-route skipBodyParse + bodyLimit decoupling (§14.2)', () => {
  beforeEach(() => {
    // Each test builds its own adapter; nothing shared to reset.
  });

  it('forwards a malformed-as-JSON signed body UNCHANGED (exact bytes) with NO 400 on a skipBodyParse route', async () => {
    const captured: { value?: CapturedRequest } = {};
    const app = await setupAdapter([
      makeRecorderRoute('/webhook', captured, { skipBodyParse: true, maxBodySize: 1024 * 1024 }),
    ]);

    // A provider-signed payload that is NOT strict JSON, yet declares JSON content-type.
    const signedBody = '{"event":"message","ts":not-json,*raw*';
    const response = await app.request(
      new Request('http://localhost/webhook', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: signedBody,
      }),
    );

    // No malformed-JSON 400 short-circuit.
    expect(response.status).toBe(200);
    // The handler received the EXACT bytes for HMAC verification, unparsed.
    expect(captured.value).toBeDefined();
    const rawBody = captured.value!.rawBody;
    expect(rawBody).toBeInstanceOf(Uint8Array);
    expect(new TextDecoder().decode(rawBody as Uint8Array)).toBe(signedBody);
    // Parsing was deferred — no parsed body was produced.
    expect(captured.value!.requestBody).toBeUndefined();
  });

  it('forwards a valid JSON signed body UNCHANGED (exact bytes) on a skipBodyParse route', async () => {
    const captured: { value?: CapturedRequest } = {};
    const app = await setupAdapter([
      makeRecorderRoute('/webhook', captured, { skipBodyParse: true, maxBodySize: 1024 * 1024 }),
    ]);

    const signedBody = JSON.stringify({ event: 'message', token: 'abc' });
    const response = await app.request(
      new Request('http://localhost/webhook', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: signedBody,
      }),
    );

    expect(response.status).toBe(200);
    expect(new TextDecoder().decode(captured.value!.rawBody as Uint8Array)).toBe(signedBody);
    // Even valid JSON is left UNPARSED on a skipBodyParse route — verification owns parsing.
    expect(captured.value!.requestBody).toBeUndefined();
  });

  it('a NON-skipBodyParse route still returns 400 on malformed JSON (unchanged)', async () => {
    const captured: { value?: CapturedRequest } = {};
    const app = await setupAdapter([makeRecorderRoute('/normal', captured)]);

    const response = await app.request(
      new Request('http://localhost/normal', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: '{"broken": ', // malformed
      }),
    );

    expect(response.status).toBe(400);
    // Handler never ran — the malformed-JSON gate short-circuited.
    expect(captured.value).toBeUndefined();
  });

  it('a NON-skipBodyParse route still parses valid JSON into requestBody (unchanged)', async () => {
    const captured: { value?: CapturedRequest } = {};
    const app = await setupAdapter([makeRecorderRoute('/normal', captured)]);

    const response = await app.request(
      new Request('http://localhost/normal', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ hello: 'world' }),
      }),
    );

    expect(response.status).toBe(200);
    expect(captured.value!.requestBody).toEqual({ hello: 'world' });
  });

  it('rejects an oversized body with 413 BEFORE buffering, from route.maxBodySize, with NO global bodyLimitOptions', async () => {
    const captured: { value?: CapturedRequest } = {};
    const maxBodySize = 1024; // 1 KiB cap for the test route
    const app = await setupAdapter([
      makeRecorderRoute('/webhook', captured, { skipBodyParse: true, maxBodySize }),
    ]);

    const oversized = 'x'.repeat(maxBodySize + 1);
    const response = await app.request(
      new Request('http://localhost/webhook', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Content-Length': String(oversized.length),
        },
        body: oversized,
      }),
    );

    // hono bodyLimit's default onError throws HTTPException(413) before the handler.
    expect(response.status).toBe(413);
    expect(captured.value).toBeUndefined();
  });

  it('allows an under-cap body through the per-route bodyLimit (no false 413)', async () => {
    const captured: { value?: CapturedRequest } = {};
    const maxBodySize = 1024;
    const app = await setupAdapter([
      makeRecorderRoute('/webhook', captured, { skipBodyParse: true, maxBodySize }),
    ]);

    const body = JSON.stringify({ small: true });
    const response = await app.request(
      new Request('http://localhost/webhook', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Content-Length': String(body.length) },
        body,
      }),
    );

    expect(response.status).toBe(200);
    expect(new TextDecoder().decode(captured.value!.rawBody as Uint8Array)).toBe(body);
  });

  it('rejects an oversized CHUNKED (no Content-Length) body at the per-route cap WITHOUT pre-buffering it (pre-buffer guarantee)', async () => {
    // This is the load-bearing characterization for BLOCKER #1. A 413 status alone
    // does NOT prove the pre-buffer property: the global context middleware runs
    // `app.use('*')` BEFORE the per-route bodyLimit, and if its skip-body-parse guard
    // misfires it calls `c.req.raw.clone().json()` and fully buffers the body into
    // memory before bodyLimit can reject. With Content-Length present, hono rejects
    // up front regardless, masking the buffering. So we send a CHUNKED stream (no
    // Content-Length) and count how many chunks are pulled from the request body:
    // a working skip lets bodyLimit stop after a few chunks at the cap; a misfiring
    // skip drains ALL chunks via the context-middleware parse first.
    const captured: { value?: CapturedRequest } = {};
    const maxBodySize = 1024; // 1 KiB cap
    const app = await setupAdapter([
      makeRecorderRoute('/webhook', captured, { skipBodyParse: true, maxBodySize }),
    ]);

    const numChunks = 100;
    const chunkSize = 512; // 51200 bytes total, far over the 1 KiB cap
    let chunksPulled = 0;
    const stream = new ReadableStream({
      pull(controller) {
        if (chunksPulled >= numChunks) {
          controller.close();
          return;
        }
        chunksPulled++;
        controller.enqueue(new Uint8Array(chunkSize).fill(65));
      },
    });

    const response = await app.request(
      new Request('http://localhost/webhook', {
        method: 'POST',
        // application/json so the (now-skipped) context middleware would try to parse;
        // NO Content-Length so the only thing that can stop the read is bodyLimit
        // streaming the body chunk-by-chunk.
        headers: { 'Content-Type': 'application/json' },
        body: stream,
        // @ts-expect-error duplex is required by the runtime for a streamed body
        duplex: 'half',
      }),
    );

    expect(response.status).toBe(413);
    // Handler never ran.
    expect(captured.value).toBeUndefined();
    // The body was NOT fully drained: bodyLimit stopped streaming well before the
    // last chunk. With the routePath bug, the context middleware's clone().json()
    // would have pulled all 100/100 chunks before the 413. We allow generous slack
    // for hono's internal buffering but require it to be a small fraction of the body.
    expect(chunksPulled).toBeLessThan(numChunks);
    expect(chunksPulled).toBeLessThanOrEqual(10);
  });

  it('the global context middleware skip FIRES for the matched route (does not consult routePath)', async () => {
    // Directly characterizes the skip decision rather than inferring it from a 413
    // or from surviving raw bytes (which .clone() preserves regardless). We spy on
    // the JSON read the context middleware performs (`c.req.raw.clone().json()`):
    // for a skipBodyParse route it must NOT be called; for a normal route it IS.
    // The spy is installed on Request.prototype.json so it observes the clone the
    // context middleware uses for its requestContext pre-parse.
    const jsonReads: number[] = [];
    const RequestProto = Request.prototype as { json: () => Promise<unknown> };
    const originalJson = RequestProto.json;
    let order = 0;
    RequestProto.json = function patched(this: Request) {
      jsonReads.push(++order);
      return originalJson.call(this);
    };

    try {
      const skipCaptured: { value?: CapturedRequest } = {};
      const normalCaptured: { value?: CapturedRequest } = {};
      const app = await setupAdapter([
        makeRecorderRoute('/webhook', skipCaptured, { skipBodyParse: true, maxBodySize: 1024 * 1024 }),
        makeRecorderRoute('/normal', normalCaptured),
      ]);

      // A valid-JSON body carrying a requestContext so the normal route's context
      // middleware definitely takes the parse branch.
      const bodyWithCtx = JSON.stringify({ requestContext: { tenant: 'acme' } });

      jsonReads.length = 0;
      order = 0;
      const skipRes = await app.request(
        new Request('http://localhost/webhook', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: bodyWithCtx,
        }),
      );
      expect(skipRes.status).toBe(200);
      // The context middleware did NOT JSON-parse the skipBodyParse route's body.
      // (getParams captures rawBody via arrayBuffer(), not json(), so json() must
      // never fire for this request.)
      expect(jsonReads.length).toBe(0);

      jsonReads.length = 0;
      order = 0;
      const normalRes = await app.request(
        new Request('http://localhost/normal', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: bodyWithCtx,
        }),
      );
      expect(normalRes.status).toBe(200);
      // The context middleware DID JSON-parse the normal route's body (control).
      expect(jsonReads.length).toBeGreaterThan(0);
    } finally {
      RequestProto.json = originalJson;
    }
  });

  it('the global context middleware does NOT JSON-parse the body of a skipBodyParse route', async () => {
    // A non-JSON-parseable body with application/json content-type. If the global
    // middleware tried to parse it (and short-circuited), or if any stage consumed
    // the stream, the handler would not receive the exact bytes. We assert the raw
    // bytes survive intact, which proves the global pre-parse was skipped for the
    // matched route AND the stream was not consumed.
    const captured: { value?: CapturedRequest } = {};
    const app = await setupAdapter([
      makeRecorderRoute('/webhook', captured, { skipBodyParse: true, maxBodySize: 1024 * 1024 }),
    ]);

    const signedBody = 'not-json-at-all \x00\x01 raw';
    const response = await app.request(
      new Request('http://localhost/webhook', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: signedBody,
      }),
    );

    expect(response.status).toBe(200);
    expect(new TextDecoder().decode(captured.value!.rawBody as Uint8Array)).toBe(signedBody);
  });
});
