import { createHash } from 'node:crypto';
import http from 'node:http';
import net from 'node:net';
import { PassThrough } from 'node:stream';
import type {
  AdapterTestContext,
  AdapterSetupOptions,
  HttpRequest,
  HttpResponse,
} from '@internal/server-adapter-test-utils';
import {
  createRouteAdapterTestSuite,
  createDefaultTestContext,
  createStreamWithSensitiveData,
  createStreamWithUnserializableChunk,
  expectSerializedStreamChunks,
  consumeSSEStream,
  createMultipartTestSuite,
  createBodyLimitTestSuite,
} from '@internal/server-adapter-test-utils';
import { Mastra } from '@mastra/core';
import { registerApiRoute } from '@mastra/core/server';
import { createRoute, SERVER_ROUTES } from '@mastra/server/server-adapter';
import type { ServerRoute } from '@mastra/server/server-adapter';
import Fastify from 'fastify';
import type { FastifyInstance } from 'fastify';
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { z } from 'zod';
import { MastraServer } from '../index';

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function waitFor(assertion: () => boolean, timeout = 500): Promise<void> {
  const start = Date.now();
  while (!assertion()) {
    if (Date.now() - start > timeout) {
      throw new Error('Timed out waiting for assertion');
    }
    await sleep(1);
  }
}

// Wrapper describe block so the factory can call describe() inside
describe('Fastify Server Adapter', () => {
  it('registers only selected built-in server routes', async () => {
    const app = Fastify();
    try {
      const mastra = new Mastra({ logger: false });
      const selectedRoutes = [SERVER_ROUTES[0], SERVER_ROUTES[2]].filter(Boolean) as ServerRoute[];
      const selectedRouteKeys = new Set(selectedRoutes.map(route => `${route.method} ${route.path}`));
      const adapter = new MastraServer({
        app,
        mastra,
        routes: route => selectedRouteKeys.has(`${route.method} ${route.path}`),
      });
      const registerRoute = vi.spyOn(adapter, 'registerRoute');

      await adapter.registerRoutes();

      const expectedRoutes = SERVER_ROUTES.filter(route => selectedRouteKeys.has(`${route.method} ${route.path}`));
      expect(registerRoute).toHaveBeenCalledTimes(expectedRoutes.length);
      expect(registerRoute.mock.calls.map(call => call[1])).toEqual(expectedRoutes);
    } finally {
      await app.close();
    }
  });

  createRouteAdapterTestSuite({
    suiteName: 'Fastify Adapter Integration Tests',

    setupAdapter: async (context: AdapterTestContext, options?: AdapterSetupOptions) => {
      // Create Fastify app
      const app = Fastify();

      // Create adapter
      const adapter = new MastraServer({
        app,
        mastra: context.mastra,
        taskStore: context.taskStore,
        customRouteAuthConfig: context.customRouteAuthConfig,
        prefix: options?.prefix,
      });

      await adapter.init();

      return { app, adapter };
    },

    executeHttpRequest: async (app: FastifyInstance, httpRequest: HttpRequest): Promise<HttpResponse> => {
      // Start server on random port
      const address = await app.listen({ port: 0 });

      try {
        // Build URL with query params
        let url = `${address}${httpRequest.path}`;
        if (httpRequest.query) {
          const queryParams = new URLSearchParams();
          Object.entries(httpRequest.query).forEach(([key, value]) => {
            if (Array.isArray(value)) {
              value.forEach(v => queryParams.append(key, String(v)));
            } else {
              queryParams.append(key, String(value));
            }
          });
          const queryString = queryParams.toString();
          if (queryString) {
            url += `?${queryString}`;
          }
        }

        // Build fetch options
        const fetchOptions: RequestInit = {
          method: httpRequest.method,
          headers: {
            'Content-Type': 'application/json',
            ...(httpRequest.headers || {}),
          },
        };

        // Add body for POST/PUT/PATCH/DELETE
        if (httpRequest.body && ['POST', 'PUT', 'PATCH', 'DELETE'].includes(httpRequest.method)) {
          fetchOptions.body = JSON.stringify(httpRequest.body);
        }

        // Execute request
        const response = await fetch(url, fetchOptions);

        // Extract headers
        const headers: Record<string, string> = {};
        response.headers.forEach((value, key) => {
          headers[key] = value;
        });

        // Check if stream response
        const contentType = response.headers.get('content-type') || '';
        const transferEncoding = response.headers.get('transfer-encoding') || '';
        const isStream =
          contentType.includes('text/plain') ||
          contentType.includes('text/event-stream') ||
          contentType.includes('audio/') ||
          contentType.includes('application/octet-stream') ||
          transferEncoding === 'chunked';

        if (isStream && response.body) {
          // Return stream response
          return {
            status: response.status,
            type: 'stream',
            stream: response.body,
            headers,
          };
        } else {
          // JSON response - check content type to decide how to parse
          let data: unknown;
          const responseContentType = response.headers.get('content-type') || '';

          if (responseContentType.includes('application/json')) {
            try {
              data = await response.json();
            } catch {
              // If JSON parsing fails, return empty object
              data = {};
            }
          } else {
            // Not JSON content type, read as text
            data = await response.text();
          }

          return {
            status: response.status,
            type: 'json',
            data,
            headers,
          };
        }
      } finally {
        // Always close server
        await app.close();
      }
    },
  });

  describe('Stream Data Redaction', () => {
    let context: AdapterTestContext;
    let app: FastifyInstance | null = null;

    beforeEach(async () => {
      context = await createDefaultTestContext();
    });

    afterEach(async () => {
      if (app) {
        app.server.closeAllConnections?.();
        await app.close();
        app = null;
      }
    });

    it('should redact sensitive data from stream chunks by default', async () => {
      app = Fastify();

      const adapter = new MastraServer({
        app,
        mastra: context.mastra,
        // Default: streamOptions.redact = true
      });

      // Create a test route that returns a stream with sensitive data
      const testRoute: ServerRoute<any, any, any> = {
        method: 'POST',
        path: '/test/stream',
        responseType: 'stream',
        streamFormat: 'sse',
        handler: async () => createStreamWithSensitiveData('v2'),
      };

      app.addHook('preHandler', adapter.createContextMiddleware());
      await adapter.registerRoute(app, testRoute, { prefix: '' });

      // Start server
      const address = await app.listen({ port: 0 });

      const response = await fetch(`${address}/test/stream`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({}),
      });

      expect(response.status).toBe(200);

      const chunks = await consumeSSEStream(response.body);

      // Verify chunks exist
      expect(chunks.length).toBeGreaterThan(0);

      // Check that sensitive data is NOT present in any chunk
      const allChunksStr = JSON.stringify(chunks);
      expect(allChunksStr).not.toContain('SECRET_SYSTEM_PROMPT');
      expect(allChunksStr).not.toContain('secret_tool');

      // Verify step-start chunk has empty request
      const stepStart = chunks.find(c => c.type === 'step-start');
      expect(stepStart).toBeDefined();
      expect(stepStart.payload.request).toEqual({});

      // Verify step-finish chunk has no request in metadata
      const stepFinish = chunks.find(c => c.type === 'step-finish');
      expect(stepFinish).toBeDefined();
      expect(stepFinish.payload.metadata.request).toBeUndefined();
      expect(stepFinish.payload.output.steps[0].request).toBeUndefined();

      // Verify finish chunk has no request in metadata
      const finish = chunks.find(c => c.type === 'finish');
      expect(finish).toBeDefined();
      expect(finish.payload.metadata.request).toBeUndefined();
    });

    it('should pass SSE comment chunks through without data wrapping', async () => {
      app = Fastify();

      const adapter = new MastraServer({
        app,
        mastra: context.mastra,
      });

      const testRoute: ServerRoute<any, any, any> = {
        method: 'POST',
        path: '/test/sse-comment',
        responseType: 'stream',
        streamFormat: 'sse',
        handler: async () =>
          new ReadableStream({
            start(controller) {
              controller.enqueue(': heartbeat\n\n');
              controller.enqueue({ type: 'text-delta', payload: { text: 'hello' } });
              controller.close();
            },
          }),
      };

      app.addHook('preHandler', adapter.createContextMiddleware());
      await adapter.registerRoute(app, testRoute, { prefix: '' });

      const address = await app.listen({ port: 0 });

      const response = await fetch(`${address}/test/sse-comment`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({}),
      });

      expect(response.status).toBe(200);
      const text = await response.text();
      expect(text).toContain(': heartbeat\n\n');
      expect(text).toContain('data: {"type":"text-delta","payload":{"text":"hello"}}\n\n');
      expect(text).not.toContain('data: ": heartbeat');
    });

    it('should write SSE connected comment when sseFlushOnConnect is true', async () => {
      app = Fastify();

      const adapter = new MastraServer({
        app,
        mastra: context.mastra,
      });

      const testRoute: ServerRoute<any, any, any> = {
        method: 'POST',
        path: '/test/sse-flush',
        responseType: 'stream',
        streamFormat: 'sse',
        sseFlushOnConnect: true,
        handler: async () =>
          new ReadableStream({
            start(controller) {
              controller.enqueue({ type: 'text-delta', payload: { text: 'hello' } });
              controller.close();
            },
          }),
      };

      app.addHook('preHandler', adapter.createContextMiddleware());
      await adapter.registerRoute(app, testRoute, { prefix: '' });

      const address = await app.listen({ port: 0 });

      const response = await fetch(`${address}/test/sse-flush`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({}),
      });

      expect(response.status).toBe(200);
      const text = await response.text();
      const connectedIndex = text.indexOf(': connected\n\n');
      const dataIndex = text.indexOf('data: ');
      expect(connectedIndex).toBeGreaterThanOrEqual(0);
      expect(dataIndex).toBeGreaterThanOrEqual(0);
      expect(connectedIndex).toBeLessThan(dataIndex);
    });

    it('should not write SSE connected comment when sseFlushOnConnect is not set', async () => {
      app = Fastify();

      const adapter = new MastraServer({
        app,
        mastra: context.mastra,
      });

      const testRoute: ServerRoute<any, any, any> = {
        method: 'POST',
        path: '/test/sse-no-flush',
        responseType: 'stream',
        streamFormat: 'sse',
        handler: async () =>
          new ReadableStream({
            start(controller) {
              controller.enqueue({ type: 'text-delta', payload: { text: 'hello' } });
              controller.close();
            },
          }),
      };

      app.addHook('preHandler', adapter.createContextMiddleware());
      await adapter.registerRoute(app, testRoute, { prefix: '' });

      const address = await app.listen({ port: 0 });

      const response = await fetch(`${address}/test/sse-no-flush`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({}),
      });

      expect(response.status).toBe(200);
      const text = await response.text();
      expect(text).not.toContain(': connected');
      expect(text).toContain('data: ');
    });

    it('should NOT redact sensitive data when streamOptions.redact is false', async () => {
      app = Fastify();

      const adapter = new MastraServer({
        app,
        mastra: context.mastra,
        streamOptions: { redact: false },
      });

      // Create a test route that returns a stream with sensitive data
      const testRoute: ServerRoute<any, any, any> = {
        method: 'POST',
        path: '/test/stream',
        responseType: 'stream',
        streamFormat: 'sse',
        handler: async () => createStreamWithSensitiveData('v2'),
      };

      app.addHook('preHandler', adapter.createContextMiddleware());
      await adapter.registerRoute(app, testRoute, { prefix: '' });

      // Start server
      const address = await app.listen({ port: 0 });

      const response = await fetch(`${address}/test/stream`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({}),
      });

      expect(response.status).toBe(200);

      const chunks = await consumeSSEStream(response.body);

      // Verify chunks exist
      expect(chunks.length).toBeGreaterThan(0);

      // Check that sensitive data IS present (not redacted)
      const allChunksStr = JSON.stringify(chunks);
      expect(allChunksStr).toContain('SECRET_SYSTEM_PROMPT');
      expect(allChunksStr).toContain('secret_tool');

      // Verify step-start chunk has full request
      const stepStart = chunks.find(c => c.type === 'step-start');
      expect(stepStart).toBeDefined();
      expect(stepStart.payload.request.body).toContain('SECRET_SYSTEM_PROMPT');
    });

    it('should redact v1 format stream chunks', async () => {
      app = Fastify();

      const adapter = new MastraServer({
        app,
        mastra: context.mastra,
        // Default: streamOptions.redact = true
      });

      // Create a test route that returns a v1 format stream
      const testRoute: ServerRoute<any, any, any> = {
        method: 'POST',
        path: '/test/stream-v1',
        responseType: 'stream',
        streamFormat: 'sse',
        handler: async () => createStreamWithSensitiveData('v1'),
      };

      app.addHook('preHandler', adapter.createContextMiddleware());
      await adapter.registerRoute(app, testRoute, { prefix: '' });

      // Start server
      const address = await app.listen({ port: 0 });

      const response = await fetch(`${address}/test/stream-v1`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({}),
      });

      expect(response.status).toBe(200);

      const chunks = await consumeSSEStream(response.body);

      // Check that sensitive data is NOT present
      const allChunksStr = JSON.stringify(chunks);
      expect(allChunksStr).not.toContain('SECRET_SYSTEM_PROMPT');
      expect(allChunksStr).not.toContain('secret_tool');

      // Verify step-start chunk has empty request (v1 format)
      const stepStart = chunks.find(c => c.type === 'step-start');
      expect(stepStart).toBeDefined();
      expect(stepStart.request).toEqual({});

      // Verify step-finish chunk has no request (v1 format)
      const stepFinish = chunks.find(c => c.type === 'step-finish');
      expect(stepFinish).toBeDefined();
      expect(stepFinish.request).toBeUndefined();
    });

    it('should pass through non-sensitive chunk types unchanged', async () => {
      app = Fastify();

      const adapter = new MastraServer({
        app,
        mastra: context.mastra,
      });

      const testRoute: ServerRoute<any, any, any> = {
        method: 'POST',
        path: '/test/stream',
        responseType: 'stream',
        streamFormat: 'sse',
        handler: async () => createStreamWithSensitiveData('v2'),
      };

      app.addHook('preHandler', adapter.createContextMiddleware());
      await adapter.registerRoute(app, testRoute, { prefix: '' });

      // Start server
      const address = await app.listen({ port: 0 });

      const response = await fetch(`${address}/test/stream`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({}),
      });

      const chunks = await consumeSSEStream(response.body);

      // Verify text-delta chunk is unchanged
      const textDelta = chunks.find(c => c.type === 'text-delta');
      expect(textDelta).toBeDefined();
      expect(textDelta.textDelta).toBe('Hello');
    });
  });

  // Repro for https://github.com/mastra-ai/mastra/issues/17821 — a chunk that
  // JSON.stringify can't handle (e.g. a BigInt step output) used to throw inside
  // the stream loop and silently close the HTTP stream.
  describe('Stream Chunk Serialization', () => {
    let context: AdapterTestContext;
    let app: FastifyInstance | null = null;

    beforeEach(async () => {
      context = await createDefaultTestContext();
    });

    afterEach(async () => {
      if (app) {
        app.server.closeAllConnections?.();
        await app.close();
        app = null;
      }
    });

    it('serializes BigInt chunks and skips unserializable chunks without killing the stream', async () => {
      app = Fastify();

      const adapter = new MastraServer({
        app,
        mastra: context.mastra,
      });

      const testRoute: ServerRoute<any, any, any> = {
        method: 'POST',
        path: '/test/unserializable-stream',
        responseType: 'stream',
        streamFormat: 'sse',
        handler: async () => createStreamWithUnserializableChunk(),
      };

      app.addHook('preHandler', adapter.createContextMiddleware());
      await adapter.registerRoute(app, testRoute, { prefix: '' });

      const address = await app.listen({ port: 0 });

      const response = await fetch(`${address}/test/unserializable-stream`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({}),
      });

      expect(response.status).toBe(200);
      const chunks = await consumeSSEStream(response.body);
      expectSerializedStreamChunks(chunks);
    });
  });

  describe('Abort Signal', () => {
    let context: AdapterTestContext;
    let app: FastifyInstance | null = null;

    beforeEach(async () => {
      context = await createDefaultTestContext();
    });

    afterEach(async () => {
      if (app) {
        await app.close();
        app = null;
      }
    });

    it('should not have aborted signal when route handler executes', async () => {
      app = Fastify();

      const adapter = new MastraServer({
        app,
        mastra: context.mastra,
      });

      // Track the abort signal state when the handler executes
      let abortSignalAborted: boolean | undefined;

      // Create a test route that checks the abort signal state
      const testRoute: ServerRoute<any, any, any> = {
        method: 'POST',
        path: '/test/abort-signal',
        responseType: 'json',
        handler: async (params: any) => {
          // Capture the abort signal state when handler runs
          abortSignalAborted = params.abortSignal?.aborted;
          return { signalAborted: abortSignalAborted };
        },
      };

      app.addHook('preHandler', adapter.createContextMiddleware());
      await adapter.registerRoute(app, testRoute, { prefix: '' });

      // Start server
      const address = await app.listen({ port: 0 });

      // Make a POST request with a JSON body (this triggers body parsing which can cause the issue)
      const response = await fetch(`${address}/test/abort-signal`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ test: 'data' }),
      });

      expect(response.status).toBe(200);
      const result = await response.json();

      // The abort signal should NOT be aborted during normal request handling
      expect(result.signalAborted).toBe(false);
      expect(abortSignalAborted).toBe(false);
    });

    it('should provide abort signal to route handlers', async () => {
      app = Fastify();

      const adapter = new MastraServer({
        app,
        mastra: context.mastra,
      });

      let receivedAbortSignal: AbortSignal | undefined;

      const testRoute: ServerRoute<any, any, any> = {
        method: 'POST',
        path: '/test/abort-signal-exists',
        responseType: 'json',
        handler: async (params: any) => {
          receivedAbortSignal = params.abortSignal;
          return { hasSignal: !!params.abortSignal };
        },
      };

      app.addHook('preHandler', adapter.createContextMiddleware());
      await adapter.registerRoute(app, testRoute, { prefix: '' });

      const address = await app.listen({ port: 0 });

      const response = await fetch(`${address}/test/abort-signal-exists`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({}),
      });

      expect(response.status).toBe(200);
      const result = await response.json();

      // Route handler should receive an abort signal
      expect(result.hasSignal).toBe(true);
      expect(receivedAbortSignal).toBeDefined();
      expect(receivedAbortSignal).toBeInstanceOf(AbortSignal);
    });
  });

  // Multipart FormData tests
  createMultipartTestSuite({
    suiteName: 'Fastify Multipart FormData',

    setupAdapter: async (context, options) => {
      const app = Fastify();

      const adapter = new MastraServer({
        app,
        mastra: context.mastra,
        taskStore: context.taskStore,
        bodyLimitOptions: options?.bodyLimitOptions,
      });

      await adapter.init();

      return { app, adapter };
    },

    startServer: async (app: FastifyInstance) => {
      const address = await app.listen({ port: 0 });

      return {
        baseUrl: address,
        cleanup: async () => {
          await app.close();
        },
      };
    },

    registerRoute: async (adapter, app, route, options) => {
      await adapter.registerRoute(app, route, options || { prefix: '' });
    },

    getContextMiddleware: adapter => adapter.createContextMiddleware(),

    applyMiddleware: (app, middleware) => {
      app.addHook('preHandler', middleware);
    },
  });

  describe('Plugin Headers on Stream Responses', () => {
    let context: AdapterTestContext;
    let app: FastifyInstance | null = null;

    beforeEach(async () => {
      context = await createDefaultTestContext();
    });

    afterEach(async () => {
      if (app) {
        await app.close();
        app = null;
      }
    });

    it('should preserve headers set by plugins/hooks on stream responses', async () => {
      app = Fastify();

      // Simulate what a CORS plugin does: set headers in an onRequest hook
      // This tests that headers set before the route handler are preserved
      // when using reply.hijack() for streaming
      app.addHook('onRequest', async (_request, reply) => {
        reply.header('access-control-allow-origin', 'https://example.com');
        reply.header('access-control-allow-credentials', 'true');
        reply.header('x-custom-header', 'custom-value');
      });

      const adapter = new MastraServer({
        app,
        mastra: context.mastra,
      });

      // Create a test route that returns a stream
      const testRoute: ServerRoute<any, any, any> = {
        method: 'POST',
        path: '/test/stream',
        responseType: 'stream',
        streamFormat: 'sse',
        handler: async () => createStreamWithSensitiveData('v2'),
      };

      app.addHook('preHandler', adapter.createContextMiddleware());
      await adapter.registerRoute(app, testRoute, { prefix: '' });

      // Start server
      const address = await app.listen({ port: 0 });

      const response = await fetch(`${address}/test/stream`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({}),
      });

      expect(response.status).toBe(200);

      // Headers set by the hook should be preserved on stream responses
      expect(response.headers.get('access-control-allow-origin')).toBe('https://example.com');
      expect(response.headers.get('access-control-allow-credentials')).toBe('true');
      expect(response.headers.get('x-custom-header')).toBe('custom-value');

      // Consume the stream to avoid hanging
      await consumeSSEStream(response.body);
    });

    it('should preserve headers set by plugins/hooks on non-stream (JSON) responses', async () => {
      app = Fastify();

      // Simulate what a CORS plugin does: set headers in an onRequest hook
      app.addHook('onRequest', async (_request, reply) => {
        reply.header('access-control-allow-origin', 'https://example.com');
        reply.header('access-control-allow-credentials', 'true');
        reply.header('x-custom-header', 'custom-value');
      });

      const adapter = new MastraServer({
        app,
        mastra: context.mastra,
      });

      // Create a test route that returns JSON (not a stream)
      const testRoute: ServerRoute<any, any, any> = {
        method: 'POST',
        path: '/test/json',
        responseType: 'json',
        handler: async () => ({ message: 'hello' }),
      };

      app.addHook('preHandler', adapter.createContextMiddleware());
      await adapter.registerRoute(app, testRoute, { prefix: '' });

      // Start server
      const address = await app.listen({ port: 0 });

      const response = await fetch(`${address}/test/json`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({}),
      });

      expect(response.status).toBe(200);

      // Headers should be present on JSON responses (this already works without the fix)
      expect(response.headers.get('access-control-allow-origin')).toBe('https://example.com');
      expect(response.headers.get('access-control-allow-credentials')).toBe('true');
      expect(response.headers.get('x-custom-header')).toBe('custom-value');

      await response.json();
    });
  });

  describe('Multipart File Handling (Busboy)', () => {
    let context: AdapterTestContext;
    let app: FastifyInstance | null = null;

    beforeEach(async () => {
      context = await createDefaultTestContext();
    });

    afterEach(async () => {
      if (app) {
        await app.close();
        app = null;
      }
    });

    it('should expose uploaded file as buffer', async () => {
      app = Fastify();

      const adapter = new MastraServer({
        app,
        mastra: context.mastra,
      });

      const testRoute: ServerRoute<any, any, any> = {
        method: 'POST',
        path: '/test/upload',
        responseType: 'json',
        handler: async (params: any) => {
          return params;
        },
      };

      adapter.registerContextMiddleware();
      await adapter.registerRoute(app, testRoute, { prefix: '' });

      const address = await app.listen({ port: 0 });

      const form = new FormData();
      form.append('file', new Blob(['hello world']), 'test.txt');

      const response = await fetch(`${address}/test/upload`, {
        method: 'POST',
        body: form as any,
      });

      const data = await response.json();

      expect(response.status).toBe(200);
      expect(data.file).toBeDefined();

      // reconstruct buffer from JSON
      const reconstructed = Buffer.from(data.file.data);

      expect(reconstructed.toString()).toBe('hello world');
    });

    it('should return error when file exceeds size limit (no hang)', async () => {
      app = Fastify();

      const adapter = new MastraServer({
        app,
        mastra: context.mastra,
        bodyLimitOptions: { maxSize: 1024 },
      });

      const testRoute: ServerRoute<any, any, any> = {
        method: 'POST',
        path: '/test/upload-limit',
        responseType: 'json',
        handler: async (params: any) => params,
      };

      adapter.registerContextMiddleware();
      await adapter.registerRoute(app, testRoute, { prefix: '' });

      const address = await app.listen({ port: 0 });

      const bigBuffer = new Uint8Array(1024 * 10);

      const form = new FormData();
      form.append('file', new Blob([bigBuffer]), 'big.txt');

      const response = await fetch(`${address}/test/upload-limit`, {
        method: 'POST',
        body: form as any,
      });

      expect(response.status).toBeGreaterThanOrEqual(400);
    });

    // --- Multipart aggregate body-limit hardening (PF-2594) ---

    const buildMultipartString = (
      boundary: string,
      parts: Array<{ name: string; value: string; filename?: string }>,
    ): string => {
      const rendered = parts
        .map(part => {
          const disposition =
            part.filename === undefined
              ? `content-disposition: form-data; name="${part.name}"\r\n`
              : `content-disposition: form-data; name="${part.name}"; filename="${part.filename}"\r\ncontent-type: application/octet-stream\r\n`;
          return `--${boundary}\r\n${disposition}\r\n${part.value}\r\n`;
        })
        .join('');
      return `${rendered}--${boundary}--\r\n`;
    };

    /**
     * Send a raw multipart POST over a plain node:http socket so oversized
     * payloads exercise the server's early-413 path deterministically (fetch's
     * upload handling can obscure an early response mid-write).
     */
    const sendRawMultipart = (
      port: number,
      path: string,
      boundary: string,
      payload: Buffer,
    ): Promise<{ status: number; body: string }> => {
      return new Promise((resolve, reject) => {
        let responded = false;
        const req = http.request(
          {
            host: '127.0.0.1',
            port,
            method: 'POST',
            path,
            headers: {
              'content-type': `multipart/form-data; boundary=${boundary}`,
              'content-length': payload.byteLength,
            },
          },
          res => {
            responded = true;
            let body = '';
            res.on('data', chunk => (body += chunk));
            res.on('end', () => {
              req.destroy();
              resolve({ status: res.statusCode ?? 0, body });
            });
            res.on('error', reject);
          },
        );
        // After an early 413 the server closes a connection whose request body
        // was never fully read; post-response socket errors are expected.
        req.on('error', error => {
          if (!responded) reject(error);
        });
        req.end(payload);
      });
    };

    const serverPort = (address: string): number => Number(new URL(address).port);

    it('rejects an aggregate-oversize multipart body with the hardened lane 413 shape and stays healthy', async () => {
      app = Fastify();

      const adapter = new MastraServer({
        app,
        mastra: context.mastra,
        bodyLimitOptions: { maxSize: 1024 },
      });

      const testRoute: ServerRoute<any, any, any> = {
        method: 'POST',
        path: '/test/upload-aggregate',
        responseType: 'json',
        handler: async (params: any) => ({ ok: true, name: params.name }),
      };

      adapter.registerContextMiddleware();
      await adapter.registerRoute(app, testRoute, { prefix: '' });

      const address = await app.listen({ port: 0 });

      // Each part is small; only the aggregate breaches the 1024-byte cap.
      const boundary = 'aggregate-boundary';
      const oversize = buildMultipartString(
        boundary,
        Array.from({ length: 64 }, (_, index) => ({ name: `field${index}`, value: 'v'.repeat(64) })),
      );
      const rejected = await sendRawMultipart(
        serverPort(address),
        '/test/upload-aggregate',
        boundary,
        Buffer.from(oversize),
      );

      expect(rejected.status).toBe(413);
      // Exact hardened-lane shape: Fastify's own FST_ERR_CTP_BODY_TOO_LARGE
      // serialization, identical to the non-multipart pre-buffer 413.
      expect(JSON.parse(rejected.body)).toMatchObject({
        statusCode: 413,
        code: 'FST_ERR_CTP_BODY_TOO_LARGE',
        message: 'Request body is too large',
      });

      // Busboy teardown must be clean: the same server keeps serving multipart.
      const followUp = new FormData();
      followUp.append('name', 'still-alive');
      const accepted = await fetch(`${address}/test/upload-aggregate`, {
        method: 'POST',
        body: followUp as any,
      });
      expect(accepted.status).toBe(200);
      await expect(accepted.json()).resolves.toMatchObject({ ok: true, name: 'still-alive' });
    });

    it('enforces a route-declared maxBodySize on multipart even without adapter bodyLimitOptions', async () => {
      app = Fastify();

      const adapter = new MastraServer({
        app,
        mastra: context.mastra,
      });

      const testRoute: ServerRoute<any, any, any> = {
        method: 'POST',
        path: '/test/upload-route-cap',
        responseType: 'json',
        maxBodySize: 256,
        handler: async (params: any) => ({ ok: true, name: params.name }),
      };

      adapter.registerContextMiddleware();
      await adapter.registerRoute(app, testRoute, { prefix: '' });

      const address = await app.listen({ port: 0 });
      const boundary = 'route-cap-boundary';

      // Over the 256-byte route cap (but far under the 1 MiB server default).
      const oversize = buildMultipartString(boundary, [{ name: 'file', value: 'x'.repeat(512), filename: 'x.bin' }]);
      const rejected = await sendRawMultipart(
        serverPort(address),
        '/test/upload-route-cap',
        boundary,
        Buffer.from(oversize),
      );
      expect(rejected.status).toBe(413);
      expect(JSON.parse(rejected.body).code).toBe('FST_ERR_CTP_BODY_TOO_LARGE');

      // Under the route cap parses normally.
      const undersize = buildMultipartString(boundary, [{ name: 'name', value: 'ok' }]);
      const accepted = await sendRawMultipart(
        serverPort(address),
        '/test/upload-route-cap',
        boundary,
        Buffer.from(undersize),
      );
      expect(accepted.status).toBe(200);
      expect(JSON.parse(accepted.body)).toMatchObject({ ok: true, name: 'ok' });
    });

    it('caps multipart at the Fastify server bodyLimit when neither route nor adapter declares one', async () => {
      app = Fastify(); // factory default bodyLimit: 1 MiB

      const adapter = new MastraServer({
        app,
        mastra: context.mastra,
      });

      const testRoute: ServerRoute<any, any, any> = {
        method: 'POST',
        path: '/test/upload-default-cap',
        responseType: 'json',
        handler: async () => ({ ok: true }),
      };

      adapter.registerContextMiddleware();
      await adapter.registerRoute(app, testRoute, { prefix: '' });

      const address = await app.listen({ port: 0 });
      const boundary = 'default-cap-boundary';

      const oversize = buildMultipartString(boundary, [
        { name: 'file', value: 'x'.repeat(1024 * 1024 + 1024), filename: 'big.bin' },
      ]);
      const rejected = await sendRawMultipart(
        serverPort(address),
        '/test/upload-default-cap',
        boundary,
        Buffer.from(oversize),
      );
      expect(rejected.status).toBe(413);
      expect(JSON.parse(rejected.body).code).toBe('FST_ERR_CTP_BODY_TOO_LARGE');
    });

    it('aborts a chunked multipart stream mid-flight with 413 and closes the connection (no hang)', async () => {
      app = Fastify();

      const adapter = new MastraServer({
        app,
        mastra: context.mastra,
        bodyLimitOptions: { maxSize: 1024 },
      });

      const testRoute: ServerRoute<any, any, any> = {
        method: 'POST',
        path: '/test/upload-chunked',
        responseType: 'json',
        handler: async (params: any) => ({ ok: true, name: params.name }),
      };

      adapter.registerContextMiddleware();
      await adapter.registerRoute(app, testRoute, { prefix: '' });

      const address = await app.listen({ port: 0 });
      const boundary = 'chunked-abort-boundary';

      // No Content-Length: the pre-buffer short-circuit cannot engage, so the
      // 413 must come from the mid-stream aggregate accounting.
      const result = await new Promise<{ status: number; body: string }>((resolve, reject) => {
        const req = http.request({
          host: '127.0.0.1',
          port: serverPort(address),
          method: 'POST',
          path: '/test/upload-chunked',
          headers: { 'content-type': `multipart/form-data; boundary=${boundary}` },
        });

        let timer: NodeJS.Timeout | undefined;
        const stopWriting = () => {
          if (timer) {
            clearInterval(timer);
            timer = undefined;
          }
        };

        // Connection reset after the early 413 is the expected teardown.
        let responded = false;
        req.on('error', error => {
          stopWriting();
          if (!responded) reject(error);
        });

        req.on('response', res => {
          responded = true;
          stopWriting();
          let body = '';
          res.on('data', chunk => (body += chunk));
          res.on('end', () => {
            req.destroy();
            resolve({ status: res.statusCode ?? 0, body });
          });
          res.on('error', reject);
        });

        req.write(
          `--${boundary}\r\ncontent-disposition: form-data; name="file"; filename="endless.bin"\r\ncontent-type: application/octet-stream\r\n\r\n`,
        );
        const filler = Buffer.alloc(256, 0x61);
        timer = setInterval(() => {
          req.write(filler);
        }, 5);
      });

      expect(result.status).toBe(413);
      expect(JSON.parse(result.body).code).toBe('FST_ERR_CTP_BODY_TOO_LARGE');

      // The abort must not wedge the server: a well-formed multipart request on
      // a fresh connection still succeeds.
      const followUp = new FormData();
      followUp.append('name', 'after-abort');
      const accepted = await fetch(`${address}/test/upload-chunked`, {
        method: 'POST',
        body: followUp as any,
      });
      expect(accepted.status).toBe(200);
      await expect(accepted.json()).resolves.toMatchObject({ ok: true, name: 'after-abort' });
    });

    // --- skipBodyParse multipart aggregate enforcement (PF-2594 review finding) ---
    //
    // skipBodyParse routes return from getParams() before the multipart
    // aggregate lane, so without the dedicated guard a multipart request to a
    // skipBodyParse route with a declared maxBodySize reaches its handler with
    // no adapter-level aggregate cap at all.

    it('enforces a declared maxBodySize on a skipBodyParse multipart route with the hardened 413 shape', async () => {
      app = Fastify();

      const adapter = new MastraServer({
        app,
        mastra: context.mastra,
      });

      let handlerRuns = 0;
      const testRoute: ServerRoute<any, any, any> = {
        method: 'POST',
        path: '/test/webhook-skip-capped',
        responseType: 'json',
        skipBodyParse: true,
        maxBodySize: 1024,
        handler: async () => {
          handlerRuns += 1;
          return { ok: true };
        },
      };

      adapter.registerContextMiddleware();
      await adapter.registerRoute(app, testRoute, { prefix: '' });

      const address = await app.listen({ port: 0 });
      const boundary = 'skip-cap-boundary';

      // Over the cap with an honest Content-Length: rejected before the
      // handler runs, with the exact hardened-lane 413 shape.
      const oversize = buildMultipartString(boundary, [{ name: 'file', value: 'x'.repeat(4096), filename: 'x.bin' }]);
      const rejected = await sendRawMultipart(
        serverPort(address),
        '/test/webhook-skip-capped',
        boundary,
        Buffer.from(oversize),
      );
      expect(rejected.status).toBe(413);
      expect(JSON.parse(rejected.body)).toMatchObject({
        statusCode: 413,
        code: 'FST_ERR_CTP_BODY_TOO_LARGE',
        message: 'Request body is too large',
      });
      expect(handlerRuns).toBe(0);

      // Under the cap the same route still reaches its handler.
      const undersize = buildMultipartString(boundary, [{ name: 'name', value: 'ok' }]);
      const accepted2 = await sendRawMultipart(
        serverPort(address),
        '/test/webhook-skip-capped',
        boundary,
        Buffer.from(undersize),
      );
      expect(accepted2.status).toBe(200);
      expect(JSON.parse(accepted2.body)).toMatchObject({ ok: true });
      expect(handlerRuns).toBe(1);
    });

    it('aborts an over-cap chunked multipart stream on a skipBodyParse route mid-consumption with 413 (no hang)', async () => {
      app = Fastify();

      const adapter = new MastraServer({
        app,
        mastra: context.mastra,
      });

      // skipBodyParse handlers do not receive the raw stream through handler
      // params; capture it per request the way an embedding server could.
      let currentRaw: FastifyRequest['raw'] | undefined;
      app.addHook('onRequest', async request => {
        currentRaw = request.raw;
      });

      let handlerSettled = false;
      const testRoute: ServerRoute<any, any, any> = {
        method: 'POST',
        path: '/test/webhook-skip-chunked',
        responseType: 'json',
        skipBodyParse: true,
        maxBodySize: 1024,
        handler: async () => {
          const raw = currentRaw!;
          let consumedBytes = 0;
          try {
            for await (const chunk of raw) {
              consumedBytes += (chunk as Buffer).length;
            }
          } catch {
            // Expected on abort: the guard's 413 + connection teardown
            // destroys the half-read request stream.
          } finally {
            handlerSettled = true;
          }
          return { ok: true, consumedBytes };
        },
      };

      adapter.registerContextMiddleware();
      await adapter.registerRoute(app, testRoute, { prefix: '' });

      const address = await app.listen({ port: 0 });
      const boundary = 'skip-chunked-boundary';

      // No Content-Length: the pre-buffer short-circuit cannot engage; the
      // 413 must come from the passive mid-stream accounting while the
      // handler is consuming. Writes are BOUNDED (8 KiB against a 1 KiB cap)
      // so a missing guard fails fast with a 200 instead of hanging.
      const result = await new Promise<{ status: number; body: string }>((resolve, reject) => {
        const req = http.request({
          host: '127.0.0.1',
          port: serverPort(address),
          method: 'POST',
          path: '/test/webhook-skip-chunked',
          headers: { 'content-type': `multipart/form-data; boundary=${boundary}` },
        });

        let timer: NodeJS.Timeout | undefined;
        const stopWriting = () => {
          if (timer) {
            clearInterval(timer);
            timer = undefined;
          }
        };

        // Connection reset after the mid-stream 413 is the expected teardown.
        let responded = false;
        req.on('error', error => {
          stopWriting();
          if (!responded) reject(error);
        });

        req.on('response', res => {
          responded = true;
          stopWriting();
          let body = '';
          res.on('data', chunk => (body += chunk));
          res.on('end', () => {
            req.destroy();
            resolve({ status: res.statusCode ?? 0, body });
          });
          res.on('error', reject);
        });

        req.write(
          `--${boundary}\r\ncontent-disposition: form-data; name="file"; filename="big.bin"\r\ncontent-type: application/octet-stream\r\n\r\n`,
        );
        const filler = Buffer.alloc(512, 0x61);
        let written = 0;
        timer = setInterval(() => {
          req.write(filler);
          written += 1;
          if (written >= 16) {
            stopWriting();
            req.end(`\r\n--${boundary}--\r\n`);
          }
        }, 2);
      });

      expect(result.status).toBe(413);
      expect(JSON.parse(result.body)).toMatchObject({
        statusCode: 413,
        code: 'FST_ERR_CTP_BODY_TOO_LARGE',
        message: 'Request body is too large',
      });

      // Clean teardown: the in-flight handler consumption terminates instead
      // of hanging on a wedged stream…
      await waitFor(() => handlerSettled, 2000);

      // …and a fresh connection still gets served end to end.
      const afterBoundary = 'skip-chunked-after';
      const small = buildMultipartString(afterBoundary, [{ name: 'name', value: 'after-abort' }]);
      const followUp = await sendRawMultipart(
        serverPort(address),
        '/test/webhook-skip-chunked',
        afterBoundary,
        Buffer.from(small),
      );
      expect(followUp.status).toBe(200);
      expect(JSON.parse(followUp.body)).toMatchObject({ ok: true });
    });

    it('hands a consuming skipBodyParse handler the intact raw multipart stream byte-exact under the cap', async () => {
      app = Fastify();

      const adapter = new MastraServer({
        app,
        mastra: context.mastra,
      });

      let currentRaw: FastifyRequest['raw'] | undefined;
      app.addHook('onRequest', async request => {
        currentRaw = request.raw;
      });

      const testRoute: ServerRoute<any, any, any> = {
        method: 'POST',
        path: '/test/webhook-skip-exact',
        responseType: 'json',
        skipBodyParse: true,
        maxBodySize: 64 * 1024,
        handler: async () => {
          const chunks: Buffer[] = [];
          for await (const chunk of currentRaw!) {
            chunks.push(chunk as Buffer);
          }
          const received = Buffer.concat(chunks);
          return { bytes: received.byteLength, sha256: createHash('sha256').update(received).digest('hex') };
        },
      };

      adapter.registerContextMiddleware();
      await adapter.registerRoute(app, testRoute, { prefix: '' });

      const address = await app.listen({ port: 0 });
      const boundary = 'skip-exact-boundary';

      // 16 KiB payload under the 64 KiB cap: the passive accounting must not
      // consume, reorder, or drop a single byte of what the handler reads.
      const payload = Buffer.from(
        buildMultipartString(boundary, [{ name: 'file', value: 'y'.repeat(16 * 1024), filename: 'y.bin' }]),
      );
      const response = await sendRawMultipart(serverPort(address), '/test/webhook-skip-exact', boundary, payload);
      expect(response.status).toBe(200);
      expect(JSON.parse(response.body)).toEqual({
        bytes: payload.byteLength,
        sha256: createHash('sha256').update(payload).digest('hex'),
      });
    });

    it('tears down an unread chunked multipart connection after the response instead of discarding forever', async () => {
      app = Fastify();

      const adapter = new MastraServer({
        app,
        mastra: context.mastra,
      });

      // The real harness-inbound shape: skipBodyParse + declared
      // maxBodySize, and a handler that CANNOT consume request.raw
      // (ServerRoute handlers only receive transport-neutral params).
      const testRoute: ServerRoute<any, any, any> = {
        method: 'POST',
        path: '/test/webhook-skip-unconsumed',
        responseType: 'json',
        skipBodyParse: true,
        maxBodySize: 1024,
        handler: async () => ({ ok: true }),
      };

      adapter.registerContextMiddleware();
      await adapter.registerRoute(app, testRoute, { prefix: '' });
      const address = await app.listen({ port: 0 });
      const boundary = 'skip-unconsumed-boundary';

      // Endless chunked body (no Content-Length): without the deliberate
      // teardown, Fastify (requestTimeout: 0) leaves the connection open
      // and dump-discards the remainder forever.
      const result = await new Promise<{ gotResponse: boolean; serverClosed: boolean; writesAfterResponse: number }>(
        resolve => {
          const sock = net.connect(serverPort(address), '127.0.0.1');
          let response = '';
          let gotResponse = false;
          let writesAfterResponse = 0;
          let settled = false;
          let timer: NodeJS.Timeout | undefined;
          let guard: NodeJS.Timeout | undefined;
          const settle = (serverClosed: boolean) => {
            if (settled) return;
            settled = true;
            if (timer) clearInterval(timer);
            if (guard) clearTimeout(guard);
            resolve({ gotResponse, serverClosed, writesAfterResponse });
            sock.destroy();
          };
          sock.on('data', chunk => {
            response += chunk;
            if (!gotResponse && response.includes('\r\n\r\n')) gotResponse = true;
          });
          // FIN or reset from the server after the response IS the
          // deliberate teardown under test.
          sock.on('end', () => settle(true));
          sock.on('error', () => settle(gotResponse));
          sock.on('close', () => settle(gotResponse));
          sock.on('connect', () => {
            sock.write(
              'POST /test/webhook-skip-unconsumed HTTP/1.1\r\n' +
                'host: 127.0.0.1\r\n' +
                `content-type: multipart/form-data; boundary=${boundary}\r\n` +
                'transfer-encoding: chunked\r\nconnection: keep-alive\r\n\r\n',
            );
            const piece = 'a'.repeat(1024);
            const frame = `${piece.length.toString(16)}\r\n${piece}\r\n`;
            timer = setInterval(() => {
              if (sock.destroyed) return;
              sock.write(frame);
              if (gotResponse) writesAfterResponse += 1;
            }, 5);
            guard = setTimeout(() => settle(false), 3000);
          });
        },
      );

      expect(result.gotResponse).toBe(true);
      // The server's teardown, not the 3s guard, must end the connection.
      expect(result.serverClosed).toBe(true);
      // Byte churn stops promptly after the response.
      expect(result.writesAfterResponse).toBeLessThan(100);

      // The teardown wedges nothing: a fresh request is served end to end.
      const followUp = buildMultipartString(boundary, [{ name: 'name', value: 'ok' }]);
      const accepted = await sendRawMultipart(
        serverPort(address),
        '/test/webhook-skip-unconsumed',
        boundary,
        Buffer.from(followUp),
      );
      expect(accepted.status).toBe(200);
    }, 10_000);

    it('leaves a handler that consumes after an await untouched until natural end', async () => {
      app = Fastify();

      const adapter = new MastraServer({
        app,
        mastra: context.mastra,
      });

      let currentRaw: FastifyRequest['raw'] | undefined;
      app.addHook('onRequest', async request => {
        currentRaw = request.raw;
      });

      const testRoute: ServerRoute<any, any, any> = {
        method: 'POST',
        path: '/test/webhook-skip-late-consume',
        responseType: 'json',
        skipBodyParse: true,
        maxBodySize: 64 * 1024,
        handler: async () => {
          // Consume only AFTER an await: the teardown keys on reply 'finish'
          // (which cannot precede the handler's return), never on handler
          // entry, so a late consumer still gets the intact stream.
          await sleep(25);
          const chunks: Buffer[] = [];
          for await (const chunk of currentRaw!) {
            chunks.push(chunk as Buffer);
          }
          const received = Buffer.concat(chunks);
          // `readableEnded` distinguishes a natural end from a teardown abort
          // (`destroyed` is useless here: for-await destroys its source on
          // completion as part of iterator cleanup).
          return {
            bytes: received.byteLength,
            sha256: createHash('sha256').update(received).digest('hex'),
            endedNaturally: currentRaw!.readableEnded,
          };
        },
      };

      adapter.registerContextMiddleware();
      await adapter.registerRoute(app, testRoute, { prefix: '' });
      const address = await app.listen({ port: 0 });

      const boundary = 'skip-late-consume-boundary';
      const payload = Buffer.from(
        buildMultipartString(boundary, [{ name: 'file', value: 'z'.repeat(16 * 1024), filename: 'z.bin' }]),
      );
      const response = await sendRawMultipart(
        serverPort(address),
        '/test/webhook-skip-late-consume',
        boundary,
        payload,
      );
      expect(response.status).toBe(200);
      expect(JSON.parse(response.body)).toEqual({
        bytes: payload.byteLength,
        sha256: createHash('sha256').update(payload).digest('hex'),
        endedNaturally: true,
      });
    });

    // --- Effective-cap precedence + generalized teardown (PF-2594 codex round 2) ---

    /**
     * Stream an endless chunked multipart body from a raw socket and KEEP
     * writing after the response arrives; report whether the server tore the
     * connection down within `boundMs` and how much churn followed.
     */
    const streamChunkedMultipartUntilClosed = (
      port: number,
      path: string,
      boundary: string,
      extraHeaders: string,
      boundMs: number,
    ): Promise<{ statusLine: string; serverClosed: boolean; writesAfterResponse: number }> =>
      new Promise(resolve => {
        const sock = net.connect(port, '127.0.0.1');
        let response = '';
        let gotResponse = false;
        let writesAfterResponse = 0;
        let settled = false;
        let timer: NodeJS.Timeout | undefined;
        let guard: NodeJS.Timeout | undefined;
        const settle = (serverClosed: boolean) => {
          if (settled) return;
          settled = true;
          if (timer) clearInterval(timer);
          if (guard) clearTimeout(guard);
          resolve({ statusLine: response.split('\r\n')[0] ?? '', serverClosed, writesAfterResponse });
          sock.destroy();
        };
        sock.on('data', chunk => {
          response += chunk;
          if (!gotResponse && response.includes('\r\n\r\n')) gotResponse = true;
        });
        sock.on('end', () => settle(true));
        sock.on('error', () => settle(gotResponse));
        sock.on('close', () => settle(gotResponse));
        sock.on('connect', () => {
          sock.write(
            `POST ${path} HTTP/1.1\r\n` +
              'host: 127.0.0.1\r\n' +
              `content-type: multipart/form-data; boundary=${boundary}\r\n` +
              extraHeaders +
              'transfer-encoding: chunked\r\nconnection: keep-alive\r\n\r\n',
          );
          const piece = 'a'.repeat(1024);
          const frame = `${piece.length.toString(16)}\r\n${piece}\r\n`;
          timer = setInterval(() => {
            if (sock.destroyed) return;
            sock.write(frame);
            if (gotResponse) writesAfterResponse += 1;
          }, 5);
          guard = setTimeout(() => settle(false), boundMs);
        });
      });

    it('enforces the adapter-level cap on skipBodyParse multipart routes without a route maxBodySize', async () => {
      app = Fastify();

      const adapter = new MastraServer({
        app,
        mastra: context.mastra,
        bodyLimitOptions: { maxSize: 1024 },
      });

      const testRoute: ServerRoute<any, any, any> = {
        method: 'POST',
        path: '/test/webhook-skip-adapter-cap',
        responseType: 'json',
        skipBodyParse: true,
        handler: async () => ({ ok: true }),
      };

      adapter.registerContextMiddleware();
      await adapter.registerRoute(app, testRoute, { prefix: '' });
      const address = await app.listen({ port: 0 });
      const boundary = 'skip-adapter-cap-boundary';

      const oversize = buildMultipartString(boundary, [{ name: 'file', value: 'x'.repeat(4096), filename: 'x.bin' }]);
      const rejected = await sendRawMultipart(
        serverPort(address),
        '/test/webhook-skip-adapter-cap',
        boundary,
        Buffer.from(oversize),
      );
      expect(rejected.status).toBe(413);
      expect(JSON.parse(rejected.body).code).toBe('FST_ERR_CTP_BODY_TOO_LARGE');

      const undersize = buildMultipartString(boundary, [{ name: 'name', value: 'ok' }]);
      const accepted = await sendRawMultipart(
        serverPort(address),
        '/test/webhook-skip-adapter-cap',
        boundary,
        Buffer.from(undersize),
      );
      expect(accepted.status).toBe(200);
    });

    it('caps skipBodyParse multipart at the server bodyLimit when neither route nor adapter declares one', async () => {
      app = Fastify({ bodyLimit: 2048 });

      const adapter = new MastraServer({
        app,
        mastra: context.mastra,
      });

      const testRoute: ServerRoute<any, any, any> = {
        method: 'POST',
        path: '/test/webhook-skip-server-cap',
        responseType: 'json',
        skipBodyParse: true,
        handler: async () => ({ ok: true }),
      };

      adapter.registerContextMiddleware();
      await adapter.registerRoute(app, testRoute, { prefix: '' });
      const address = await app.listen({ port: 0 });
      const boundary = 'skip-server-cap-boundary';

      const oversize = buildMultipartString(boundary, [{ name: 'file', value: 'x'.repeat(4096), filename: 'x.bin' }]);
      const rejected = await sendRawMultipart(
        serverPort(address),
        '/test/webhook-skip-server-cap',
        boundary,
        Buffer.from(oversize),
      );
      expect(rejected.status).toBe(413);
      expect(JSON.parse(rejected.body).code).toBe('FST_ERR_CTP_BODY_TOO_LARGE');

      const undersize = buildMultipartString(boundary, [{ name: 'name', value: 'ok' }]);
      const accepted = await sendRawMultipart(
        serverPort(address),
        '/test/webhook-skip-server-cap',
        boundary,
        Buffer.from(undersize),
      );
      expect(accepted.status).toBe(200);
    });

    it('lets a route-level maxBodySize win over the adapter-level cap on skipBodyParse multipart', async () => {
      app = Fastify();

      const adapter = new MastraServer({
        app,
        mastra: context.mastra,
        bodyLimitOptions: { maxSize: 256 },
      });

      const testRoute: ServerRoute<any, any, any> = {
        method: 'POST',
        path: '/test/webhook-skip-route-wins',
        responseType: 'json',
        skipBodyParse: true,
        maxBodySize: 8192,
        handler: async () => ({ ok: true }),
      };

      adapter.registerContextMiddleware();
      await adapter.registerRoute(app, testRoute, { prefix: '' });
      const address = await app.listen({ port: 0 });
      const boundary = 'skip-route-wins-boundary';

      // Over the adapter cap but under the route cap: the route wins → 200.
      const midSized = buildMultipartString(boundary, [{ name: 'file', value: 'x'.repeat(1024), filename: 'x.bin' }]);
      const accepted = await sendRawMultipart(
        serverPort(address),
        '/test/webhook-skip-route-wins',
        boundary,
        Buffer.from(midSized),
      );
      expect(accepted.status).toBe(200);

      // Over the route cap as well → 413. The pre-auth check is header-only
      // (honest Content-Length), so stream just the first bytes and read the
      // 413 — uploading the full payload would race the connection close that
      // follows an early 413 (kernel-buffered unread bytes turn the close
      // into a reset).
      const rejected = await new Promise<{ status: number; code: string }>((resolve, reject) => {
        // A dedicated socket: Node's keep-alive agent would otherwise race
        // this request onto the pooled socket of the preceding 200 while the
        // client-side destroy is tearing it down.
        const req = http.request({
          host: '127.0.0.1',
          port: serverPort(address),
          method: 'POST',
          path: '/test/webhook-skip-route-wins',
          agent: false,
          headers: {
            'content-type': `multipart/form-data; boundary=${boundary}`,
            'content-length': 12288,
          },
        });
        let responded = false;
        req.on('error', error => {
          if (!responded) reject(error);
        });
        req.on('response', res => {
          responded = true;
          let body = '';
          res.on('data', chunk => (body += chunk));
          res.on('end', () => {
            req.destroy();
            resolve({ status: res.statusCode ?? 0, code: JSON.parse(body).code });
          });
        });
        req.write(Buffer.alloc(1024, 0x61));
      });
      expect(rejected.status).toBe(413);
      expect(rejected.code).toBe('FST_ERR_CTP_BODY_TOO_LARGE');
    });

    it('closes the connection after a parsed-lane multipart 413 while the client keeps writing', async () => {
      app = Fastify();

      const adapter = new MastraServer({
        app,
        mastra: context.mastra,
        bodyLimitOptions: { maxSize: 1024 },
      });

      const testRoute: ServerRoute<any, any, any> = {
        method: 'POST',
        path: '/test/parsed-keep-writing',
        responseType: 'json',
        handler: async (params: any) => ({ ok: true, name: params.name }),
      };

      adapter.registerContextMiddleware();
      await adapter.registerRoute(app, testRoute, { prefix: '' });
      const address = await app.listen({ port: 0 });

      // Chunked (no Content-Length) over-cap body from a client that KEEPS
      // writing after the 413 arrives: without the teardown the server
      // dump-discards the remainder on an open connection indefinitely.
      const result = await streamChunkedMultipartUntilClosed(
        serverPort(address),
        '/test/parsed-keep-writing',
        'parsed-keep-writing-boundary',
        '',
        3000,
      );

      expect(result.statusLine).toContain(' 413 ');
      expect(result.serverClosed).toBe(true);
      expect(result.writesAfterResponse).toBeLessThan(100);

      // Clean teardown: a fresh connection still parses multipart fine.
      const followUp = new FormData();
      followUp.append('name', 'still-alive');
      const accepted = await fetch(`${address}/test/parsed-keep-writing`, {
        method: 'POST',
        body: followUp as any,
      });
      expect(accepted.status).toBe(200);
      await expect(accepted.json()).resolves.toMatchObject({ ok: true, name: 'still-alive' });
    }, 10_000);

    it('tears down an unauthenticated oversized chunked multipart request after the 401', async () => {
      app = Fastify();

      const originalGetServer = context.mastra.getServer.bind(context.mastra);
      context.mastra.getServer = () =>
        ({
          ...originalGetServer(),
          auth: {
            authenticateToken: async (token: string) => (token === 'valid-token' ? { id: 'user-1' } : null),
            authorize: async () => true,
          },
        }) as ReturnType<typeof originalGetServer>;

      const adapter = new MastraServer({
        app,
        mastra: context.mastra,
        bodyLimitOptions: { maxSize: 1024 },
      });

      const testRoute: ServerRoute<any, any, any> = {
        method: 'POST',
        path: '/api/test/multipart-protected',
        responseType: 'json',
        handler: async () => ({ ok: true }),
      };

      adapter.registerContextMiddleware();
      await adapter.registerRoute(app, testRoute, { prefix: '' });
      const address = await app.listen({ port: 0 });

      // No authorization header: auth 401s BEFORE getParams ever runs, so
      // neither body-limit lane engages — only the shared multipart unread
      // teardown can close the connection while the client keeps writing.
      const result = await streamChunkedMultipartUntilClosed(
        serverPort(address),
        '/api/test/multipart-protected',
        'multipart-auth-boundary',
        '',
        3000,
      );

      expect(result.statusLine).toContain(' 401 ');
      expect(result.serverClosed).toBe(true);
      expect(result.writesAfterResponse).toBeLessThan(100);

      // The server keeps serving authorized multipart afterwards.
      const followUp = new FormData();
      followUp.append('name', 'authed');
      const accepted = await fetch(`${address}/api/test/multipart-protected`, {
        method: 'POST',
        headers: { authorization: 'Bearer valid-token' },
        body: followUp as any,
      });
      expect(accepted.status).toBe(200);
    }, 10_000);

    it('rejects an honest over-cap Content-Length multipart before auth with 413', async () => {
      app = Fastify();

      const originalGetServer = context.mastra.getServer.bind(context.mastra);
      let authenticateCalls = 0;
      context.mastra.getServer = () =>
        ({
          ...originalGetServer(),
          auth: {
            authenticateToken: async (token: string) => {
              authenticateCalls += 1;
              return token === 'valid-token' ? { id: 'user-1' } : null;
            },
            authorize: async () => true,
          },
        }) as ReturnType<typeof originalGetServer>;

      const adapter = new MastraServer({
        app,
        mastra: context.mastra,
        bodyLimitOptions: { maxSize: 1024 },
      });

      const testRoute: ServerRoute<any, any, any> = {
        method: 'POST',
        path: '/api/test/multipart-preauth-cap',
        responseType: 'json',
        handler: async () => ({ ok: true }),
      };

      adapter.registerContextMiddleware();
      await adapter.registerRoute(app, testRoute, { prefix: '' });
      const address = await app.listen({ port: 0 });
      const boundary = 'preauth-cap-boundary';

      // Unauthenticated AND over-cap with an honest Content-Length: the
      // header-only parser check must reject 413 BEFORE auth runs (mirroring
      // Fastify's buffering parsers), not 401.
      const oversize = buildMultipartString(boundary, [{ name: 'file', value: 'x'.repeat(4096), filename: 'x.bin' }]);
      const rejected = await sendRawMultipart(
        serverPort(address),
        '/api/test/multipart-preauth-cap',
        boundary,
        Buffer.from(oversize),
      );
      expect(rejected.status).toBe(413);
      expect(JSON.parse(rejected.body).code).toBe('FST_ERR_CTP_BODY_TOO_LARGE');
      expect(authenticateCalls).toBe(0);
    });

    // --- Premature request closure during multipart parsing (PF-2594 CodeRabbit round) ---
    //
    // A request stream can close WITHOUT an 'error' event (client abort), or
    // arrive already destroyed (abort during an earlier await). Busboy then
    // never emits 'finish' and the parser promise would pend forever.

    it('settles multipart parsing when the request stream closes prematurely without an error', async () => {
      app = Fastify();
      const adapter = new MastraServer({ app, mastra: context.mastra });

      const boundary = 'premature-close-boundary';
      const route: ServerRoute<any, any, any> = {
        method: 'POST',
        path: '/test/premature-close',
        responseType: 'json',
        handler: async () => ({ ok: true }),
      };
      const makeFakeRequest = (raw: PassThrough) =>
        ({
          params: {},
          query: {},
          body: undefined,
          headers: { 'content-type': `multipart/form-data; boundary=${boundary}` },
          server: { initialConfig: { bodyLimit: 1024 * 1024 } },
          raw,
        }) as any;
      const bounded = <T>(parse: Promise<T>) =>
        Promise.race([
          parse,
          sleep(2000).then(() => {
            throw new Error('parser promise still pending after premature close');
          }),
        ]);

      // Close WITHOUT 'error' mid-parse: destroy() with no error argument
      // emits only 'close'; busboy never finishes on its own.
      const midParseRaw = new PassThrough();
      const midParse = adapter.getParams(route, makeFakeRequest(midParseRaw));
      midParseRaw.write(`--${boundary}\r\ncontent-disposition: form-data; name="a"\r\n\r\npartial`);
      await sleep(10);
      midParseRaw.destroy();
      const midParseParams = await bounded(midParse);
      expect(midParseParams.bodyParseError?.message).toMatch(/closed before the body completed/);

      // Stream already destroyed BEFORE the parse starts: no future events at
      // all, so the parse must fail immediately instead of waiting for them.
      const deadRaw = new PassThrough();
      deadRaw.destroy();
      await sleep(1);
      const deadParams = await bounded(adapter.getParams(route, makeFakeRequest(deadRaw)));
      expect(deadParams.bodyParseError?.message).toMatch(/closed before the body completed/);
    });

    it('recovers when a client aborts a multipart upload mid-stream (socket teardown)', async () => {
      app = Fastify();
      const adapter = new MastraServer({ app, mastra: context.mastra });

      const testRoute: ServerRoute<any, any, any> = {
        method: 'POST',
        path: '/test/abort-upload',
        responseType: 'json',
        handler: async (params: any) => ({ ok: true, name: params.name }),
      };

      adapter.registerContextMiddleware();
      await adapter.registerRoute(app, testRoute, { prefix: '' });

      // Capture the in-flight getParams promise so settlement is observable.
      const getParamsResults: Array<Promise<unknown>> = [];
      const realGetParams = adapter.getParams.bind(adapter);
      vi.spyOn(adapter, 'getParams').mockImplementation((route, request) => {
        const parse = realGetParams(route, request);
        getParamsResults.push(parse);
        return parse;
      });

      const address = await app.listen({ port: 0 });
      const boundary = 'abort-mid-upload';

      await new Promise<void>(resolve => {
        const req = http.request({
          host: '127.0.0.1',
          port: serverPort(address),
          method: 'POST',
          path: '/test/abort-upload',
          headers: {
            'content-type': `multipart/form-data; boundary=${boundary}`,
            // Declared length far beyond what is sent: the parse is
            // mid-stream when the socket dies.
            'content-length': 512 * 1024,
          },
        });
        req.on('error', () => resolve());
        req.on('close', () => resolve());
        req.write(
          `--${boundary}\r\ncontent-disposition: form-data; name="file"; filename="a.bin"\r\ncontent-type: application/octet-stream\r\n\r\n`,
        );
        req.write(Buffer.alloc(4096, 0x61));
        setTimeout(() => req.destroy(), 20);
      });

      // The in-flight parse must settle in bounded time (via 'error' or the
      // premature-close backstop) — never a pending-forever leak.
      await waitFor(() => getParamsResults.length === 1, 1000);
      const settlement = await Promise.race([
        getParamsResults[0]!.then(
          () => 'settled',
          () => 'settled',
        ),
        sleep(2000).then(() => 'pending'),
      ]);
      expect(settlement).toBe('settled');

      // The abort must not wedge the server: a fresh multipart request works.
      const followUp = new FormData();
      followUp.append('name', 'after-client-abort');
      const accepted = await fetch(`${address}/test/abort-upload`, {
        method: 'POST',
        body: followUp as any,
      });
      expect(accepted.status).toBe(200);
      await expect(accepted.json()).resolves.toMatchObject({ ok: true, name: 'after-client-abort' });
    });

    it('does not spuriously reject multipart parsing on normal completion (close after end)', async () => {
      app = Fastify();
      const adapter = new MastraServer({ app, mastra: context.mastra });

      const testRoute: ServerRoute<any, any, any> = {
        method: 'POST',
        path: '/test/normal-complete',
        responseType: 'json',
        handler: async (params: any) => ({ ok: true, name: params.name }),
      };

      adapter.registerContextMiddleware();
      await adapter.registerRoute(app, testRoute, { prefix: '' });

      const getParamsResults: Array<Promise<{ body?: unknown; bodyParseError?: { message: string } }>> = [];
      const realGetParams = adapter.getParams.bind(adapter);
      vi.spyOn(adapter, 'getParams').mockImplementation((route, request) => {
        const parse = realGetParams(route, request);
        getParamsResults.push(parse);
        return parse;
      });

      const address = await app.listen({ port: 0 });

      const form = new FormData();
      form.append('name', 'all-good');
      form.append('file', new Blob(['hello world']), 'h.txt');
      const response = await fetch(`${address}/test/normal-complete`, { method: 'POST', body: form as any });
      expect(response.status).toBe(200);
      await expect(response.json()).resolves.toMatchObject({ ok: true, name: 'all-good' });

      // The raw socket's 'close' after a completed body must not have turned
      // into a premature-close rejection.
      const params = await getParamsResults[0]!;
      expect(params.bodyParseError).toBeUndefined();
      expect(params.body).toMatchObject({ name: 'all-good' });
    });
  });

  describe('Custom route prefix validation', () => {
    it('should throw when a custom route path starts with the server prefix', async () => {
      const customRoutes = [
        registerApiRoute('/mastra/custom', {
          method: 'GET',
          handler: async c => c.json({ message: 'should not work' }),
        }),
      ];

      const mastra = new Mastra({});
      const app = Fastify();

      const adapter = new MastraServer({
        app,
        mastra,
        customApiRoutes: customRoutes,
        prefix: '/mastra',
      });

      await expect(adapter.init()).rejects.toThrow(/must not start with "\/mastra"/);
      await app.close();
    });

    it('should allow custom routes at paths not starting with the server prefix', async () => {
      const customRoutes = [
        registerApiRoute('/custom/hello', {
          method: 'GET',
          handler: async c => c.json({ message: 'Hello from custom route!' }),
        }),
      ];

      const mastra = new Mastra({});
      const app = Fastify();

      const adapter = new MastraServer({
        app,
        mastra,
        customApiRoutes: customRoutes,
        prefix: '/mastra',
      });

      await adapter.init();
      const address = await app.listen({ port: 0 });

      const response = await fetch(`${address}/custom/hello`);

      expect(response.status).toBe(200);
      const data = await response.json();
      expect(data).toEqual({ message: 'Hello from custom route!' });
      await app.close();
    });

    it('registers createRoute routes from customApiRoutes with runtime validation', async () => {
      const route = createRoute({
        method: 'POST',
        path: '/custom/validated',
        responseType: 'json',
        requiresAuth: false,
        bodySchema: z.object({ name: z.string() }),
        handler: async ({ name }) => ({ greeting: `Hello, ${name}` }),
      });
      const protectedRoute = createRoute({
        method: 'GET',
        path: '/custom/secure',
        responseType: 'json',
        requiresAuth: true,
        handler: async () => ({ secret: true }),
      });

      const mastra = new Mastra({ server: { apiRoutes: [route, protectedRoute] } });
      const originalGetServer = mastra.getServer.bind(mastra);
      mastra.getServer = () =>
        ({
          ...originalGetServer(),
          auth: {
            authenticateToken: async (token: string) => (token === 'valid-token' ? { id: 'user-1' } : null),
            authorize: async () => true,
          },
        }) as any;
      const app = Fastify();

      const adapter = new MastraServer({ app, mastra });
      await adapter.init();
      const address = await app.listen({ port: 0 });

      try {
        const invalidResponse = await fetch(`${address}/custom/validated`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ name: 42 }),
        });
        expect(invalidResponse.status).toBe(400);
        await expect(invalidResponse.json()).resolves.toMatchObject({ error: 'Invalid request body' });

        const validResponse = await fetch(`${address}/custom/validated`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ name: 'Ada' }),
        });
        expect(validResponse.status).toBe(200);
        await expect(validResponse.json()).resolves.toEqual({ greeting: 'Hello, Ada' });

        const unauthenticated = await fetch(`${address}/custom/secure`);
        expect(unauthenticated.status).toBe(401);

        const authenticated = await fetch(`${address}/custom/secure`, {
          headers: { Authorization: 'Bearer valid-token' },
        });
        expect(authenticated.status).toBe(200);
        await expect(authenticated.json()).resolves.toEqual({ secret: true });
      } finally {
        await app.close();
      }
    });
  });

  describe('Custom route stream disconnect handling', () => {
    let app: FastifyInstance | null = null;

    afterEach(async () => {
      if (app) {
        app.server.closeAllConnections?.();
        await app.close();
        app = null;
      }
    });

    it('cancels a custom route stream when the client cancels the response body', async () => {
      const cancel = vi.fn();
      const signalAbort = vi.fn();
      const customRoutes = [
        registerApiRoute('/custom/stream', {
          method: 'GET',
          handler: async c => {
            c.req.raw.signal.addEventListener('abort', signalAbort);
            return new Response(
              new ReadableStream({
                async pull(controller) {
                  controller.enqueue(new TextEncoder().encode('chunk\n'));
                  await sleep(5);
                },
                cancel,
              }),
            );
          },
        }),
      ];

      app = Fastify();
      const adapter = new MastraServer({
        app,
        mastra: new Mastra({}),
        customApiRoutes: customRoutes,
      });

      await adapter.init();
      const address = await app.listen({ port: 0 });

      const response = await fetch(`${address}/custom/stream`);
      const reader = response.body!.getReader();
      await reader.read();
      await reader.cancel();

      await waitFor(() => cancel.mock.calls.length > 0);
      await waitFor(() => signalAbort.mock.calls.length > 0);
    });

    it('does not cancel a custom POST stream when the completed request body closes normally', async () => {
      const cancel = vi.fn();
      const signalAbort = vi.fn();
      const customRoutes = [
        registerApiRoute('/custom/post-stream', {
          method: 'POST',
          handler: async c => {
            c.req.raw.signal.addEventListener('abort', signalAbort);
            return new Response(
              new ReadableStream({
                start(controller) {
                  controller.enqueue(new TextEncoder().encode('one\n'));
                  setTimeout(() => {
                    controller.enqueue(new TextEncoder().encode('two\n'));
                    controller.enqueue(new TextEncoder().encode('three\n'));
                    controller.close();
                  }, 10);
                },
                cancel,
              }),
            );
          },
        }),
      ];

      app = Fastify();
      const adapter = new MastraServer({
        app,
        mastra: new Mastra({}),
        customApiRoutes: customRoutes,
      });

      await adapter.init();
      const address = await app.listen({ port: 0 });

      const response = await fetch(`${address}/custom/post-stream`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ hello: 'world' }),
      });

      await expect(response.text()).resolves.toBe('one\ntwo\nthree\n');
      await sleep(10);
      expect(cancel).not.toHaveBeenCalled();
      expect(signalAbort).not.toHaveBeenCalled();
    });
  });

  describe('Channel webhook diagnostics', () => {
    it('warns for an unregistered channel webhook when no custom API routes exist', async () => {
      const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});
      const app = Fastify();
      const adapter = new MastraServer({ app, mastra: new Mastra({}) });

      try {
        await adapter.init();

        const response = await app.inject({
          method: 'POST',
          url: '/api/agents/support/channels/slack/webhook',
        });

        expect(response.statusCode).toBe(404);
        expect(warnSpy).toHaveBeenCalledWith(
          expect.stringContaining('channels.adapters configuration'),
          expect.objectContaining({ agentId: 'support', platform: 'slack' }),
        );
      } finally {
        warnSpy.mockRestore();
        await app.close();
      }
    });
  });

  createBodyLimitTestSuite({
    suiteName: 'Body Size Limit',

    createApp: () => Fastify({ logger: false }),

    setupAdapter: (app, mastra, bodyLimitOptions) => {
      const adapter = new MastraServer({ app, mastra, bodyLimitOptions });
      adapter.registerContextMiddleware();
      return { adapter, app };
    },

    registerRoute: (adapter, app, route) => adapter.registerRoute(app, route, { prefix: '' }),

    // The adapter enforces bodyLimitOptions.maxSize as an aggregate cap across
    // an entire multipart payload (parseMultipartFormData raw-stream accounting).
    supportsMultipartAggregateLimit: true,

    executeRequest: async (app, method, url, options = {}) => {
      const parsedUrl = new URL(url);
      const injectOptions: any = {
        method,
        url: parsedUrl.pathname + parsedUrl.search,
        headers: options.headers || {},
      };

      if (options.body) {
        injectOptions.payload = options.body;
        // Default to JSON only when the case did not choose its own content
        // type (the multipart cases carry an explicit multipart Content-Type).
        const hasContentType = Object.keys(injectOptions.headers).some(name => name.toLowerCase() === 'content-type');
        if (!hasContentType) {
          injectOptions.headers['content-type'] = 'application/json';
        }
      }

      const response = await app.inject(injectOptions);
      return { status: response.statusCode };
    },

    cleanupApp: app => app.close(),
  });
});
