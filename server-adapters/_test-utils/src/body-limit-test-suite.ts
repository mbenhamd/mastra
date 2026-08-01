import { Mastra } from '@mastra/core/mastra';
import type { BodyLimitOptions, ServerRoute } from '@mastra/server/server-adapter';
import { describe, it, expect } from 'vitest';

/**
 * Configuration for the body-limit test suite
 */
export interface BodyLimitTestSuiteConfig<TApp> {
  /** Name for the test suite */
  suiteName?: string;

  /** Body size limit (in bytes) to configure the adapter with. Defaults to 100. */
  maxSize?: number;

  /** Create a new app instance */
  createApp: () => TApp;

  /**
   * Construct the adapter for the given app/Mastra instance with the provided
   * bodyLimitOptions, wiring up any context middleware the adapter needs.
   */
  setupAdapter: (
    app: TApp,
    mastra: Mastra,
    bodyLimitOptions: BodyLimitOptions,
  ) => { adapter: any; app: TApp } | Promise<{ adapter: any; app: TApp }>;

  /** Register the given ServerRoute on the app through the adapter's registerRoute() */
  registerRoute: (adapter: any, app: TApp, route: ServerRoute<any, any, any>) => void | Promise<void>;

  /** Execute an HTTP request against the app and return its status code */
  executeRequest: (
    app: TApp,
    method: string,
    url: string,
    options?: { headers?: Record<string, string>; body?: string },
  ) => Promise<{ status: number }>;

  /** Optional teardown for the app instance (e.g. closing a listening server) */
  cleanupApp?: (app: TApp) => void | Promise<void>;

  /**
   * Whether the adapter enforces bodyLimitOptions.maxSize as an AGGREGATE cap
   * over an entire multipart/form-data payload (all part bodies, boundaries,
   * and part headers combined), rejecting a breach with the same 413 as the
   * non-multipart lane. Adapters that have not implemented multipart aggregate
   * enforcement leave this unset and the multipart cases below are registered
   * as explicit skips (visible in the reporter) instead of failing — the same
   * config-driven gating the suites in this package use for other optional
   * capabilities. NOTE: an adapter opting in must forward the suite-provided
   * Content-Type header untouched in its executeRequest wiring.
   */
  supportsMultipartAggregateLimit?: boolean;
}

/** Build a deterministic multipart/form-data payload as a raw string body. */
function buildMultipartPayload(
  boundary: string,
  parts: Array<{ name: string; value: string; filename?: string }>,
): string {
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
}

/**
 * Creates a standardized body-limit test suite for server adapters.
 *
 * Exercises the adapter's registerRoute() body-limit gate (bodyLimitOptions.maxSize)
 * for both POST and DELETE. DELETE is included alongside the long-established POST
 * behavior as regression coverage: DELETE requests previously bypassed the body-limit
 * check entirely, even though the adapters' getParams() reads and JSON-parses the
 * request body for DELETE the same way it does for POST/PUT/PATCH.
 */
export function createBodyLimitTestSuite<TApp>(config: BodyLimitTestSuiteConfig<TApp>) {
  const {
    suiteName = 'Body Size Limit',
    maxSize = 100,
    createApp,
    setupAdapter,
    registerRoute,
    executeRequest,
    cleanupApp,
    supportsMultipartAggregateLimit,
  } = config;

  describe(suiteName, () => {
    const oversizedPayload = JSON.stringify({ padding: 'x'.repeat(maxSize * 4) });

    it.each(['POST', 'DELETE'] as const)('rejects an oversized %s body with 413', async method => {
      const mastra = new Mastra({});
      const app = createApp();
      const { adapter, app: wiredApp } = await setupAdapter(app, mastra, {
        maxSize,
        onError: () => ({ error: 'Request body too large' }),
      });

      const testRoute: ServerRoute<any, any, any> = {
        method,
        path: '/test/body-limit',
        responseType: 'json',
        handler: async ({ body }) => ({ receivedBody: body }),
      };

      await registerRoute(adapter, wiredApp, testRoute);

      const response = await executeRequest(wiredApp, method, 'http://localhost/test/body-limit', {
        headers: { 'Content-Type': 'application/json' },
        body: oversizedPayload,
      });

      expect(response.status).toBe(413);

      await cleanupApp?.(wiredApp);
    });

    describe('multipart aggregate', () => {
      // Register the cases as explicit skips for adapters that have not opted
      // in, so unsupported adapters surface the gap in their reporter instead
      // of failing or silently omitting the coverage.
      const multipartIt = supportsMultipartAggregateLimit ? it : it.skip;

      // A one-character boundary keeps per-part wire overhead minimal (~51
      // bytes for a field part) so the cases stay meaningful under small caps.
      const boundary = 'b';
      const multipartHeaders = { 'Content-Type': `multipart/form-data; boundary=${boundary}` };

      const runMultipartCase = async (body: string): Promise<number> => {
        const mastra = new Mastra({});
        const app = createApp();
        const { adapter, app: wiredApp } = await setupAdapter(app, mastra, {
          maxSize,
          onError: () => ({ error: 'Request body too large' }),
        });

        const testRoute: ServerRoute<any, any, any> = {
          method: 'POST',
          path: '/test/multipart-body-limit',
          responseType: 'json',
          handler: async ({ requestBody }) => ({ receivedBody: requestBody }),
        };

        await registerRoute(adapter, wiredApp, testRoute);

        const response = await executeRequest(wiredApp, 'POST', 'http://localhost/test/multipart-body-limit', {
          headers: multipartHeaders,
          body,
        });

        await cleanupApp?.(wiredApp);
        return response.status;
      };

      multipartIt('accepts a multipart body whose aggregate stays under the limit', async () => {
        const payload = buildMultipartPayload(boundary, [{ name: 'a', value: 'x' }]);
        expect(payload.length).toBeLessThanOrEqual(maxSize);

        expect(await runMultipartCase(payload)).toBe(200);
      });

      multipartIt('rejects a multipart body whose aggregate exceeds the limit with 413', async () => {
        // Each part stays under every per-part cap; only the aggregate breaches.
        const partValue = 'y'.repeat(Math.floor(maxSize / 2));
        const payload = buildMultipartPayload(boundary, [
          { name: 'a', value: partValue },
          { name: 'b', value: partValue },
        ]);
        expect(payload.length).toBeGreaterThan(maxSize);

        expect(await runMultipartCase(payload)).toBe(413);
      });

      multipartIt('rejects a single file part larger than the limit with 413', async () => {
        const payload = buildMultipartPayload(boundary, [
          { name: 'file', value: 'z'.repeat(maxSize * 4), filename: 'oversize.bin' },
        ]);

        expect(await runMultipartCase(payload)).toBe(413);
      });

      multipartIt('rejects a flood of small parts whose aggregate exceeds the limit with 413', async () => {
        const payload = buildMultipartPayload(
          boundary,
          Array.from({ length: maxSize }, (_, index) => ({ name: `f${index}`, value: 'x' })),
        );
        expect(payload.length).toBeGreaterThan(maxSize);

        expect(await runMultipartCase(payload)).toBe(413);
      });
    });
  });
}
