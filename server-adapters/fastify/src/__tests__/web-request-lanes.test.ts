import type { AdapterTestContext } from '@internal/server-adapter-test-utils';
import { createDefaultTestContext } from '@internal/server-adapter-test-utils';
import type { ServerRoute } from '@mastra/server/server-adapter';
import Fastify from 'fastify';
import type { FastifyInstance } from 'fastify';
import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { MastraServer } from '../index';

/**
 * PF-2594 memoized WHATWG Request lanes.
 *
 * The adapter builds the WHATWG Request lazily and memoizes it per LANE:
 * auth callbacks (authenticateToken / legacy authorize) share ONE instance,
 * and the handler-visible `ctx.request` is a SEPARATE instance. Auth
 * callbacks receive the constructed Request with its live Headers and may
 * mutate it — those mutations must never leak into what the handler sees.
 * The no-auth path must keep constructing ZERO Requests per request.
 */

/** Auth headers seen by each participant during a single request. */
interface SeenHeaders {
  authMutated: string | null;
  authorizeMutated: string | null;
  authorization: string | null;
}

describe('WHATWG Request lane isolation (PF-2594)', () => {
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

  it('keeps auth-callback header mutations out of the handler-visible ctx.request while auth still works', async () => {
    app = Fastify();

    let authorizeSawAuthenticateMutation: string | null | undefined;

    // Mutating auth callbacks: authenticateToken writes AND deletes headers on
    // the Request it is handed; legacy authorize mutates its context Request.
    const originalGetServer = context.mastra.getServer.bind(context.mastra);
    context.mastra.getServer = () =>
      ({
        ...originalGetServer(),
        auth: {
          authenticateToken: async (token: string, authRequest: { headers: Headers }) => {
            authRequest.headers.set('x-auth-mutated', 'from-authenticate');
            authRequest.headers.delete('authorization');
            return token === 'valid-token' ? { id: 'user-1' } : null;
          },
          authorize: async (_path: string, _method: string, user: unknown, webRequest: globalThis.Request) => {
            // Within the auth lane the SAME instance is shared (constructed
            // lazily, once): authorize observes authenticateToken's mutation.
            authorizeSawAuthenticateMutation = webRequest.headers.get('x-auth-mutated');
            webRequest.headers.set('x-authorize-mutated', 'from-authorize');
            return Boolean(user);
          },
        },
      }) as ReturnType<typeof originalGetServer>;

    const adapter = new MastraServer({ app, mastra: context.mastra });

    let seenByHandler: SeenHeaders | undefined;
    const route: ServerRoute<any, any, any> = {
      method: 'GET',
      path: '/api/test/web-request-isolation',
      responseType: 'json',
      handler: async (params: any) => {
        const handlerRequest = params.request as globalThis.Request;
        seenByHandler = {
          authMutated: handlerRequest.headers.get('x-auth-mutated'),
          authorizeMutated: handlerRequest.headers.get('x-authorize-mutated'),
          authorization: handlerRequest.headers.get('authorization'),
        };
        return { ok: true };
      },
    };

    adapter.registerContextMiddleware();
    await adapter.registerRoute(app, route, { prefix: '' });
    const address = await app.listen({ port: 0 });

    // Auth still works: a valid token authenticates AND authorizes.
    const authorized = await fetch(`${address}/api/test/web-request-isolation`, {
      headers: { authorization: 'Bearer valid-token' },
    });
    expect(authorized.status).toBe(200);
    await expect(authorized.json()).resolves.toEqual({ ok: true });

    // The auth lane shares one instance across authenticateToken + authorize.
    expect(authorizeSawAuthenticateMutation).toBe('from-authenticate');

    // The handler-visible Request never observes ANY auth mutation: no
    // injected headers, and the deleted authorization header is still intact.
    expect(seenByHandler).toEqual({
      authMutated: null,
      authorizeMutated: null,
      authorization: 'Bearer valid-token',
    });

    // Auth still rejects: a missing token is a 401, not a pass-through.
    const rejected = await fetch(`${address}/api/test/web-request-isolation`);
    expect(rejected.status).toBe(401);
  });
});

describe('WHATWG Request construction counting (PF-2594)', () => {
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

  /**
   * Deterministic construction probe (same technique as the PF-2594 bench):
   * `toWebRequest` resolves `globalThis.Request` dynamically, so a counting
   * subclass observes every adapter-side construction.
   */
  async function withCountingRequest(run: (constructions: () => number) => Promise<void>): Promise<void> {
    const OriginalRequest = globalThis.Request;
    let count = 0;
    class CountingRequest extends OriginalRequest {
      constructor(...args: ConstructorParameters<typeof OriginalRequest>) {
        count += 1;
        super(...args);
      }
    }
    globalThis.Request = CountingRequest as typeof globalThis.Request;
    try {
      await run(() => count);
    } finally {
      globalThis.Request = OriginalRequest;
    }
  }

  it('constructs ZERO Requests for a no-auth request whose handler never reads ctx.request', async () => {
    app = Fastify();
    const adapter = new MastraServer({ app, mastra: context.mastra });

    const route: ServerRoute<any, any, any> = {
      method: 'GET',
      path: '/api/test/count-no-auth',
      responseType: 'json',
      handler: async () => ({ ok: true }),
    };

    adapter.registerContextMiddleware();
    await adapter.registerRoute(app, route, { prefix: '' });
    await app.ready();

    await withCountingRequest(async constructions => {
      for (let i = 0; i < 3; i += 1) {
        const response = await app!.inject({ method: 'GET', url: '/api/test/count-no-auth' });
        expect(response.statusCode).toBe(200);
      }
      expect(constructions()).toBe(0);
    });
  });

  it('constructs exactly one Request for the whole auth lane, plus one when the handler reads ctx.request', async () => {
    app = Fastify();

    const originalGetServer = context.mastra.getServer.bind(context.mastra);
    context.mastra.getServer = () =>
      ({
        ...originalGetServer(),
        auth: {
          authenticateToken: async (token: string) => (token === 'valid-token' ? { id: 'user-1' } : null),
          // Legacy authorize reads its context Request: a second auth-side
          // consumer that must NOT trigger a second construction.
          authorize: async (_path: string, _method: string, user: unknown, webRequest: globalThis.Request) =>
            Boolean(user) && webRequest.headers.has('authorization'),
        },
      }) as ReturnType<typeof originalGetServer>;

    const adapter = new MastraServer({ app, mastra: context.mastra });

    const readingRoute: ServerRoute<any, any, any> = {
      method: 'GET',
      path: '/api/test/count-auth-read',
      responseType: 'json',
      handler: async (params: any) => ({ host: (params.request as globalThis.Request).headers.get('host') }),
    };
    const nonReadingRoute: ServerRoute<any, any, any> = {
      method: 'GET',
      path: '/api/test/count-auth-noread',
      responseType: 'json',
      handler: async () => ({ ok: true }),
    };

    adapter.registerContextMiddleware();
    await adapter.registerRoute(app, readingRoute, { prefix: '' });
    await adapter.registerRoute(app, nonReadingRoute, { prefix: '' });
    await app.ready();

    await withCountingRequest(async constructions => {
      const reading = await app!.inject({
        method: 'GET',
        url: '/api/test/count-auth-read',
        headers: { authorization: 'Bearer valid-token' },
      });
      expect(reading.statusCode).toBe(200);
      // One shared auth-lane instance + one handler-lane instance.
      expect(constructions()).toBe(2);

      const nonReading = await app!.inject({
        method: 'GET',
        url: '/api/test/count-auth-noread',
        headers: { authorization: 'Bearer valid-token' },
      });
      expect(nonReading.statusCode).toBe(200);
      // Only the auth lane constructed; the handler never read ctx.request.
      expect(constructions()).toBe(3);
    });
  });
});
