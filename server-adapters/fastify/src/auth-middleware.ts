import type { Mastra } from '@mastra/core/mastra';
import { RequestContext } from '@mastra/core/request-context';
import { coreAuthMiddleware } from '@mastra/server/auth';
import type { FastifyReply, FastifyRequest, preHandlerHookHandler } from 'fastify';

export interface FastifyAuthMiddlewareOptions {
  mastra: Mastra;
  requiresAuth?: boolean;
}

function toWebRequest(request: FastifyRequest): globalThis.Request {
  // request.protocol/request.host are trustProxy-aware Fastify getters (unlike
  // raw headers.host), and request.host falls back to the HTTP/2 :authority
  // pseudo-header. Keep final fallbacks so fully host-less requests (e.g.
  // HTTP/1.0 without a Host header) never derive an invalid URL.
  const protocol = request.protocol || 'http';
  const host = request.host || 'localhost';
  const url = `${protocol}://${host}${request.url}`;

  const headers = new Headers();
  for (const [key, value] of Object.entries(request.headers)) {
    // HTTP/2 pseudo-headers (:method, :path, :authority, ...) are not legal
    // Web API header names — Headers.set() throws on them.
    if (!value || key.startsWith(':')) continue;
    if (Array.isArray(value)) {
      value.forEach(v => headers.append(key, v));
    } else {
      headers.set(key, value);
    }
  }

  return new globalThis.Request(url, {
    method: request.method,
    headers,
  });
}

export function createAuthMiddleware({
  mastra,
  requiresAuth = true,
}: FastifyAuthMiddlewareOptions): preHandlerHookHandler {
  return async (request: FastifyRequest, reply: FastifyReply) => {
    if (!requiresAuth) {
      return;
    }

    const authConfig = mastra.getServer()?.auth;
    if (!authConfig) {
      return;
    }

    request.requestContext ??= new RequestContext();
    request.mastra ??= mastra;

    const path = String(request.url.split('?')[0] || '/');
    const method = String(request.method || 'GET');
    const customRouteAuthConfig = new Map<string, boolean>(request.customRouteAuthConfig ?? []);
    customRouteAuthConfig.set(`${method}:${path}`, true);

    const authHeader = request.headers.authorization;
    let token: string | null = authHeader ? authHeader.replace('Bearer ', '') : null;
    const query = request.query as Record<string, string>;
    if (!token && query.apiKey) {
      token = query.apiKey || null;
    }

    // At most one WHATWG Request per request, constructed lazily:
    // `coreAuthMiddleware` reads `rawRequest` exactly once, and only AFTER its
    // dev-playground/unprotected/public skip checks, so requests those checks
    // wave through construct nothing. authenticateToken and authorize() share
    // the single instance (it carries method + headers only — no body).
    let webRequest: globalThis.Request | undefined;
    const getWebRequest = () => (webRequest ??= toWebRequest(request));

    const result = await coreAuthMiddleware({
      path,
      method,
      getHeader: name => request.headers[name.toLowerCase()] as string | undefined,
      mastra,
      authConfig,
      customRouteAuthConfig,
      requestContext: request.requestContext,
      get rawRequest() {
        return getWebRequest();
      },
      token,
      buildAuthorizeContext: getWebRequest,
    });

    if (result.action === 'error') {
      return reply.status(result.status).send(result.body);
    }
  };
}
