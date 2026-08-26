import { SERVER_ROUTES } from '@mastra/server/server-adapter';
import type { MastraServerOptions } from '@mastra/server/server-adapter/selected';
import type { FastifyInstance } from 'fastify';

import { MastraServer as SelectedMastraServer } from './selected';

export * from './selected';

/**
 * Fastify adapter with the complete built-in Mastra route registry.
 *
 * Use `@mastra/fastify/selected` with a reviewed per-domain `routeRegistry`
 * when the host intentionally exposes only a small built-in route set.
 */
export class MastraServer extends SelectedMastraServer {
  constructor(options: MastraServerOptions<FastifyInstance>) {
    super({ ...options, routeRegistry: SERVER_ROUTES });
  }
}
