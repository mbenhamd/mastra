import { execFileSync } from 'node:child_process';
import { createRequire } from 'node:module';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

import { describe, it } from 'vitest';

const packageRoot = resolve(dirname(fileURLToPath(import.meta.url)), '../..');
const requireFromTest = createRequire(import.meta.url);

function runNode(args: string[]) {
  execFileSync(process.execPath, args, {
    cwd: packageRoot,
    encoding: 'utf8',
    timeout: 30_000,
  });
}

describe('selected public package exports', () => {
  it('loads the built ESM server and Fastify subpaths without evaluating the global route registry', () => {
    runNode([
      '--input-type=module',
      '--eval',
      `
        import assert from 'node:assert/strict';
        import inspector from 'node:inspector';

        const routeKey = route => \`${'${route.method}'} ${'${route.path}'}\`;
        const isRouteRegistry = value =>
          Array.isArray(value) &&
          value.length > 0 &&
          value.every(
            route =>
              route &&
              typeof route === 'object' &&
              typeof route.method === 'string' &&
              typeof route.path === 'string' &&
              typeof route.handler === 'function',
          );
        const assertOnlyHarnessRegistries = (namespaces, harnessRoutes) => {
          const harnessRouteKeys = new Set(harnessRoutes.map(routeKey));
          for (const namespace of namespaces) {
            for (const candidate of Object.values(namespace)) {
              if (!isRouteRegistry(candidate)) continue;
              const unexpectedRoute = candidate.find(route => !harnessRouteKeys.has(routeKey(route)));
              assert.equal(
                unexpectedRoute,
                undefined,
                \`selected subpaths evaluated a non-Harness route registry containing ${'${routeKey(unexpectedRoute ?? {})}'}\`,
              );
            }
          }
        };

        const session = new inspector.Session();
        const evaluatedUrls = new Set();
        session.connect();
        session.on('Debugger.scriptParsed', message => evaluatedUrls.add(message.params.url));
        await new Promise((resolve, reject) => {
          session.post('Debugger.enable', error => (error ? reject(error) : resolve()));
        });

        let app;
        try {
          const selectedServerUrl = import.meta.resolve('@mastra/server/server-adapter/selected');
          const serverDistUrl = new URL('../../', selectedServerUrl).href;
          const selectedServer = await import('@mastra/server/server-adapter/selected');
          const harnessRoutes = await import('@mastra/server/server-adapter/routes/harness');
          const selectedFastify = await import('@mastra/fastify/selected');
          const { Mastra } = await import('@mastra/core/mastra');
          const { default: Fastify } = await import('fastify');

          assert.equal(Object.hasOwn(selectedServer, 'SERVER_ROUTES'), false);
          assert.equal(Object.hasOwn(selectedFastify, 'SERVER_ROUTES'), false);
          assert.equal(harnessRoutes.HARNESS_SESSION_CONTROL_ROUTES.length, 3);

          app = Fastify();
          const adapter = new selectedFastify.MastraServer({
            app,
            mastra: new Mastra({ logger: false }),
            routeRegistry: harnessRoutes.HARNESS_SESSION_CONTROL_ROUTES,
          });
          const selectedRoutes = adapter.getServerRoutes();
          assert.equal(Object.isFrozen(selectedRoutes), true);
          assert.deepEqual(selectedRoutes.map(routeKey), harnessRoutes.HARNESS_SESSION_CONTROL_ROUTES.map(routeKey));
          assert.ok(selectedRoutes.every((route, index) => route === harnessRoutes.HARNESS_SESSION_CONTROL_ROUTES[index]));

          await new Promise(resolve => setImmediate(resolve));
          const evaluatedServerUrls = [...evaluatedUrls].filter(
            url => url.startsWith(serverDistUrl) && url.endsWith('.js'),
          );
          const evaluatedServerModules = await Promise.all(evaluatedServerUrls.map(url => import(url)));
          assertOnlyHarnessRegistries(evaluatedServerModules, harnessRoutes.HARNESS_ROUTES);
        } finally {
          if (app) await app.close();
          session.disconnect();
        }
      `,
    ]);
  });

  it('loads the built CommonJS server and Fastify subpaths without evaluating the global route registry', () => {
    runNode([
      '--eval',
      `
        const assert = require('node:assert/strict');
        const { dirname, resolve, sep } = require('node:path');

        const routeKey = route => \`${'${route.method}'} ${'${route.path}'}\`;
        const isRouteRegistry = value =>
          Array.isArray(value) &&
          value.length > 0 &&
          value.every(
            route =>
              route &&
              typeof route === 'object' &&
              typeof route.method === 'string' &&
              typeof route.path === 'string' &&
              typeof route.handler === 'function',
          );
        const assertOnlyHarnessRegistries = (namespaces, harnessRoutes) => {
          const harnessRouteKeys = new Set(harnessRoutes.map(routeKey));
          for (const namespace of namespaces) {
            for (const candidate of Object.values(namespace)) {
              if (!isRouteRegistry(candidate)) continue;
              const unexpectedRoute = candidate.find(route => !harnessRouteKeys.has(routeKey(route)));
              assert.equal(
                unexpectedRoute,
                undefined,
                \`selected subpaths evaluated a non-Harness route registry containing ${'${routeKey(unexpectedRoute ?? {})}'}\`,
              );
            }
          }
        };

        void (async () => {
          const selectedServerEntry = require.resolve('@mastra/server/server-adapter/selected');
          const serverDistPath = resolve(dirname(selectedServerEntry), '../..') + sep;
          const selectedServer = require('@mastra/server/server-adapter/selected');
          const harnessRoutes = require('@mastra/server/server-adapter/routes/harness');
          const selectedFastify = require('@mastra/fastify/selected');
          const { Mastra } = require('@mastra/core/mastra');
          const Fastify = require('fastify');

          assert.equal(Object.hasOwn(selectedServer, 'SERVER_ROUTES'), false);
          assert.equal(Object.hasOwn(selectedFastify, 'SERVER_ROUTES'), false);
          assert.equal(harnessRoutes.HARNESS_SESSION_CONTROL_ROUTES.length, 3);

          const app = Fastify();
          try {
            const adapter = new selectedFastify.MastraServer({
              app,
              mastra: new Mastra({ logger: false }),
              routeRegistry: harnessRoutes.HARNESS_SESSION_CONTROL_ROUTES,
            });
            const selectedRoutes = adapter.getServerRoutes();
            assert.equal(Object.isFrozen(selectedRoutes), true);
            assert.deepEqual(selectedRoutes.map(routeKey), harnessRoutes.HARNESS_SESSION_CONTROL_ROUTES.map(routeKey));
            assert.ok(
              selectedRoutes.every((route, index) => route === harnessRoutes.HARNESS_SESSION_CONTROL_ROUTES[index]),
            );

            const evaluatedServerModules = Object.values(require.cache)
              .filter(module => typeof module?.filename === 'string' && module.filename.startsWith(serverDistPath))
              .map(module => module.exports);
            assertOnlyHarnessRegistries(evaluatedServerModules, harnessRoutes.HARNESS_ROUTES);
          } finally {
            await app.close();
          }
        })().catch(error => {
          console.error(error);
          process.exitCode = 1;
        });
      `,
    ]);
  });

  it('resolves the built selected declaration exports for NodeNext ESM and CommonJS consumers', () => {
    runNode([
      requireFromTest.resolve('typescript/bin/tsc'),
      '--project',
      'test-fixtures/selected-package-types/tsconfig.json',
    ]);
  });
});
