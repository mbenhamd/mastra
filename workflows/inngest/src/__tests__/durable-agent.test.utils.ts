/**
 * Test utilities for InngestAgent tests
 *
 * All tests share the same Inngest infrastructure. The workflow reconstructs
 * tools/model from Mastra at runtime by looking up the agent via agentId,
 * so test isolation is achieved through unique agent IDs and run IDs
 * rather than separate Inngest apps.
 */
import crypto from 'node:crypto';
import { serve } from '@hono/node-server';
import type { ServerType } from '@hono/node-server';
import { Mastra } from '@mastra/core/mastra';
import type { ApiRoute } from '@mastra/core/server';
import { createHonoServer } from '@mastra/deployer/server';
import { DefaultStorage } from '@mastra/libsql';
import { Inngest } from 'inngest';

import { serve as inngestServe } from '../index';
import {
  createInngestTestRuntimeConfig,
  createLocalTestEndpoints,
  InngestTestRuntimeManager,
} from './inngest-test-runtime';

export const INNGEST_PORT = 4100;
export const HANDLER_PORT = 4101;

const DURABLE_AGENT_TEST_ENDPOINTS = createLocalTestEndpoints({
  inngestPort: INNGEST_PORT,
  handlerPort: HANDLER_PORT,
});

// =============================================================================
// Shared Test Infrastructure
// =============================================================================

/** Shared state for all tests - initialized once in beforeAll */
let sharedInngest: Inngest | null = null;
let sharedMastra: Mastra | null = null;
let sharedServer: ServerType | null = null;
let sharedRuntime: InngestTestRuntimeManager | null = null;

type ApiRouteCreateHandler = Extract<ApiRoute, { createHandler: unknown }>['createHandler'];
type ApiRouteHandler = Awaited<ReturnType<ApiRouteCreateHandler>>;

/**
 * Generate unique test ID to isolate each test.
 * Uses a short UUID for readability in logs.
 */
export function generateTestId(): string {
  return crypto.randomUUID().slice(0, 8);
}

/**
 * Get the shared Inngest client.
 * All tests use the same Inngest client since workflow state is isolated by runId/agentId.
 */
export function getSharedInngest(): Inngest {
  if (!sharedInngest) {
    sharedInngest = new Inngest({
      id: 'durable-agent-tests',
      baseUrl: `http://localhost:${INNGEST_PORT}`,
    });
  }
  return sharedInngest;
}

/**
 * Get the shared Mastra instance.
 * @throws Error if called before setupSharedTestInfrastructure()
 */
export function getSharedMastra(): Mastra {
  if (!sharedMastra) {
    throw new Error('Shared Mastra not initialized. Call setupSharedTestInfrastructure() first.');
  }
  return sharedMastra;
}

/**
 * Wait for Inngest to sync with the app.
 */
export async function waitForInngestSync(ms = 500): Promise<void> {
  await new Promise(resolve => setTimeout(resolve, ms));
}

async function closeServer(server: ServerType): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    server.close(error => {
      if ((error as NodeJS.ErrnoException | undefined)?.code === 'ERR_SERVER_NOT_RUNNING') resolve();
      else if (error) reject(error);
      else resolve();
    });
  });
}

export async function cleanupSharedTestResources<TServer, TRuntime>(params: {
  server: TServer | null;
  runtime: TRuntime | null;
  closeServer: (server: TServer) => Promise<void>;
  stopRuntime: (runtime: TRuntime) => Promise<void>;
}): Promise<{ server: TServer | null; runtime: TRuntime | null; errors: unknown[] }> {
  let { server, runtime } = params;
  const errors: unknown[] = [];

  if (runtime) {
    try {
      await params.stopRuntime(runtime);
      runtime = null;
    } catch (error) {
      errors.push(error);
    }
  }
  if (server) {
    try {
      await params.closeServer(server);
      server = null;
    } catch (error) {
      errors.push(error);
    }
  }

  return { server, runtime, errors };
}

async function cleanupSharedTestInfrastructureResources(): Promise<unknown[]> {
  const server = sharedServer;
  const runtime = sharedRuntime;
  const cleanup = await cleanupSharedTestResources({
    server,
    runtime,
    closeServer,
    stopRuntime: activeRuntime => activeRuntime.stop(),
  });

  if (sharedServer === server) sharedServer = cleanup.server;
  if (sharedRuntime === runtime) sharedRuntime = cleanup.runtime;
  return cleanup.errors;
}

/**
 * Initialize shared test infrastructure.
 * Call this once in beforeAll for the test suite.
 *
 * The handler starts before the policy-owned, digest-pinned Inngest runtime so
 * registration readiness proves this exact test app is reachable.
 */
export async function setupSharedTestInfrastructure(): Promise<void> {
  if (sharedRuntime || sharedServer) {
    throw new Error('Shared Inngest test infrastructure still requires cleanup; retry teardown before setup.');
  }

  const runtime = new InngestTestRuntimeManager(createInngestTestRuntimeConfig(DURABLE_AGENT_TEST_ENDPOINTS));
  sharedRuntime = runtime;

  try {
    // Create shared Inngest client
    const inngest = getSharedInngest();

    // Create the shared workflow
    const { createInngestDurableAgenticWorkflow } = await import('../durable-agent/create-inngest-agentic-workflow');
    const workflow = createInngestDurableAgenticWorkflow({ inngest });

    // Create shared Mastra instance with the workflow pre-registered
    // This is required because Inngest reads workflows at serve() time
    sharedMastra = new Mastra({
      storage: new DefaultStorage({
        id: 'shared-test-storage',
        url: ':memory:',
      }),
      workflows: {
        [workflow.id]: workflow,
      },
      server: {
        apiRoutes: [
          {
            path: '/inngest/api',
            method: 'ALL',
            createHandler: async ({ mastra }) =>
              inngestServe({
                mastra,
                inngest,
                ...runtime.registerOptions,
              }) as unknown as ApiRouteHandler,
          },
        ],
      },
    });

    // Create and start shared server
    const app = await createHonoServer(sharedMastra);
    await new Promise<void>((resolve, reject) => {
      const onError = (error: Error) => {
        server.off('error', onError);
        reject(error);
      };
      const server = serve(
        {
          fetch: app.fetch,
          port: HANDLER_PORT,
        },
        () => {
          server.off('error', onError);
          resolve();
        },
      );
      sharedServer = server;
      server.once('error', onError);
    });

    await runtime.ensureReady([`workflow.${workflow.id}`]);
  } catch (error) {
    const cleanupErrors = await cleanupSharedTestInfrastructureResources();
    sharedMastra = null;
    sharedInngest = null;

    if (cleanupErrors.length > 0) {
      throw new AggregateError([error, ...cleanupErrors], 'Failed to set up and clean up shared Inngest test runtime');
    }
    throw error;
  }
}

/**
 * Teardown shared test infrastructure.
 * Call this once in afterAll for the test suite.
 */
export async function teardownSharedTestInfrastructure(): Promise<void> {
  const cleanupErrors = await cleanupSharedTestInfrastructureResources();
  sharedMastra = null;
  sharedInngest = null;

  if (cleanupErrors.length === 1) throw cleanupErrors[0];
  if (cleanupErrors.length > 1) {
    throw new AggregateError(cleanupErrors, 'Failed to clean up shared Inngest test infrastructure');
  }
}

// =============================================================================
// Compatibility API
// =============================================================================

/**
 * Test setup result containing everything needed to run a test.
 */
export interface TestSetup {
  mastra: Mastra;
  cleanup: () => Promise<void>;
}
