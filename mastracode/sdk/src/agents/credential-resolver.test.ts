import { EventEmitterPubSub } from '@mastra/core/events';
import { Mastra } from '@mastra/core/mastra';
import { MASTRA_AUTH_ORGANIZATION_KEY, RequestContext } from '@mastra/core/request-context';
import { MockStore } from '@mastra/core/storage';
import { createStep, createWorkflow } from '@mastra/core/workflows/evented';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { z } from 'zod/v4';
import type { AuthCredential, CredentialStore } from '../auth/types.js';
import {
  hasCredentialStoreProvider,
  resolveCredentialStore,
  resolveTenantFromRequestContext,
  setCredentialStoreProvider,
} from './credential-resolver.js';
import { MastraCodeGateway } from './mastracode-gateway.js';

function fakeStore(data: Record<string, AuthCredential>): CredentialStore {
  return {
    reload: () => {},
    get: provider => data[provider],
    getStoredApiKey: provider => {
      const cred = data[provider];
      return cred?.type === 'api_key' ? cred.key : undefined;
    },
    getApiKey: async provider => {
      const cred = data[provider];
      if (!cred) return undefined;
      return cred.type === 'api_key' ? cred.key : cred.access;
    },
  };
}

afterEach(() => {
  setCredentialStoreProvider(undefined);
});

describe('credential store provider registry', () => {
  it('reports no provider by default', () => {
    expect(hasCredentialStoreProvider()).toBe(false);
    expect(resolveCredentialStore(new RequestContext())).toBeUndefined();
  });

  it('resolves the tenant store when a provider and authenticated user exist', () => {
    const store = fakeStore({});
    let seenTenant: unknown;
    setCredentialStoreProvider(tenant => {
      seenTenant = tenant;
      return store;
    });
    expect(hasCredentialStoreProvider()).toBe(true);

    const ctx = new RequestContext();
    ctx.set('user', { workosId: 'user_1', id: 'prov_1', organizationId: 'org_1' });

    expect(resolveCredentialStore(ctx)).toBe(store);
    expect(seenTenant).toEqual({ orgId: 'org_1', userId: 'user_1' });
  });

  it('fails closed without an authenticated tenant on the request context', async () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
    setCredentialStoreProvider(() => fakeStore({}));
    const withoutContext = resolveCredentialStore(undefined);
    const emptyContext = resolveCredentialStore(new RequestContext());

    expect(withoutContext).toMatchObject({ allowEnvironmentFallback: false });
    expect(emptyContext).toBe(withoutContext);
    await expect(withoutContext?.getApiKey('anthropic')).resolves.toBeUndefined();
    expect(warn).toHaveBeenNthCalledWith(1, '[MastraCode] Tenant credential resolution failed closed', {
      reason: 'missing-user-context',
      hasRequestContext: false,
      factorySession: false,
    });
    expect(warn).toHaveBeenNthCalledWith(2, '[MastraCode] Tenant credential resolution failed closed', {
      reason: 'missing-user-context',
      hasRequestContext: true,
      factorySession: false,
    });
    warn.mockRestore();
  });

  it('fails closed when the tenant store provider cannot resolve a store', async () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
    setCredentialStoreProvider(() => undefined);
    const ctx = new RequestContext();
    ctx.set('user', { workosId: 'user_1', organizationId: 'org_1' });

    const store = resolveCredentialStore(ctx);
    expect(store).toMatchObject({ allowEnvironmentFallback: false });
    await expect(store?.getApiKey('anthropic')).resolves.toBeUndefined();
    expect(warn).toHaveBeenCalledWith('[MastraCode] Tenant credential resolution failed closed', {
      reason: 'credential-store-unavailable',
      hasOrganization: true,
      orgFirst: false,
    });
    warn.mockRestore();
  });

  it('falls back to the provider id when workosId is absent', () => {
    const ctx = new RequestContext();
    ctx.set('user', { id: 'prov_2' });
    expect(resolveTenantFromRequestContext(ctx)).toEqual({ orgId: undefined, userId: 'prov_2' });
  });

  it('reads a session-shaped user, whose org lives on the session half', () => {
    const ctx = new RequestContext();
    ctx.set('user', { session: { activeOrganizationId: 'org_1' }, user: { id: 'prov_3' } });
    expect(resolveTenantFromRequestContext(ctx)).toEqual({ orgId: 'org_1', userId: 'prov_3' });
  });

  it('takes a session-shaped user org from the session half only, never the inner user', () => {
    const ctx = new RequestContext();
    ctx.set('user', { session: {}, user: { id: 'prov_4', organizationId: 'org_9' } });
    // `toFactoryAuthUser` in `@mastra/factory` reads the session half and nothing
    // else. Falling back to the inner user here would resolve a tenant the
    // Factory refuses, and the two would disagree about who the caller is.
    expect(resolveTenantFromRequestContext(ctx)).toEqual({ orgId: undefined, userId: 'prov_4' });
  });

  it('carries the org-first flag only when it is exactly true', () => {
    const flagged = new RequestContext();
    flagged.set('user', { workosId: 'user_1', organizationId: 'org_1', orgFirstCredentials: true });
    expect(resolveTenantFromRequestContext(flagged)).toEqual({ orgId: 'org_1', userId: 'user_1', orgFirst: true });

    const truthy = new RequestContext();
    truthy.set('user', { workosId: 'user_1', organizationId: 'org_1', orgFirstCredentials: 'yes' });
    expect(resolveTenantFromRequestContext(truthy)).toEqual({ orgId: 'org_1', userId: 'user_1' });
  });

  it('reads the org-first flag stamped on a session-shaped wrapper', () => {
    const ctx = new RequestContext();
    ctx.set('user', {
      session: { activeOrganizationId: 'org_1' },
      user: { id: 'prov_6' },
      orgFirstCredentials: true,
    });
    expect(resolveTenantFromRequestContext(ctx)).toEqual({ orgId: 'org_1', userId: 'prov_6', orgFirst: true });
  });

  it('flips org-first for runs on a factory-owned session, keyed off controller state', () => {
    const ctx = new RequestContext();
    ctx.set('user', { workosId: 'user_1', organizationId: 'org_1' });
    ctx.set('controller', { state: { factoryProjectId: 'project-1' } });
    expect(resolveTenantFromRequestContext(ctx)).toEqual({ orgId: 'org_1', userId: 'user_1', orgFirst: true });
  });

  it('keeps user-first when controller state carries no factory project', () => {
    const ctx = new RequestContext();
    ctx.set('user', { workosId: 'user_1', organizationId: 'org_1' });
    ctx.set('controller', { state: { projectPath: '/tmp/x' } });
    expect(resolveTenantFromRequestContext(ctx)).toEqual({ orgId: 'org_1', userId: 'user_1' });
  });

  it('ignores malformed user values', () => {
    const ctx = new RequestContext();
    ctx.set('user', 'not-a-user');
    expect(resolveTenantFromRequestContext(ctx)).toBeUndefined();
  });

  it('refuses a tenant whose resolved user id is not a string', () => {
    const ctx = new RequestContext();
    ctx.set('user', { session: { activeOrganizationId: 'org_1' }, user: { id: 7 } });
    expect(resolveTenantFromRequestContext(ctx)).toBeUndefined();
  });

  it('refuses a tenant whose resolved org id is not a string', () => {
    const ctx = new RequestContext();
    ctx.set('user', { session: { activeOrganizationId: 7 }, user: { id: 'prov_5' } });
    expect(resolveTenantFromRequestContext(ctx)).toBeUndefined();
  });
});

describe('evented workflow tenant authority', () => {
  async function createTenantWorkflow() {
    const storage = new MockStore();
    const seen: Array<ReturnType<typeof resolveTenantFromRequestContext>> = [];
    const schema = z.object({ value: z.string() });
    const observe = createStep({
      id: 'observe-tenant',
      inputSchema: schema,
      outputSchema: schema,
      execute: async ({ inputData, requestContext }) => {
        seen.push(resolveTenantFromRequestContext(requestContext));
        requestContext.set('stepNote', { keep: true });
        return inputData;
      },
    });
    const child = createWorkflow({ id: 'tenant-child', inputSchema: schema, outputSchema: schema })
      .then(observe)
      .commit();
    const pause = createStep({
      id: 'pause-tenant',
      inputSchema: schema,
      outputSchema: schema,
      suspendSchema: z.object({ waiting: z.boolean() }),
      resumeSchema: z.object({ approved: z.boolean() }),
      execute: async ({ inputData, requestContext, resumeData, suspend }) => {
        seen.push(resolveTenantFromRequestContext(requestContext));
        if (!resumeData?.approved) await suspend({ waiting: true });
        return inputData;
      },
    });
    const workflow = createWorkflow({ id: 'tenant-workflow', inputSchema: schema, outputSchema: schema })
      .dowhile(child, async ({ iterationCount }) => iterationCount < 2)
      .then(pause)
      .commit();
    const mastra = new Mastra({
      logger: false,
      storage,
      pubsub: new EventEmitterPubSub(),
      workflows: { [workflow.id]: workflow },
    });
    const store = await storage.getStore('workflows');
    const persist = vi.spyOn(store, 'persistWorkflowSnapshot');
    await mastra.startWorkers();
    const run = await workflow.createRun();
    const requestContext = new RequestContext();
    requestContext.set('user', { id: 'user_1', organizationId: 'org_1' });
    requestContext.set(MASTRA_AUTH_ORGANIZATION_KEY, { userId: 'user_1', organizationId: 'org_2' });
    requestContext.set('workflowNote', { keep: ['original'] });
    return { mastra, workflow, run, store, persist, requestContext, seen };
  }

  it('keeps selected organization live through nested loops without persisting it, then uses fresh resume selection', async () => {
    const { mastra, workflow, run, store, persist, requestContext, seen } = await createTenantWorkflow();
    try {
      expect(await run.start({ inputData: { value: 'ok' }, requestContext })).toMatchObject({ status: 'suspended' });
      const suspended = await store.loadWorkflowSnapshot({ workflowName: workflow.id, runId: run.runId });
      const freshContext = new RequestContext();
      freshContext.set('user', { id: 'user_1', organizationId: 'org_1' });
      freshContext.set(MASTRA_AUTH_ORGANIZATION_KEY, { userId: 'user_1', organizationId: 'org_3' });
      expect(await run.resume({ resumeData: { approved: true }, requestContext: freshContext })).toMatchObject({
        status: 'success',
        result: { value: 'ok' },
      });
      expect(seen).toEqual([
        { userId: 'user_1', orgId: 'org_2' },
        { userId: 'user_1', orgId: 'org_2' },
        { userId: 'user_1', orgId: 'org_2' },
        { userId: 'user_1', orgId: 'org_3' },
      ]);
      expect(suspended?.requestContext).toMatchObject({
        user: { id: 'user_1', organizationId: 'org_1' },
        workflowNote: { keep: ['original'] },
        stepNote: { keep: true },
      });
      expect(suspended?.requestContext).not.toHaveProperty(MASTRA_AUTH_ORGANIZATION_KEY);
      const completed = await store.loadWorkflowSnapshot({ workflowName: workflow.id, runId: run.runId });
      expect(completed?.requestContext).toMatchObject({ workflowNote: { keep: ['original'] } });
      expect(completed?.requestContext).not.toHaveProperty(MASTRA_AUTH_ORGANIZATION_KEY);
      const children = await store.listWorkflowRuns({ workflowName: 'tenant-child' });
      expect(children.total).toBe(2);
      for (const child of children.runs) {
        const snapshot = await store.loadWorkflowSnapshot({ workflowName: 'tenant-child', runId: child.runId });
        expect(snapshot?.requestContext).toMatchObject({ stepNote: { keep: true } });
        expect(snapshot?.requestContext).not.toHaveProperty(MASTRA_AUTH_ORGANIZATION_KEY);
      }
      for (const [{ snapshot }] of persist.mock.calls) {
        expect(snapshot.requestContext ?? {}).not.toHaveProperty(MASTRA_AUTH_ORGANIZATION_KEY);
      }
      expect(requestContext.get(MASTRA_AUTH_ORGANIZATION_KEY)).toEqual({
        userId: 'user_1',
        organizationId: 'org_2',
      });
    } finally {
      await mastra.shutdown();
    }
  });

  it('does not recover a saved organization selection when an async run resumes without one', async () => {
    const { mastra, workflow, run, store, persist, requestContext, seen } = await createTenantWorkflow();
    try {
      await run.startAsync({ inputData: { value: 'ok' }, requestContext });
      await vi.waitFor(async () => {
        expect(await store.loadWorkflowSnapshot({ workflowName: workflow.id, runId: run.runId })).toMatchObject({
          status: 'suspended',
        });
      });
      const suspended = await store.loadWorkflowSnapshot({ workflowName: workflow.id, runId: run.runId });
      const initial = persist.mock.calls.find(
        ([args]) => args.runId === run.runId && args.snapshot.status === 'running',
      )?.[0].snapshot;
      // A retained snapshot from before the exclusion must not supply fresh authority.
      await store.persistWorkflowSnapshot({
        workflowName: workflow.id,
        runId: run.runId,
        snapshot: {
          ...suspended!,
          requestContext: {
            ...suspended?.requestContext,
            [MASTRA_AUTH_ORGANIZATION_KEY]: { userId: 'user_1', organizationId: 'org_2' },
          },
        },
      });
      const freshContext = new RequestContext();
      freshContext.set('user', { id: 'user_1', organizationId: 'org_1' });
      const resumed = await workflow.createRun({ runId: run.runId });
      expect(await resumed.resume({ resumeData: { approved: true }, requestContext: freshContext })).toMatchObject({
        status: 'success',
        result: { value: 'ok' },
      });
      expect(seen.at(-1)).toEqual({ userId: 'user_1', orgId: 'org_1' });
      expect(initial?.requestContext).toMatchObject({ workflowNote: { keep: ['original'] } });
      expect(initial?.requestContext).not.toHaveProperty(MASTRA_AUTH_ORGANIZATION_KEY);
      expect(suspended?.requestContext).not.toHaveProperty(MASTRA_AUTH_ORGANIZATION_KEY);
      expect(freshContext.has(MASTRA_AUTH_ORGANIZATION_KEY)).toBe(false);
    } finally {
      await mastra.shutdown();
    }
  });
});

describe('gateway credentialStore injection', () => {
  const gatewayOptions = {
    mastraGatewayBaseUrl: 'https://gateway.example.com',
    routeThroughMastraGateway: false,
  };

  it('resolves auth from the injected store instead of the global AuthStorage', () => {
    const gateway = new MastraCodeGateway({
      ...gatewayOptions,
      credentialStore: fakeStore({ anthropic: { type: 'api_key', key: 'tenant-key' } }),
    });

    const auth = gateway.resolveAuth({
      gatewayId: 'mastracode',
      providerId: 'anthropic',
      modelId: 'claude-opus-4-6',
      routerId: 'mastracode/anthropic/claude-opus-4-6',
    });
    expect(auth).toEqual({ apiKey: 'tenant-key', source: 'gateway' });
  });

  it('reports the oauth marker when the injected store holds an OAuth credential', () => {
    const gateway = new MastraCodeGateway({
      ...gatewayOptions,
      credentialStore: fakeStore({
        'openai-codex': { type: 'oauth', refresh: 'r', access: 'a', expires: Date.now() + 60_000 },
      }),
    });

    const auth = gateway.resolveAuth({
      gatewayId: 'mastracode',
      providerId: 'openai',
      modelId: 'gpt-5.2-codex',
      routerId: 'mastracode/openai/gpt-5.2-codex',
    });
    expect(auth).toEqual({ bearerToken: 'oauth', source: 'gateway' });
  });

  it('returns undefined from the injected store when the tenant has no credential', () => {
    const gateway = new MastraCodeGateway({
      ...gatewayOptions,
      credentialStore: fakeStore({}),
    });

    const auth = gateway.resolveAuth({
      gatewayId: 'mastracode',
      providerId: 'xai',
      modelId: 'grok-4',
      routerId: 'mastracode/xai/grok-4',
    });
    expect(auth).toBeUndefined();
  });

  it('getApiKey resolves the tenant credential for the model provider', async () => {
    const gateway = new MastraCodeGateway({
      ...gatewayOptions,
      credentialStore: fakeStore({ xai: { type: 'api_key', key: 'tenant-xai-key' } }),
    });
    await expect(gateway.getApiKey('xai/grok-4')).resolves.toBe('tenant-xai-key');
  });
});
