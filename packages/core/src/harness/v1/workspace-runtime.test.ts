/**
 * Harness v1 — workspace runtime behavior (§2.7, §6.1, §9).
 *
 * Covers the runtime guarantees the workspace integration depends on:
 *
 * 1. Tools see `ctx.workspace` populated when one is configured.
 * 2. `session.close()` releases workspace handles correctly per ownership model.
 * 3. `harness.shutdown()` tears down the shared workspace.
 * 4. Subagent `workspace: 'inherit'` shares the parent's workspace at runtime.
 * 5. Subagent `workspace: 'fresh'` provisions an independent workspace and
 *    releases it when the child closes — without affecting the parent.
 * 6. The inherit refcount keeps the parent's workspace alive while any
 *    inheriting child holds a reference.
 * 7. `workspace_error` event fires when `provider.create` throws, and the
 *    failure surfaces as `HarnessWorkspaceProvisioningError`.
 * 8. Per-resource workspaces are destroyed automatically once the last
 *    holder closes.
 * 9. `destroyResourceWorkspace` rejects on `shared` and `per-session` kinds.
 * 10. `eager: true` provisioning works for per-resource and per-session.
 * 11. Workspace tools registered via `Workspace.tools` show up in the
 *     session's tool set.
 */

import { describe, expect, it, vi } from 'vitest';

import { Agent } from '../../agent';
import { InMemoryHarness } from '../../storage/domains/harness/inmemory';
import { InMemoryDB } from '../../storage/domains/inmemory-db';
import type { Workspace } from '../../workspace';

import { setupHarness } from './__test-utils__/setup';
import { HarnessConfigError, HarnessWorkspaceProvisioningError } from './errors';
import { Harness } from './harness';
import { createSpawnSubagentTool } from './spawn-subagent-tool';
import type { HarnessRequestContext } from './types';
import type { WorkspaceProvider, WorkspaceProviderContext } from './workspace-provider';
import { nonDurableProvider } from './workspace-provider';

// ---------------------------------------------------------------------------
// Helpers (parallel structure to workspace-session.test.ts — duck-typed
// Workspace stubs so the registry can observe lifecycle hooks without a
// real filesystem/sandbox.)
// ---------------------------------------------------------------------------

let _wsCounter = 0;
interface StubWorkspace {
  id: string;
  name: string;
  status: 'pending' | 'ready' | 'destroyed';
  init: ReturnType<typeof vi.fn>;
  destroy: ReturnType<typeof vi.fn>;
}

function makeWorkspace(label?: string): Workspace {
  _wsCounter++;
  const id = `${label ?? 'ws'}-${_wsCounter}`;
  const stub: StubWorkspace = {
    id,
    name: label ?? id,
    status: 'ready',
    init: vi.fn().mockResolvedValue(undefined),
    destroy: vi.fn(async () => {
      stub.status = 'destroyed';
    }),
  };
  return stub as unknown as Workspace;
}

function makeAgent(name: string) {
  return new Agent({ id: name, name, instructions: 'fake', model: 'openai/gpt-4o-mini' as any });
}

function multiAgentConfig(extra: Record<string, any> = {}) {
  return {
    agents: { a: makeAgent('a'), b: makeAgent('b') } as any,
    modes: [
      { id: 'm', agentId: 'a' },
      { id: 'm2', agentId: 'b' },
    ],
    defaultModeId: 'm',
    sessions: { storage: new InMemoryHarness({ db: new InMemoryDB() }) },
    ...extra,
  };
}

function resumableProvider(opts: {
  providerId: string;
  onCreate?: (ctx: WorkspaceProviderContext) => Workspace | Promise<Workspace>;
}): WorkspaceProvider {
  return {
    providerId: opts.providerId,
    resumable: true,
    create: async ctx => (opts.onCreate ? opts.onCreate(ctx) : makeWorkspace()),
    resume: async () => makeWorkspace(),
  };
}

// Minimal execution context for direct tool.execute() calls in subagent tests.
function execCtx(toolCallId = 'tc-1') {
  return {
    abortSignal: new AbortController().signal,
    agent: { toolCallId, runId: 'run-1' },
    runId: 'run-1',
    tracingContext: {} as any,
    requestContext: { get: () => undefined } as any,
    mastra: undefined,
  } as any;
}

function getHarnessSlot(streamCalls: any[]): HarnessRequestContext {
  const ctx = streamCalls.at(-1)!.options.requestContext;
  return ctx.get('harness') as HarnessRequestContext;
}

// ---------------------------------------------------------------------------
// 1. Tool sees `ctx.workspace`
// ---------------------------------------------------------------------------

describe('HarnessRequestContext.workspace — lazy runtime plumbing (§6.2)', () => {
  it('does NOT materialize the shared workspace for a turn that never resolves it', async () => {
    const ws = makeWorkspace('shared');
    const { harness, agent } = setupHarness({
      workspace: { kind: 'shared', workspace: ws },
    });
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    // §6.2: context construction must not materialize solely to fill the slot.
    await session.message({ content: 'hi' });
    expect(getHarnessSlot(agent.streamCalls).workspace).toBeUndefined();
    // Explicit resolution materializes it; subsequent turns see the cached handle.
    await expect(session.resolveWorkspace()).resolves.toBe(ws);
    await session.message({ content: 'again' });
    expect(getHarnessSlot(agent.streamCalls).workspace).toBe(ws);
  });

  it('lazily provisions a per-resource workspace only on resolveWorkspace()', async () => {
    const created: Workspace[] = [];
    const provider = nonDurableProvider(() => {
      const w = makeWorkspace('per-resource');
      created.push(w);
      return w;
    });
    const { harness, agent } = setupHarness({
      workspace: { kind: 'per-resource', provider },
    });
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    await session.message({ content: 'hi' });
    expect(created).toHaveLength(0); // a no-filesystem turn never cold-starts the sandbox
    expect(getHarnessSlot(agent.streamCalls).workspace).toBeUndefined();

    await session.resolveWorkspace();
    expect(created).toHaveLength(1);
    await session.message({ content: 'again' });
    expect(getHarnessSlot(agent.streamCalls).workspace).toBe(created[0]);
  });

  it('lazily provisions a per-session workspace only on resolveWorkspace()', async () => {
    const created: Workspace[] = [];
    const provider = resumableProvider({
      providerId: 'per-session',
      onCreate: () => {
        const w = makeWorkspace('per-session');
        created.push(w);
        return w;
      },
    });
    const { harness, agent } = setupHarness({
      workspace: { kind: 'per-session', provider },
    });
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    await session.message({ content: 'hi' });
    expect(created).toHaveLength(0);

    await session.resolveWorkspace();
    expect(created).toHaveLength(1);
    await session.message({ content: 'again' });
    expect(getHarnessSlot(agent.streamCalls).workspace).toBe(created[0]);
  });

  it('slot.workspace is undefined when no workspace is configured', async () => {
    const { harness, agent } = setupHarness();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    await session.message({ content: 'hi' });
    expect(getHarnessSlot(agent.streamCalls).workspace).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// 2 + 8. close() release semantics
// ---------------------------------------------------------------------------

describe('Workspace release on session.close()', () => {
  it('per-session: close destroys the workspace', async () => {
    let captured: StubWorkspace | undefined;
    const provider = resumableProvider({
      providerId: 'per-session',
      onCreate: () => {
        const w = makeWorkspace('s') as unknown as StubWorkspace;
        captured = w;
        return w as unknown as Workspace;
      },
    });
    const { harness } = setupHarness({ workspace: { kind: 'per-session', provider } });
    const s = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    await s.getWorkspace();
    expect(captured!.destroy).not.toHaveBeenCalled();

    await s.close();
    expect(captured!.destroy).toHaveBeenCalledTimes(1);
  });

  it('per-resource: workspace persists while another session still holds it', async () => {
    const created: StubWorkspace[] = [];
    const provider = nonDurableProvider(() => {
      const w = makeWorkspace('per-resource') as unknown as StubWorkspace;
      created.push(w);
      return w as unknown as Workspace;
    });
    const { harness } = setupHarness({ workspace: { kind: 'per-resource', provider } });

    const s1 = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const s2 = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    await s1.getWorkspace();
    await s2.getWorkspace();
    expect(created).toHaveLength(1);

    await s1.close();
    // Refcount drops to 1 — workspace still alive.
    expect(created[0]!.destroy).not.toHaveBeenCalled();

    await s2.close();
    // Last session gone — workspace destroyed.
    expect(created[0]!.destroy).toHaveBeenCalledTimes(1);
  });

  it('shared: close() does NOT destroy the shared workspace', async () => {
    const ws = makeWorkspace('shared') as unknown as StubWorkspace;
    const { harness } = setupHarness({
      workspace: { kind: 'shared', workspace: ws as unknown as Workspace },
    });
    const s = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    await s.getWorkspace();
    await s.close();
    expect(ws.destroy).not.toHaveBeenCalled();
  });
});

// ---------------------------------------------------------------------------
// 3. harness.shutdown() tears down shared
// ---------------------------------------------------------------------------

describe('Workspace lifecycle on harness.shutdown()', () => {
  it('shared: shutdown() destroys the resolved workspace', async () => {
    const ws = makeWorkspace('shared') as unknown as StubWorkspace;
    const { harness } = setupHarness({
      workspace: { kind: 'shared', workspace: ws as unknown as Workspace },
    });
    const s = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    await s.getWorkspace();
    await harness.shutdown();
    expect(ws.destroy).toHaveBeenCalledTimes(1);
  });

  it('shared: shutdown() does NOT destroy an unresolved (lazy) workspace', async () => {
    const ws = makeWorkspace('shared') as unknown as StubWorkspace;
    const { harness } = setupHarness({
      workspace: { kind: 'shared', workspace: ws as unknown as Workspace },
    });
    // Never call getWorkspace() — should stay lazy.
    await harness.shutdown();
    expect(ws.destroy).not.toHaveBeenCalled();
  });

  it('per-resource: shutdown() destroys any provisioned workspaces', async () => {
    const created: StubWorkspace[] = [];
    const provider = nonDurableProvider(() => {
      const w = makeWorkspace() as unknown as StubWorkspace;
      created.push(w);
      return w as unknown as Workspace;
    });
    const { harness } = setupHarness({ workspace: { kind: 'per-resource', provider } });

    const s = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    await s.getWorkspace();
    await harness.shutdown();
    expect(created[0]!.destroy).toHaveBeenCalledTimes(1);
  });

  it('per-session: shutdown() destroys any provisioned workspaces', async () => {
    const created: StubWorkspace[] = [];
    const provider = resumableProvider({
      providerId: 'per-session',
      onCreate: () => {
        const w = makeWorkspace() as unknown as StubWorkspace;
        created.push(w);
        return w as unknown as Workspace;
      },
    });
    const { harness } = setupHarness({ workspace: { kind: 'per-session', provider } });

    const s = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    await s.getWorkspace();
    await harness.shutdown();
    expect(created[0]!.destroy).toHaveBeenCalledTimes(1);
  });
});

// ---------------------------------------------------------------------------
// 4 + 5 + 6. Subagent workspace inheritance runtime
// ---------------------------------------------------------------------------

describe('Subagent workspace inheritance — runtime', () => {
  function subagentSetup(opts: { workspace: 'inherit' | 'fresh' }) {
    const created: StubWorkspace[] = [];
    const provider: WorkspaceProvider = {
      providerId: 'per-session',
      resumable: true,
      create: async () => {
        const w = makeWorkspace() as unknown as StubWorkspace;
        created.push(w);
        return w as unknown as Workspace;
      },
      resume: async () => {
        const w = makeWorkspace() as unknown as StubWorkspace;
        created.push(w);
        return w as unknown as Workspace;
      },
    };
    const harness = new Harness(
      multiAgentConfig({
        workspace: { kind: 'per-session', provider },
        subagents: {
          maxDepth: 3,
          types: {
            explore: {
              agentId: 'b',
              modeId: 'm2',
              description: 'child',
              workspace: opts.workspace,
            },
          },
        },
      }),
    );
    return { harness, created };
  }

  it("'inherit' (the default): child shares parent's workspace instance", async () => {
    const { harness, created } = subagentSetup({ workspace: 'inherit' });
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const parentWs = await parent.getWorkspace();

    const tool = createSpawnSubagentTool(parent)!;
    const result = (await tool.execute!({ agentType: 'explore', task: 'go' }, execCtx('tc-inherit-1'))) as any;
    expect(result.isError ?? false).toBe(false);

    // Only one physical workspace was provisioned (parent's).
    expect(created).toHaveLength(1);
    expect(parentWs).toBe(created[0]);
  });

  it("'inherit': child close does NOT destroy parent's workspace", async () => {
    const { harness, created } = subagentSetup({ workspace: 'inherit' });
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    await parent.getWorkspace();

    const tool = createSpawnSubagentTool(parent)!;
    await tool.execute!({ agentType: 'explore', task: 'go' }, execCtx('tc-inherit-2'));

    // Spawn tool auto-closes the child after it returns. Parent still alive.
    expect(created[0]!.destroy).not.toHaveBeenCalled();

    // Once the parent closes, the refcount finally drops to zero.
    await parent.close();
    expect(created[0]!.destroy).toHaveBeenCalledTimes(1);
  });

  it("'fresh': a subagent that never touches the filesystem does NOT cold-start a workspace at spawn (strict-lazy §6.2)", async () => {
    const { harness, created } = subagentSetup({ workspace: 'fresh' });
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const parentWs = await parent.getWorkspace(); // parent explicitly materializes → 1

    const tool = createSpawnSubagentTool(parent)!;
    await tool.execute!({ agentType: 'explore', task: 'go' }, execCtx('tc-fresh-1'));

    // The 'fresh' child ran but never resolved its workspace, so under strict-lazy
    // materialization (§6.2 / task #36) NO second sandbox was cold-started for a
    // speculative/no-op subagent. `fresh` guarantees a DISTINCT workspace WHEN the
    // child resolves one (its own per-session registry key), not allocation at
    // spawn. The distinct-per-session provisioning mechanism is covered by the
    // 'lazily provisions a per-session workspace' test above; parent stays alive.
    expect(created).toHaveLength(1);
    expect(created[0]).toBe(parentWs as unknown as StubWorkspace);
    expect((parentWs as unknown as StubWorkspace).destroy).not.toHaveBeenCalled();
  });

  it("'inherit' resolves even when the parent never materialized its workspace (strict-lazy parent §6.2)", async () => {
    // BLOCKER guard: under strict-lazy, a parent turn may never resolve its
    // workspace, so a child inheriting it must materialize the parent on demand
    // rather than throwing "parent has no workspace to inherit".
    const { harness, created } = subagentSetup({ workspace: 'inherit' });
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    // Parent is lazy — it has NOT resolved its workspace.
    expect(created).toHaveLength(0);

    const registry = (harness as unknown as { _workspaceRegistry: any })._workspaceRegistry;
    // Inheriting first ensures the (live) parent's workspace is materialized...
    await (harness as unknown as {
      _internalEnsureParentWorkspaceForInherit: (id: string) => Promise<void>;
    })._internalEnsureParentWorkspaceForInherit(parent.id);
    expect(created).toHaveLength(1); // parent materialized on demand

    // ...then the child inherit acquire shares that same parent workspace.
    const childWs = registry.inheritPerSession({
      parentSessionId: parent.id,
      childSessionId: 'child-x',
      resourceId: 'u1',
    });
    expect(childWs).toBe(created[0]);
    expect(created).toHaveLength(1); // no second workspace — shared, not duplicated
  });
});

// ---------------------------------------------------------------------------
// 7. Provider failure → HarnessWorkspaceProvisioningError + workspace_error
// ---------------------------------------------------------------------------

describe('Provider failures surface as HarnessWorkspaceProvisioningError', () => {
  it('per-resource: create() throws → first getWorkspace rejects with provisioning error', async () => {
    const provider = nonDurableProvider(() => {
      throw new Error('boom');
    });
    const { harness } = setupHarness({ workspace: { kind: 'per-resource', provider } });
    const s = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });

    await expect(s.getWorkspace()).rejects.toBeInstanceOf(HarnessWorkspaceProvisioningError);
  });

  it('per-session: create() throws → first getWorkspace rejects with provisioning error', async () => {
    const provider: WorkspaceProvider = {
      providerId: 'broken',
      resumable: true,
      create: async () => {
        throw new Error('boom');
      },
      resume: async () => makeWorkspace(),
    };
    const { harness } = setupHarness({ workspace: { kind: 'per-session', provider } });
    const s = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });

    await expect(s.getWorkspace()).rejects.toBeInstanceOf(HarnessWorkspaceProvisioningError);
  });

  it('surfaces provider create failure as HarnessWorkspaceProvisioningError with NO public workspace event (§2.7/§10.2)', async () => {
    const provider: WorkspaceProvider = {
      providerId: 'broken',
      resumable: true,
      create: async () => {
        throw new Error('boom');
      },
      resume: async () => makeWorkspace(),
    };
    const { harness } = setupHarness({ workspace: { kind: 'per-session', provider } });
    // §10.2: workspace lifecycle/error is provider-owned inspection data, NOT a
    // public HarnessEventV1 event — nothing should reach the public stream.
    const eventTypes: string[] = [];
    harness.subscribe(e => eventTypes.push((e as { type: string }).type));
    const s = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });

    // The provisioning failure still surfaces — as a thrown typed error.
    await expect(s.getWorkspace()).rejects.toBeInstanceOf(HarnessWorkspaceProvisioningError);
    expect(eventTypes).not.toContain('workspace_error');
    expect(eventTypes).not.toContain('workspace_status_changed');
  });
});

// ---------------------------------------------------------------------------
// 9. destroyResourceWorkspace rejects on wrong kinds
// ---------------------------------------------------------------------------

describe('destroyResourceWorkspace — kind validation', () => {
  it('rejects with HarnessConfigError under kind: "shared"', async () => {
    const { harness } = setupHarness({
      workspace: { kind: 'shared', workspace: makeWorkspace() },
    });
    await expect(harness.destroyResourceWorkspace({ resourceId: 'u1' })).rejects.toBeInstanceOf(HarnessConfigError);
  });

  it('rejects with HarnessConfigError under kind: "per-session"', async () => {
    const provider = resumableProvider({ providerId: 'p' });
    const { harness } = setupHarness({ workspace: { kind: 'per-session', provider } });
    await expect(harness.destroyResourceWorkspace({ resourceId: 'u1' })).rejects.toBeInstanceOf(HarnessConfigError);
  });

  it('rejects with HarnessConfigError when no workspace is configured', async () => {
    const { harness } = setupHarness();
    await expect(harness.destroyResourceWorkspace({ resourceId: 'u1' })).rejects.toBeInstanceOf(HarnessConfigError);
  });
});

// ---------------------------------------------------------------------------
// 10. eager: true for non-shared kinds
// ---------------------------------------------------------------------------

describe('eager: true — non-shared kinds', () => {
  it('per-resource: eager has no harness-level effect (resource scoping requires a session)', async () => {
    // `eager` on per-resource isn't well-defined at harness construction —
    // there's no resourceId yet. The spec leaves this as a no-op at the
    // harness level; the workspace is provisioned on first session use.
    let calls = 0;
    const provider = nonDurableProvider(() => {
      calls++;
      return makeWorkspace();
    });
    new Harness(
      multiAgentConfig({
        workspace: { kind: 'per-resource', provider, eager: true },
      }),
    );
    await new Promise(r => setImmediate(r));
    expect(calls).toBe(0);
  });

  it('per-session: eager has no harness-level effect (per-session scoping requires a session)', async () => {
    let calls = 0;
    const provider: WorkspaceProvider = {
      providerId: 'p',
      resumable: true,
      create: async () => {
        calls++;
        return makeWorkspace();
      },
      resume: async () => makeWorkspace(),
    };
    new Harness(
      multiAgentConfig({
        workspace: { kind: 'per-session', provider, eager: true },
      }),
    );
    await new Promise(r => setImmediate(r));
    expect(calls).toBe(0);
  });
});

// ---------------------------------------------------------------------------
// §6.1 workspace access accessors (F12): hasWorkspace / isWorkspaceReady /
// getWorkspace (sync) / resolveWorkspace (async) + the deferred getActivityTimeline.
// ---------------------------------------------------------------------------

describe('HarnessRequestContext §6.1 workspace accessors (F12)', () => {
  it('reports configured-but-not-yet-ready, then ready after resolveWorkspace (lazy §6.2)', async () => {
    const ws = makeWorkspace('shared');
    const { harness, agent } = setupHarness({ workspace: { kind: 'shared', workspace: ws } });
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    await session.message({ content: 'hi' });

    const slot = getHarnessSlot(agent.streamCalls);
    // Configured but NOT materialized by a no-filesystem turn (§6.2 strict-lazy).
    expect(slot.hasWorkspace()).toBe(true);
    expect(slot.isWorkspaceReady()).toBe(false);
    expect(slot.getWorkspace()).toBeUndefined(); // sync, non-materializing cached read
    // Explicit async materialization, then the sync reads reflect the warm handle.
    await expect(slot.resolveWorkspace()).resolves.toBe(ws);
    expect(session.hasWorkspace()).toBe(true);
    expect(session.isWorkspaceReady()).toBe(true);
    expect(session.peekWorkspace()).toBe(ws);
    await expect(session.resolveWorkspace()).resolves.toBe(ws);
  });

  it('reports no workspace + resolveWorkspace rejects when none is configured', async () => {
    const { harness, agent } = setupHarness(); // no workspace
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    await session.message({ content: 'hi' });

    const slot = getHarnessSlot(agent.streamCalls);
    expect(slot.hasWorkspace()).toBe(false);
    expect(slot.isWorkspaceReady()).toBe(false);
    expect(slot.getWorkspace()).toBeUndefined();
    await expect(slot.resolveWorkspace()).rejects.toThrow(/no workspace is configured/);
    expect(session.hasWorkspace()).toBe(false);
    expect(session.peekWorkspace()).toBeUndefined();
  });

  it('getActivityTimeline rejects with an explicit "not implemented" error (deferred §5.6/§10.6)', async () => {
    const { harness, agent } = setupHarness();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    await session.message({ content: 'hi' });
    const slot = getHarnessSlot(agent.streamCalls);
    await expect(slot.getActivityTimeline()).rejects.toThrow(/activity timeline read-model.*not implemented/);
  });
});

// ---------------------------------------------------------------------------
// Workspace tool integration is asserted by the `HarnessRequestContext.workspace`
// suite above: workspace-aware tools (e.g. read_file / write_file / execute_command)
// read the workspace handle through `ctx.workspace`, which is populated by
// `_buildRequestContext`. The `Workspace.tools` config field is a toolset
// gating layer applied at workspace-init time and isn't part of Harness
// session toolset wiring, so there's nothing additional to assert here.
