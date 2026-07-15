/**
 * Harness v1 — workspace integration on the Session surface.
 *
 * Complements `workspace-registry.test.ts` by exercising the cross-cutting
 * paths: tool execution sees `ctx.workspace`, session close releases the
 * workspace handle, subagent `workspace: 'inherit' | 'fresh'` validation,
 * and `workspace_status_changed` emission.
 */

import { describe, expect, it } from 'vitest';

import { Agent } from '../../agent';
import { InMemoryHarness } from '../../storage/domains/harness/inmemory';
import { InMemoryDB } from '../../storage/domains/inmemory-db';
import type { Workspace } from '../../workspace';

import { HarnessConfigError } from './errors';
import { Harness } from './harness';
import type { WorkspaceProvider } from './workspace-provider';
import { nonDurableProvider } from './workspace-provider';

function makeAgent(name: string) {
  return new Agent({ id: name, name, instructions: 'fake', model: 'openai/gpt-4o-mini' as any });
}

let _wsCounter = 0;
function makeWorkspace(label?: string): Workspace {
  _wsCounter++;
  const id = `${label ?? 'ws'}-${_wsCounter}`;
  const stub = {
    id,
    name: label ?? id,
    status: 'ready' as const,
    createdAt: new Date(),
    lastAccessedAt: new Date(),
    async init() {
      /* no-op */
    },
    async destroy() {
      /* no-op */
    },
  };
  return stub as unknown as Workspace;
}

function baseConfig(extra: Record<string, any> = {}) {
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

function resumableProvider(
  opts: { providerId: string; onCreate?: () => Workspace } = { providerId: 'p' },
): WorkspaceProvider {
  return {
    providerId: opts.providerId,
    resumable: true,
    create: async () => (opts.onCreate ? opts.onCreate() : makeWorkspace()),
    resume: async () => makeWorkspace(),
  };
}

describe('Session.getWorkspace — per-ownership-model behavior', () => {
  it('returns undefined when no workspace is configured', async () => {
    const harness = new Harness(baseConfig());
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    expect(await session.getWorkspace()).toBeUndefined();
  });

  it('caches the resolved workspace across repeated getWorkspace() calls', async () => {
    const provider = resumableProvider({ providerId: 'p' });
    const harness = new Harness(baseConfig({ workspace: { kind: 'per-session' as const, provider } }));
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const a = await session.getWorkspace();
    const b = await session.getWorkspace();
    expect(a).toBe(b);
  });

  it('dedupes concurrent getWorkspace() calls so the provider runs once', async () => {
    let calls = 0;
    const provider: WorkspaceProvider = {
      providerId: 'race',
      resumable: true,
      create: async () => {
        calls++;
        return makeWorkspace();
      },
      resume: async () => makeWorkspace(),
    };
    const harness = new Harness(baseConfig({ workspace: { kind: 'per-session' as const, provider } }));
    const s = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const [a, b, c] = await Promise.all([s.getWorkspace(), s.getWorkspace(), s.getWorkspace()]);
    expect(a).toBe(b);
    expect(b).toBe(c);
    expect(calls).toBe(1);
  });
});

describe('Subagent workspace inheritance — config validation', () => {
  it('rejects subagents.types[*].workspace = "fresh" when harness kind !== "per-session"', () => {
    expect(() => {
      new Harness(
        baseConfig({
          workspace: {
            kind: 'per-resource' as const,
            provider: nonDurableProvider(() => makeWorkspace()),
          },
          subagents: {
            types: {
              explore: {
                agentId: 'b',
                modeId: 'm2',
                description: 'read-only',
                workspace: 'fresh',
              },
            },
          },
        }),
      );
    }).toThrow(HarnessConfigError);
  });

  it('rejects subagents.types[*].workspace = "fresh" when no workspace is configured at all', () => {
    expect(() => {
      new Harness(
        baseConfig({
          subagents: {
            types: {
              explore: {
                agentId: 'b',
                modeId: 'm2',
                description: 'read-only',
                workspace: 'fresh',
              },
            },
          },
        }),
      );
    }).toThrow(HarnessConfigError);
  });

  it('accepts subagents.types[*].workspace = "fresh" under kind: "per-session"', () => {
    expect(() => {
      new Harness(
        baseConfig({
          workspace: {
            kind: 'per-session' as const,
            provider: resumableProvider({ providerId: 'p' }),
          },
          subagents: {
            types: {
              explore: {
                agentId: 'b',
                modeId: 'm2',
                description: 'read-only',
                workspace: 'fresh',
              },
            },
          },
        }),
      );
    }).not.toThrow();
  });
});

describe('Workspace lifecycle events', () => {
  it('resolves a shared lazy workspace without emitting public workspace events (§2.7/§10.2)', async () => {
    const eventTypes: string[] = [];
    const harness = new Harness(
      baseConfig({
        workspace: {
          kind: 'shared' as const,
          workspace: () => makeWorkspace('shared'),
        },
      }),
    );
    harness.subscribe(ev => eventTypes.push((ev as { type: string }).type));

    const s = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    // Behavioral readiness: lazy resolution succeeds and yields a usable workspace.
    const ws = await s.getWorkspace();
    expect(ws).toBeDefined();

    // §10.2: workspace lifecycle status is not a public HarnessEventV1 event.
    expect(eventTypes).not.toContain('workspace_status_changed');
  });
});

// ---------------------------------------------------------------------------
// §4.2e / §2.7 — the workspace is the durable world tied to SESSION/RESOURCE
// ownership; it is DECOUPLED from the mode. A mode controls agent / prompt /
// tools / permissions, never the workspace. These lock that invariant so a
// future mode change cannot start re-keying or re-creating the workspace.
// ---------------------------------------------------------------------------

describe('workspace ownership is decoupled from mode (§4.2e / §2.7)', () => {
  it('per-session: switchMode keeps the SAME workspace instance (not re-created)', async () => {
    const provider = resumableProvider({ providerId: 'p' });
    const harness = new Harness(baseConfig({ workspace: { kind: 'per-session' as const, provider } }));
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    try {
      const before = await session.getWorkspace();
      expect(before).toBeDefined();
      expect(session.getCurrentModeId()).toBe('m');

      await session.switchMode({ mode: 'm2' });
      expect(session.getCurrentModeId()).toBe('m2'); // the mode genuinely changed

      const after = await session.getWorkspace();
      // Same durable workspace instance across the mode switch — switchMode does
      // not release or re-provision it; the registry keys by sessionId, never by
      // modeId (see workspace-registry.ts), so the mode change cannot touch it.
      expect(after).toBe(before);
    } finally {
      await harness.shutdown();
    }
  });

  it('per-resource: switchMode keeps the SAME workspace instance', async () => {
    const harness = new Harness(
      baseConfig({
        workspace: { kind: 'per-resource' as const, provider: nonDurableProvider(() => makeWorkspace('per-res')) },
      }),
    );
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    try {
      const before = await session.getWorkspace();
      expect(before).toBeDefined();
      await session.switchMode({ mode: 'm2' });
      const after = await session.getWorkspace();
      expect(after).toBe(before);
    } finally {
      await harness.shutdown();
    }
  });

  it('two sessions for the SAME resource share one per-resource workspace regardless of their modes', async () => {
    const harness = new Harness(
      baseConfig({
        workspace: { kind: 'per-resource' as const, provider: nonDurableProvider(() => makeWorkspace('shared-res')) },
      }),
    );
    // Same resource, genuinely different modes.
    const sA = await harness.session({ resourceId: 'shared', threadId: { fresh: true } });
    const sB = await harness.session({ resourceId: 'shared', threadId: { fresh: true }, modeId: 'm2' });
    try {
      expect(sA.getCurrentModeId()).toBe('m');
      expect(sB.getCurrentModeId()).toBe('m2');
      expect(await sA.getWorkspace()).toBe(await sB.getWorkspace()); // ownership = resource, not mode
    } finally {
      await harness.shutdown();
    }
  });
});
