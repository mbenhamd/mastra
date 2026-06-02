/**
 * §4.2e — Mode-controlled base permission policy + workspace tool profile.
 *
 * A mode controls the agent, prompt, base tools, PERMISSIONS, and (optionally) a
 * workspace tool profile. The workspace itself stays owned by the session/resource
 * (see session.workspace-ownership.test.ts) — these tests cover the two new mode
 * controls only:
 *   - `HarnessMode.permissions` seeds the session's base `permissionRules` on mode
 *     ENTRY (create / switchMode / plan-approval transition). Runtime grants +
 *     setPolicy overlay it until the next mode entry re-establishes the base.
 *   - `HarnessMode.workspaceTools.expose` withholds workspace-category tools
 *     (read/edit/execute) not in the list from the model, leaving mcp/other/
 *     uncategorized tools + harness built-ins untouched.
 */

import { describe, expect, it } from 'vitest';
import { z } from 'zod';

import { createTool } from '../../tools';

import { setupHarness } from './__test-utils__/setup';
import type { HarnessMode } from './types';

// ---------------------------------------------------------------------------
// Mode-seeded permission policy
// ---------------------------------------------------------------------------

const PERMISSION_MODES: HarnessMode[] = [
  {
    id: 'restricted',
    agentId: 'default',
    permissions: { categories: { edit: 'deny', execute: 'ask' }, tools: { dangerousTool: 'deny' } },
  },
  { id: 'open', agentId: 'default', permissions: { categories: {}, tools: {} } },
  { id: 'plain', agentId: 'default' }, // declares NO permissions
];

describe('Mode-seeded base permission policy (§4.2e)', () => {
  it('seeds the initial mode permission policy onto the session at create', async () => {
    const { harness } = setupHarness({ modes: PERMISSION_MODES, defaultModeId: 'restricted' });
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    try {
      const rules = session.permissions.getRules();
      expect(rules.categories.edit).toBe('deny');
      expect(rules.categories.execute).toBe('ask');
      expect(rules.tools.dangerousTool).toBe('deny');
    } finally {
      await harness.shutdown();
    }
  });

  it('switchMode re-establishes the entered mode base policy (replaces the prior one)', async () => {
    const { harness } = setupHarness({ modes: PERMISSION_MODES, defaultModeId: 'restricted' });
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    try {
      await session.switchMode({ mode: 'open' });
      const rules = session.permissions.getRules();
      // open declares an empty base → restricted's deny/ask are gone.
      expect(rules.categories.edit).toBeUndefined();
      expect(rules.categories.execute).toBeUndefined();
      expect(rules.tools.dangerousTool).toBeUndefined();
    } finally {
      await harness.shutdown();
    }
  });

  it('runtime setPolicy overlays the base; the next mode entry re-establishes the base', async () => {
    const { harness } = setupHarness({ modes: PERMISSION_MODES, defaultModeId: 'open' });
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    try {
      await session.permissions.setPolicy({ category: 'read', policy: 'deny' });
      expect(session.permissions.getRules().categories.read).toBe('deny'); // runtime overlay

      await session.switchMode({ mode: 'restricted' });
      const rules = session.permissions.getRules();
      expect(rules.categories.read).toBeUndefined(); // overlay wiped by the re-seeded base
      expect(rules.categories.edit).toBe('deny'); // restricted's declared base
    } finally {
      await harness.shutdown();
    }
  });

  it('entering a mode that declares NO permissions leaves the existing rules untouched (opt-in)', async () => {
    const { harness } = setupHarness({ modes: PERMISSION_MODES, defaultModeId: 'restricted' });
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    try {
      await session.switchMode({ mode: 'plain' }); // no `permissions` field
      // The base from `restricted` persists — `plain` does not own/clear it.
      expect(session.permissions.getRules().categories.edit).toBe('deny');
    } finally {
      await harness.shutdown();
    }
  });

  it('rejects a malformed mode permission policy at construction', () => {
    expect(() =>
      setupHarness({
        modes: [{ id: 'bad', agentId: 'default', permissions: { categories: { edit: 'nope' as any }, tools: {} } }],
        defaultModeId: 'bad',
      }),
    ).toThrow(/permissions/);
  });
});

// ---------------------------------------------------------------------------
// Mode workspace tool profile (exposure filter)
// ---------------------------------------------------------------------------

const tool = (id: string) => createTool({ id, description: id, inputSchema: z.object({}), execute: async () => ({}) });
const categoryResolver = (name: string): 'read' | 'edit' | 'execute' | 'mcp' | 'other' | null =>
  name === 'readDoc'
    ? 'read'
    : name === 'editDoc'
      ? 'edit'
      : name === 'runCmd'
        ? 'execute'
        : name === 'mcpThing'
          ? 'mcp'
          : null;

describe('Mode workspace tool profile (§4.2e)', () => {
  it('withholds workspace-category tools not in expose; mcp/uncategorized stay', async () => {
    const modes: HarnessMode[] = [
      {
        id: 'reader',
        agentId: 'default',
        tools: {
          readDoc: tool('readDoc'),
          editDoc: tool('editDoc'),
          runCmd: tool('runCmd'),
          mcpThing: tool('mcpThing'),
          misc: tool('misc'),
        },
        workspaceTools: { expose: ['read'] },
      },
    ];
    const { harness } = setupHarness({ modes, defaultModeId: 'reader', toolCategoryResolver: categoryResolver });
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    try {
      const toolsets = (session as any)._buildToolsets(harness.getMode('reader')!);
      const modeTools = Object.keys(toolsets['mode:reader']).sort();
      // editDoc (edit) + runCmd (execute) withheld; readDoc kept; mcpThing + misc (uncategorized) untouched.
      expect(modeTools).toEqual(['mcpThing', 'misc', 'readDoc']);
      // The harness built-ins are never filtered.
      expect(toolsets['harness:builtin']).toBeDefined();
    } finally {
      await harness.shutdown();
    }
  });

  it('exposes read+edit+execute when all are listed', async () => {
    const modes: HarnessMode[] = [
      {
        id: 'full',
        agentId: 'default',
        tools: { readDoc: tool('readDoc'), editDoc: tool('editDoc'), runCmd: tool('runCmd') },
        workspaceTools: { expose: ['read', 'edit', 'execute'] },
      },
    ];
    const { harness } = setupHarness({ modes, defaultModeId: 'full', toolCategoryResolver: categoryResolver });
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    try {
      const toolsets = (session as any)._buildToolsets(harness.getMode('full')!);
      expect(Object.keys(toolsets['mode:full']).sort()).toEqual(['editDoc', 'readDoc', 'runCmd']);
    } finally {
      await harness.shutdown();
    }
  });

  it('a SubagentDefinition.tools override is layered onto the subagent tool surface (§9)', async () => {
    const modes: HarnessMode[] = [{ id: 'sub', agentId: 'default' }];
    const { harness } = setupHarness({ modes, defaultModeId: 'sub' });
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    try {
      // Simulate the spawn/delegate wiring that sets the child's override.
      (session as any)._subagentToolsOverride = { extraTool: tool('extraTool') };
      const toolsets = (session as any)._buildToolsets(harness.getMode('sub')!);
      expect(toolsets['subagent:tools']).toBeDefined();
      expect(Object.keys(toolsets['subagent:tools'])).toEqual(['extraTool']);
    } finally {
      await harness.shutdown();
    }
  });

  it('without a workspaceTools profile, every mode tool is exposed', async () => {
    const modes: HarnessMode[] = [
      { id: 'all', agentId: 'default', tools: { readDoc: tool('readDoc'), editDoc: tool('editDoc') } },
    ];
    const { harness } = setupHarness({ modes, defaultModeId: 'all', toolCategoryResolver: categoryResolver });
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    try {
      const toolsets = (session as any)._buildToolsets(harness.getMode('all')!);
      expect(Object.keys(toolsets['mode:all']).sort()).toEqual(['editDoc', 'readDoc']);
    } finally {
      await harness.shutdown();
    }
  });

  it('rejects non-workspace categories (bogus, mcp, other) in workspaceTools.expose at construction', () => {
    // The filter only acts on read/edit/execute, so mcp/other (and anything else)
    // must be rejected rather than silently doing nothing.
    for (const cat of ['bogus', 'mcp', 'other']) {
      expect(() =>
        setupHarness({
          modes: [{ id: 'bad', agentId: 'default', workspaceTools: { expose: [cat as any] } }],
          defaultModeId: 'bad',
        }),
      ).toThrow(/workspaceTools/);
    }
  });
});

// ---------------------------------------------------------------------------
// Inheritance footgun + grants behavior + harder malformed-rejection coverage
// (raised by the 5-angle harsh review)
// ---------------------------------------------------------------------------

describe('Mode permission seeding — inheritance + grants edge cases (§4.2e)', () => {
  it('STICKY INHERITANCE: a permissive base survives into a no-permissions mode (documented opt-in footgun)', async () => {
    const modes: HarnessMode[] = [
      {
        id: 'allowAll',
        agentId: 'default',
        permissions: { categories: { edit: 'allow', execute: 'allow' }, tools: {} },
      },
      { id: 'plain', agentId: 'default' }, // declares no permissions → inherits
    ];
    const { harness } = setupHarness({ modes, defaultModeId: 'allowAll' });
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    try {
      await session.switchMode({ mode: 'plain' });
      // The permissive base is STICKY: `plain` does not own/clear permissions, so
      // `allow` carries over. This is the intentional opt-in semantics — a mode
      // that wants a neutral base must declare `permissions: { categories: {}, tools: {} }`.
      expect(session.permissions.getRules().categories.edit).toBe('allow');
      expect(session.permissions.getRules().categories.execute).toBe('allow');
    } finally {
      await harness.shutdown();
    }
  });

  it('runtime grants PERSIST across a mode switch (only permissionRules are re-seeded)', async () => {
    const modes: HarnessMode[] = [
      { id: 'a', agentId: 'default', permissions: { categories: { edit: 'ask' }, tools: {} } },
      { id: 'b', agentId: 'default', permissions: { categories: {}, tools: {} } },
    ];
    const { harness } = setupHarness({ modes, defaultModeId: 'a' });
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    try {
      await session.permissions.grantCategory({ category: 'read' });
      expect(session.permissions.getGrants().categories).toContain('read');
      await session.switchMode({ mode: 'b' });
      // Re-seed replaces permissionRules but leaves sessionGrants intact.
      expect(session.permissions.getRules().categories.edit).toBeUndefined(); // b's base
      expect(session.permissions.getGrants().categories).toContain('read'); // grant survives
    } finally {
      await harness.shutdown();
    }
  });

  it('rejects malformed mode permissions at construction (array, bad category key, empty tool key)', () => {
    const cases: HarnessMode[][] = [
      [{ id: 'm', agentId: 'default', permissions: { categories: [] as any, tools: {} } }],
      [{ id: 'm', agentId: 'default', permissions: { categories: { bogus: 'deny' } as any, tools: {} } }],
      [{ id: 'm', agentId: 'default', permissions: { categories: {}, tools: { '': 'deny' } } }],
      [{ id: 'm', agentId: 'default', permissions: [] as any }],
    ];
    for (const modes of cases) {
      expect(() => setupHarness({ modes, defaultModeId: 'm' })).toThrow(/permissions/);
    }
  });
});
