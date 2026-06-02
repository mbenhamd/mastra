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
        modes: [
          { id: 'bad', agentId: 'default', permissions: { categories: { edit: 'nope' as any }, tools: {} } },
        ],
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
  name === 'readDoc' ? 'read' : name === 'editDoc' ? 'edit' : name === 'runCmd' ? 'execute' : name === 'mcpThing' ? 'mcp' : null;

describe('Mode workspace tool profile (§4.2e)', () => {
  it('withholds workspace-category tools not in expose; mcp/uncategorized stay', async () => {
    const modes: HarnessMode[] = [
      {
        id: 'reader',
        agentId: 'default',
        tools: { readDoc: tool('readDoc'), editDoc: tool('editDoc'), runCmd: tool('runCmd'), mcpThing: tool('mcpThing'), misc: tool('misc') },
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

  it('rejects an unknown category in workspaceTools.expose at construction', () => {
    expect(() =>
      setupHarness({
        modes: [{ id: 'bad', agentId: 'default', workspaceTools: { expose: ['bogus' as any] } }],
        defaultModeId: 'bad',
      }),
    ).toThrow(/workspaceTools/);
  });
});
