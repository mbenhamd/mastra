/**
 * Harness v1 — §4.2e permission GATE enforced through the REAL agent loop.
 *
 * The session resolves a per-tool policy (tool rule > category rule > default;
 * grants turn `ask`→`allow`) and threads it onto the request context; the loop's
 * tool-call step consults it: `deny` blocks the call with a non-aborting result,
 * `ask` forces approval (suspend), `allow` runs the tool. Enforcement is OPT-IN —
 * a harness with no configured policy gates nothing (today's behavior).
 *
 * Only the language model is mocked; the Agent + tools + loop are real.
 */

import { describe, expect, it, vi } from 'vitest';
import { z } from 'zod';

import { Agent } from '../../agent';
import { convertArrayToReadableStream, MockLanguageModelV2 } from '../../agent/__tests__/mock-model';
import { InMemoryStore } from '../../storage';
import { createTool } from '../../tools';

import type { HarnessEvent } from './events';
import { Harness } from './harness';
import type { HarnessMode, PermissionPolicy } from './types';

const testUsage = { inputTokens: 10, outputTokens: 20, totalTokens: 30 };

function textStream(deltas: string[]) {
  return convertArrayToReadableStream([
    { type: 'stream-start', warnings: [] },
    { type: 'response-metadata', id: 'id-text', modelId: 'mock-model-id', timestamp: new Date(0) },
    { type: 'text-start', id: 'text-1' },
    ...deltas.map(delta => ({ type: 'text-delta', id: 'text-1', delta })),
    { type: 'text-end', id: 'text-1' },
    { type: 'finish', finishReason: 'stop', usage: testUsage },
  ]);
}

function toolCallStream(toolCallId: string, toolName: string, inputJson: string) {
  return convertArrayToReadableStream([
    { type: 'stream-start', warnings: [] },
    { type: 'response-metadata', id: `id-${toolCallId}`, modelId: 'mock-model-id', timestamp: new Date(0) },
    { type: 'tool-call', toolCallId, toolName, input: inputJson, providerExecuted: false },
    { type: 'finish', finishReason: 'tool-calls', usage: testUsage },
  ]);
}

/**
 * Build a real harness whose agent calls `writeDoc` (an `edit`-category tool that
 * records when it runs) then replies. `permissions` / `defaultPermissionPolicy`
 * configure the gate.
 */
function buildHarness(opts: { permissions?: HarnessMode['permissions']; defaultPermissionPolicy?: PermissionPolicy }) {
  const ran = { writeDoc: false };
  const writeDoc = createTool({
    id: 'writeDoc',
    description: 'edit a doc',
    inputSchema: z.object({ text: z.string() }),
    execute: async input => {
      ran.writeDoc = true;
      return { wrote: (input as { text: string }).text };
    },
  });

  let call = 0;
  const model = new MockLanguageModelV2({
    doStream: async () => {
      call++;
      if (call === 1) {
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: toolCallStream('call-1', 'writeDoc', JSON.stringify({ text: 'hello' })),
        };
      }
      return { rawCall: { rawPrompt: null, rawSettings: {} }, warnings: [], stream: textStream(['done']) };
    },
  });

  const agent = new Agent({ id: 'default', name: 'default', instructions: 'use writeDoc', model, tools: { writeDoc } });
  const mode: HarnessMode = {
    id: 'default',
    agentId: 'default',
    ...(opts.permissions ? { permissions: opts.permissions } : {}),
  };
  const harness = new Harness({
    agents: { default: agent } as any,
    storage: new InMemoryStore(),
    modes: [mode],
    defaultModeId: 'default',
    // writeDoc resolves to the 'edit' workspace category.
    toolCategoryResolver: (name: string) => (name === 'writeDoc' ? 'edit' : null),
    ...(opts.defaultPermissionPolicy ? { defaultPermissionPolicy: opts.defaultPermissionPolicy } : {}),
  });
  return { harness, ran };
}

describe('Harness v1 §4.2e permission gate — enforced through the real loop', () => {
  it('DENY blocks the tool (removed pre-exposure); it never runs and the turn completes', async () => {
    const { harness, ran } = buildHarness({ permissions: { categories: { edit: 'deny' }, tools: {} } });
    try {
      const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      const result = (await session.message({ content: 'write it' })) as any;
      // The denied tool never executes (removed at the pre-exposure gate; the
      // pre-action gate is the safety net for tools that slip through — see the
      // resume test below). The turn completes normally, not suspended.
      expect(ran.writeDoc).toBe(false);
      expect(result.finishReason).not.toBe('suspended');
      expect(result.text).toContain('done');
    } finally {
      await harness.shutdown();
    }
  });

  it('§O4: a DENY surfaces a pre-exposure tool_denied (the tool is removed from the model surface)', async () => {
    const { harness } = buildHarness({ permissions: { categories: { edit: 'deny' }, tools: {} } });
    try {
      const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(e => events.push(e));
      await session.message({ content: 'write it' });

      const denied = events.filter(e => e.type === 'tool_denied') as Array<{ toolName: string; stage: string }>;
      expect(denied.some(e => e.stage === 'pre-exposure' && e.toolName === 'writeDoc')).toBe(true);
    } finally {
      await harness.shutdown();
    }
  });

  it('§O4: an UNCONFIGURED gate emits no tool_denied (allowed tool runs)', async () => {
    const { harness, ran } = buildHarness({});
    try {
      const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(e => events.push(e));
      await session.message({ content: 'write it' });
      expect(ran.writeDoc).toBe(true);
      expect(events.some(e => e.type === 'tool_denied')).toBe(false);
    } finally {
      await harness.shutdown();
    }
  });

  it('ALLOW runs the tool', async () => {
    const { harness, ran } = buildHarness({ permissions: { categories: { edit: 'allow' }, tools: {} } });
    try {
      const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(e => events.push(e));
      const result = (await session.message({ content: 'write it' })) as any;
      expect(ran.writeDoc).toBe(true);
      const toolEnd = events.find(e => e.type === 'tool_end' && (e as any).toolName === 'writeDoc') as any;
      expect(toolEnd.output.wrote).toBe('hello');
      expect(result.text).toContain('done');
    } finally {
      await harness.shutdown();
    }
  });

  it('ASK suspends for approval; the tool does not run until approved', async () => {
    const { harness, ran } = buildHarness({ permissions: { categories: { edit: 'ask' }, tools: {} } });
    try {
      const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      const result = (await session.message({ content: 'write it' })) as any;
      // Policy 'ask' → the turn suspends for approval; the tool has not run.
      expect(result.finishReason).toBe('suspended');
      expect(ran.writeDoc).toBe(false);
      const pending = session.getRecord().pendingResume;
      expect(pending?.kind).toBe('tool-approval');
    } finally {
      await harness.shutdown();
    }
  });

  it('a per-tool ALLOW rule overrides a category DENY', async () => {
    const { harness, ran } = buildHarness({
      permissions: { categories: { edit: 'deny' }, tools: { writeDoc: 'allow' } },
    });
    try {
      const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      await session.message({ content: 'write it' });
      expect(ran.writeDoc).toBe(true); // tool rule wins over category rule
    } finally {
      await harness.shutdown();
    }
  });

  it('a session GRANT turns an ask into allow', async () => {
    const { harness, ran } = buildHarness({ permissions: { categories: { edit: 'ask' }, tools: {} } });
    try {
      const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      await session.permissions.grantCategory({ category: 'edit' });
      await session.message({ content: 'write it' });
      expect(ran.writeDoc).toBe(true); // grant suppressed the policy-level ask
    } finally {
      await harness.shutdown();
    }
  });

  it('the gate RE-EVALUATES on resume: a deny set after an ask blocks the approved tool', async () => {
    // §4.2e pre-action gate must run before RESUME, not just the initial turn:
    // the resumed turn carries the rebuilt request context + resolver.
    const { harness, ran } = buildHarness({ permissions: { categories: { edit: 'ask' }, tools: {} } });
    try {
      const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(e => events.push(e));
      const suspended = (await session.message({ content: 'write it' })) as any;
      expect(suspended.finishReason).toBe('suspended');
      expect(ran.writeDoc).toBe(false);

      // Flip the policy to deny WHILE the call is parked behind approval.
      await session.permissions.setPolicy({ category: 'edit', policy: 'deny' });
      // Approve the original ask — but the resume must re-evaluate and now DENY.
      await session.respondToToolApproval({ approved: true });

      // Re-evaluated on resume → blocked; the tool still never ran.
      expect(ran.writeDoc).toBe(false);
      // §O4: the approved tool reached ACTION time and was refused there → a
      // tool_denied(stage:'action') surfaces the rationale (it slipped past the
      // pre-exposure gate because the surface was built while policy was 'ask').
      expect(
        (events.filter(e => e.type === 'tool_denied') as Array<{ toolName: string; stage: string }>).some(
          e => e.stage === 'action' && e.toolName === 'writeDoc',
        ),
      ).toBe(true);
    } finally {
      await harness.shutdown();
    }
  });

  it('harness BUILT-INS (plan-task tools) bypass the gate even under defaultPermissionPolicy deny', async () => {
    // An explicit default 'deny' engages the gate and would block any non-builtin
    // tool. The harness's own built-ins (task_add etc.) must still run — else the
    // gate would break the harness's own orchestration.
    let call = 0;
    const model = new MockLanguageModelV2({
      doStream: async () => {
        call++;
        if (call === 1) {
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: toolCallStream('b-add', 'task_add', JSON.stringify({ content: 'plan step' })),
          };
        }
        return { rawCall: { rawPrompt: null, rawSettings: {} }, warnings: [], stream: textStream(['done']) };
      },
    });
    const agent = new Agent({ id: 'default', name: 'default', instructions: 'plan', model });
    const harness = new Harness({
      agents: { default: agent } as any,
      storage: new InMemoryStore(),
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      defaultPermissionPolicy: 'deny', // gate engaged; would deny any non-builtin tool
    });
    try {
      const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(e => events.push(e));
      await session.message({ content: 'plan it' });
      // The built-in task_add ran (not denied) → a real plan task was created.
      const toolEnd = events.find(e => e.type === 'tool_end' && (e as any).toolName === 'task_add') as any;
      expect(toolEnd).toBeDefined();
      expect(toolEnd.isError).toBe(false);
      expect(toolEnd.output?.taskId).toBeTruthy();
      expect(session.getDisplayState().planTasks?.total).toBe(1);
    } finally {
      await harness.shutdown();
    }
  });

  it('PRE-EXPOSURE: a denied tool is not even shown to the model (allowed tools still are)', async () => {
    const readTool = createTool({
      id: 'readDoc',
      description: 'read',
      inputSchema: z.object({}),
      execute: async () => ({}),
    });
    const writeTool = createTool({
      id: 'writeDoc',
      description: 'edit',
      inputSchema: z.object({}),
      execute: async () => ({}),
    });
    let seenToolsJson = '';
    const model = new MockLanguageModelV2({
      doStream: async (options: any) => {
        seenToolsJson = JSON.stringify(options?.tools ?? []);
        return { rawCall: { rawPrompt: null, rawSettings: {} }, warnings: [], stream: textStream(['ok']) };
      },
    });
    const agent = new Agent({
      id: 'default',
      name: 'default',
      instructions: 'x',
      model,
      tools: { readDoc: readTool, writeDoc: writeTool },
    });
    const harness = new Harness({
      agents: { default: agent } as any,
      storage: new InMemoryStore(),
      modes: [{ id: 'default', agentId: 'default', permissions: { categories: { edit: 'deny' }, tools: {} } }],
      defaultModeId: 'default',
      toolCategoryResolver: (n: string) => (n === 'writeDoc' ? 'edit' : n === 'readDoc' ? 'read' : null),
    });
    try {
      const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      await session.message({ content: 'go' });
      expect(seenToolsJson).toContain('readDoc'); // allowed tool exposed to the model
      expect(seenToolsJson).not.toContain('writeDoc'); // denied tool hidden BEFORE the model call
    } finally {
      await harness.shutdown();
    }
  });

  it('YOLO (queued turn) auto-grants a policy ASK — the tool runs without suspending', async () => {
    // A queued turn carries `yolo`. With policy `ask`, the gate would normally
    // suspend; `yolo` auto-grants so the tool runs and the turn completes.
    const { harness, ran } = buildHarness({ permissions: { categories: { edit: 'ask' }, tools: {} } });
    try {
      const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      const result = (await session.queue({ content: 'write it', yolo: true })) as any;
      expect(ran.writeDoc).toBe(true);
      expect(result.finishReason).not.toBe('suspended');
      expect(result.text).toContain('done');
    } finally {
      await harness.shutdown();
    }
  });

  it('YOLO never bypasses a DENY — the denied tool still never runs', async () => {
    // `yolo` is honored only AFTER the deny short-circuit, and `deny` also removes
    // the tool pre-exposure. A yolo'd queued turn cannot run a denied tool.
    const { harness, ran } = buildHarness({ permissions: { categories: { edit: 'deny' }, tools: {} } });
    try {
      const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      const result = (await session.queue({ content: 'write it', yolo: true })) as any;
      expect(ran.writeDoc).toBe(false);
      expect(result.finishReason).not.toBe('suspended');
    } finally {
      await harness.shutdown();
    }
  });

  it('YOLO does NOT suppress a TOOL-OWNED approval — the turn still suspends', async () => {
    // Per spec, yolo clears ONLY the policy-level ask; a tool's own `requireApproval`
    // is independent of the §4.2e gate and survives yolo, so the turn still suspends.
    const ran = { danger: false };
    const danger = createTool({
      id: 'danger',
      description: 'dangerous',
      inputSchema: z.object({}),
      requireApproval: true,
      execute: async () => {
        ran.danger = true;
        return { ok: true };
      },
    });
    let call = 0;
    const model = new MockLanguageModelV2({
      doStream: async () => {
        call++;
        if (call === 1) {
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: toolCallStream('d-1', 'danger', '{}'),
          };
        }
        return { rawCall: { rawPrompt: null, rawSettings: {} }, warnings: [], stream: textStream(['done']) };
      },
    });
    const agent = new Agent({ id: 'default', name: 'default', instructions: 'x', model, tools: { danger } });
    const harness = new Harness({
      agents: { default: agent } as any,
      storage: new InMemoryStore(),
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      // No permissions, no defaultPermissionPolicy → the §4.2e gate is OFF.
    });
    try {
      const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      // The turn suspends for the tool-owned approval, so the queue promise does not
      // resolve — observe the durable pending state instead of awaiting it.
      void session.queue({ content: 'go', yolo: true });
      await vi.waitFor(() => {
        expect(session.getRecord().pendingResume?.kind).toBe('tool-approval');
      });
      expect(ran.danger).toBe(false); // tool-owned approval NOT auto-granted by yolo
    } finally {
      await harness.shutdown();
    }
  });

  it('UNCONFIGURED harness gates nothing — the tool runs (pre-§4.2e behavior preserved)', async () => {
    // No mode.permissions and no explicit defaultPermissionPolicy → gate OFF.
    const { harness, ran } = buildHarness({});
    try {
      const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      const result = (await session.message({ content: 'write it' })) as any;
      expect(ran.writeDoc).toBe(true);
      expect(result.finishReason).not.toBe('suspended');
    } finally {
      await harness.shutdown();
    }
  });
});
