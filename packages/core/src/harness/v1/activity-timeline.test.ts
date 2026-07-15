/**
 * §5.1b.4 / §5.6 / §10.6 — pure activity-timeline assembler tests. No storage:
 * the builder is fed normalized source DTOs and exercised for entry building,
 * ordering, redaction, cursor pagination, scope validation, and includeDescendants.
 */

import { describe, expect, it } from 'vitest';

import type { AgentControllerMessage as HarnessMessage } from '../../agent-controller/types';

import { ACTIVITY_TIMELINE_DEFAULT_LIMIT, buildActivityTimeline } from './activity-timeline';
import type { ActivityTimelineSessionInput, BuildActivityTimelineInput } from './activity-timeline';

function msg(id: string, role: HarnessMessage['role'], at: number, content: HarnessMessage['content']): HarnessMessage {
  return { id, role, content, createdAt: new Date(at) };
}

function addressed(over: Partial<ActivityTimelineSessionInput> = {}): ActivityTimelineSessionInput {
  return { sessionId: 's1', threadId: 't1', depth: 0, messages: [], ...over };
}

function input(sessions: ActivityTimelineSessionInput[], generatedAt = 1000): BuildActivityTimelineInput {
  return { addressedSessionId: 's1', addressedThreadId: 't1', generatedAt, sessions };
}

describe('buildActivityTimeline — entry building', () => {
  it('builds message + tool-call + tool-result entries from the thread log', () => {
    const messages: HarnessMessage[] = [
      msg('m1', 'user', 100, [{ type: 'text', text: 'hello world' }]),
      msg('m2', 'assistant', 200, [
        { type: 'text', text: 'on it' },
        { type: 'tool_call', id: 'tc1', name: 'writeDoc', args: { secret: 'top' } },
      ]),
      msg('m3', 'assistant', 300, [
        { type: 'tool_result', id: 'tc1', name: 'writeDoc', result: { token: 'sk-123' }, isError: false },
      ]),
    ];
    const tl = buildActivityTimeline(input([addressed({ messages })]));
    const kinds = tl.entries.map(e => e.kind);
    // Ordering is (occurredAt, sessionId, entryId): at the shared occurredAt=200 of m2,
    // entryId 'message-tool-call:…' sorts before 'message:…' ('-' < ':'), per the spec's
    // lexicographic entryId tiebreak.
    expect(kinds).toEqual(['message', 'message-tool-call', 'message', 'message-tool-result']);
    const call = tl.entries.find(e => e.kind === 'message-tool-call')!;
    expect(call.title).toBe('Tool call: writeDoc');
    expect(call.toolCallId).toBe('tc1');
    // Redaction: raw args / results never surface anywhere in the entries.
    const serialized = JSON.stringify(tl.entries);
    expect(serialized).not.toContain('top');
    expect(serialized).not.toContain('sk-123');
    const result = tl.entries.find(e => e.kind === 'message-tool-result')!;
    expect(result.summary).toBe('ok');
  });

  it('omits the message-level entry for a pure tool-call turn (no text/thinking)', () => {
    const messages: HarnessMessage[] = [
      msg('m1', 'assistant', 100, [{ type: 'tool_call', id: 'tc1', name: 'x', args: {} }]),
    ];
    const tl = buildActivityTimeline(input([addressed({ messages })]));
    expect(tl.entries.map(e => e.kind)).toEqual(['message-tool-call']);
  });

  it('truncates long text summaries and never leaks the full body', () => {
    const long = 'a'.repeat(1000);
    const tl = buildActivityTimeline(
      input([addressed({ messages: [msg('m1', 'user', 1, [{ type: 'text', text: long }])] })]),
    );
    const entry = tl.entries[0]!;
    expect(entry.summary!.length).toBeLessThan(long.length);
    expect(entry.summary!.endsWith('…')).toBe(true);
  });

  it('builds a goal entry and a pending-inbox entry', () => {
    const tl = buildActivityTimeline(
      input([
        addressed({
          goal: {
            id: 'g1',
            objective: 'ship it',
            status: 'active',
            createdAt: 50,
            lastDecision: { decision: 'continue', reason: 'not done', judgedAt: 150 },
          },
          pendingInbox: [{ itemId: 'p1', kind: 'tool-approval', requestedAt: 120, toolName: 'rm', runId: 'r1' }],
        }),
      ]),
    );
    const goal = tl.entries.find(e => e.kind === 'goal')!;
    expect(goal.occurredAt).toBe(150); // lastDecision.judgedAt wins over createdAt
    expect(goal.title).toBe('Goal: active');
    expect((goal.payload as any).decision).toBe('continue');
    const pending = tl.entries.find(e => e.kind === 'pending-inbox')!;
    expect(pending.occurredAt).toBe(120);
    expect(pending.runId).toBe('r1');
  });
});

describe('buildActivityTimeline — ordering + pagination', () => {
  const messages: HarnessMessage[] = [
    msg('m1', 'user', 100, [{ type: 'text', text: 'a' }]),
    msg('m2', 'assistant', 200, [{ type: 'text', text: 'b' }]),
    msg('m3', 'user', 300, [{ type: 'text', text: 'c' }]),
  ];

  it('sorts by (occurredAt, sessionId, entryId) ascending', () => {
    const tl = buildActivityTimeline(input([addressed({ messages })]));
    const times = tl.entries.map(e => e.occurredAt);
    expect(times).toEqual([...times].sort((a, b) => a - b));
  });

  it('paginates with a forward-seek cursor and reports truncated', () => {
    const first = buildActivityTimeline(input([addressed({ messages })]), { limit: 2 });
    expect(first.entries).toHaveLength(2);
    expect(first.truncated).toBe(true);
    expect(first.nextCursor).toBeTruthy();

    const second = buildActivityTimeline(input([addressed({ messages })]), { cursor: first.nextCursor, limit: 2 });
    expect(second.entries).toHaveLength(1);
    expect(second.truncated).toBe(false);
    expect(second.nextCursor).toBeUndefined();
    // No overlap between pages.
    const firstIds = new Set(first.entries.map(e => e.entryId));
    expect(second.entries.every(e => !firstIds.has(e.entryId))).toBe(true);
  });

  it('defaults the page size and caps it', () => {
    const tl = buildActivityTimeline(input([addressed({ messages })]));
    expect(tl.entries.length).toBeLessThanOrEqual(ACTIVITY_TIMELINE_DEFAULT_LIMIT);
  });
});

describe('buildActivityTimeline — cursor validation (before any scan)', () => {
  it('rejects a malformed cursor', () => {
    expect(() => buildActivityTimeline(input([addressed()]), { cursor: 'not-base64-json!!' })).toThrow(/cursor/);
  });

  it('rejects a cursor issued for a different session', () => {
    const other = buildActivityTimeline(
      {
        addressedSessionId: 'OTHER',
        addressedThreadId: 't1',
        generatedAt: 1,
        sessions: [
          {
            sessionId: 'OTHER',
            threadId: 't1',
            depth: 0,
            messages: [
              msg('m1', 'user', 1, [{ type: 'text', text: 'x' }]),
              msg('m2', 'user', 2, [{ type: 'text', text: 'y' }]),
            ],
          },
        ],
      },
      { limit: 1 },
    );
    expect(() => buildActivityTimeline(input([addressed()]), { cursor: other.nextCursor })).toThrow(
      /different session/,
    );
  });

  it('rejects a cursor issued for a different includeDescendants scope', () => {
    const messages = [
      msg('m1', 'user', 1, [{ type: 'text', text: 'x' }]),
      msg('m2', 'user', 2, [{ type: 'text', text: 'y' }]),
    ];
    const withDesc = buildActivityTimeline(input([addressed({ messages })]), { limit: 1, includeDescendants: true });
    expect(() =>
      buildActivityTimeline(input([addressed({ messages })]), {
        cursor: withDesc.nextCursor,
        includeDescendants: false,
      }),
    ).toThrow(/includeDescendants/);
  });

  it('rejects a non-positive-integer limit', () => {
    expect(() => buildActivityTimeline(input([addressed()]), { limit: 0 })).toThrow(/limit/);
    expect(() => buildActivityTimeline(input([addressed()]), { limit: 1.5 })).toThrow(/limit/);
  });
});

describe('buildActivityTimeline — includeDescendants', () => {
  const parent = addressed({ messages: [msg('pm', 'user', 100, [{ type: 'text', text: 'parent' }])] });
  const child: ActivityTimelineSessionInput = {
    sessionId: 'c1',
    threadId: 'tc1',
    parentSessionId: 's1',
    depth: 1,
    messages: [msg('cm', 'assistant', 150, [{ type: 'text', text: 'child' }])],
    subagent: { parentSessionId: 's1', childSessionId: 'c1', createdAt: 140 },
  };

  it('omits descendant + subagent entries when includeDescendants is false', () => {
    const tl = buildActivityTimeline(input([parent, child]));
    expect(tl.entries.every(e => e.sessionId === 's1')).toBe(true);
    expect(tl.entries.some(e => e.kind === 'subagent')).toBe(false);
  });

  it('includes a subagent entry + descendant messages when includeDescendants is true', () => {
    const tl = buildActivityTimeline(input([parent, child]), { includeDescendants: true });
    const sub = tl.entries.find(e => e.kind === 'subagent')!;
    expect(sub.subagentSessionId).toBe('c1');
    expect(sub.sessionId).toBe('s1'); // attributed to the parent
    const childMsg = tl.entries.find(e => e.sessionId === 'c1' && e.kind === 'message')!;
    expect(childMsg.summary).toBe('child');
    expect(childMsg.depth).toBe(1);
  });
});
