/**
 * §5.1b.4 / §5.6 / §10.6 — `Session.getActivityTimeline()` end-to-end over the
 * REAL gather path (memory thread log + record goal/pending), proving the stub is
 * replaced. Entry-building/ordering/cursor detail is covered by the pure
 * activity-timeline.test.ts; here we verify the session wiring.
 */

import { describe, expect, it, vi } from 'vitest';

import type { MastraDBMessage } from '../../agent/types';

import { setupHarness } from './__test-utils__';

async function seed(harness: ReturnType<typeof setupHarness>['harness'], messages: MastraDBMessage[]) {
  const memory = await harness._internalTryGetMemoryStorage();
  if (!memory) throw new Error('test setup expected memory storage');
  await memory.saveMessages({ messages });
}

function userMessage(id: string, threadId: string, resourceId: string, text: string, createdAt: Date): MastraDBMessage {
  return {
    id,
    role: 'user',
    threadId,
    resourceId,
    createdAt,
    content: { format: 2, parts: [{ type: 'text', text }] },
  } as MastraDBMessage;
}

describe('Session.getActivityTimeline() — session wiring (§5.1b.4)', () => {
  it('projects the durable thread log into message entries (stub is gone)', async () => {
    const { harness } = setupHarness();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    try {
      await seed(harness, [
        userMessage('m1', session.threadId, 'u1', 'first', new Date(1000)),
        userMessage('m2', session.threadId, 'u1', 'second', new Date(2000)),
      ]);

      const tl = await session.getActivityTimeline();
      expect(tl.sessionId).toBe(session.id);
      expect(tl.threadId).toBe(session.threadId);
      expect(tl.includeDescendants).toBe(false);
      const summaries = tl.entries.filter(e => e.kind === 'message').map(e => e.summary);
      expect(summaries).toContain('first');
      expect(summaries).toContain('second');
      // Ordered ascending by occurredAt.
      const times = tl.entries.map(e => e.occurredAt);
      expect(times).toEqual([...times].sort((a, b) => a - b));
    } finally {
      await harness.shutdown();
    }
  });

  it('includes a goal entry from the session record', async () => {
    const { harness } = setupHarness();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    try {
      await session.setGoal({ objective: 'finish the audit', judgeModel: 'openai/gpt-4o-mini' });
      const tl = await session.getActivityTimeline();
      const goal = tl.entries.find(e => e.kind === 'goal');
      expect(goal).toBeDefined();
      expect(goal!.summary).toBe('finish the audit');
      expect(goal!.actor?.kind).toBe('goal');
    } finally {
      await harness.shutdown();
    }
  });

  it('paginates with a forward cursor over the real log', async () => {
    const { harness } = setupHarness();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    try {
      await seed(harness, [
        userMessage('m1', session.threadId, 'u1', 'a', new Date(1000)),
        userMessage('m2', session.threadId, 'u1', 'b', new Date(2000)),
        userMessage('m3', session.threadId, 'u1', 'c', new Date(3000)),
      ]);
      const first = await session.getActivityTimeline({ limit: 2 });
      expect(first.entries).toHaveLength(2);
      expect(first.truncated).toBe(true);
      const second = await session.getActivityTimeline({ cursor: first.nextCursor, limit: 2 });
      expect(second.entries).toHaveLength(1);
      expect(second.truncated).toBe(false);
    } finally {
      await harness.shutdown();
    }
  });

  it('reads at most `limit + 1` messages per thread instead of the whole log (P6.3), and still paginates correctly', async () => {
    const { harness } = setupHarness();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    try {
      const memory = await harness._internalTryGetMemoryStorage();
      if (!memory) throw new Error('test setup expected memory storage');
      // 50 messages — far more than the page limit below.
      await seed(
        harness,
        Array.from({ length: 50 }, (_, i) =>
          userMessage(`m${i}`, session.threadId, 'u1', `msg-${i}`, new Date(1000 + i)),
        ),
      );

      const listSpy = vi.spyOn(memory, 'listMessages');
      const first = await session.getActivityTimeline({ limit: 5 });

      // The bounded read: perPage is `limit + 1` (NOT `false`), ordered ascending.
      expect(listSpy).toHaveBeenCalled();
      const args = listSpy.mock.calls[0]![0] as {
        perPage?: number | false;
        orderBy?: { field?: string; direction?: string };
        filter?: unknown;
      };
      expect(args.perPage).toBe(6);
      expect(args.orderBy).toEqual({ field: 'createdAt', direction: 'ASC' });
      expect(args.filter).toBeUndefined(); // no cursor on the first page

      // Output is still correct: first page is the 5 oldest, truncated, with a cursor.
      expect(first.entries).toHaveLength(5);
      expect(first.entries.map(e => e.summary)).toEqual(['msg-0', 'msg-1', 'msg-2', 'msg-3', 'msg-4']);
      expect(first.truncated).toBe(true);

      // The next page reads with an inclusive dateRange lower bound from the cursor.
      listSpy.mockClear();
      const second = await session.getActivityTimeline({ cursor: first.nextCursor, limit: 5 });
      const args2 = listSpy.mock.calls[0]![0] as { perPage?: number | false; filter?: { dateRange?: { start?: Date } } };
      expect(args2.perPage).toBe(6);
      expect(args2.filter?.dateRange?.start).toBeInstanceOf(Date);
      expect(second.entries.map(e => e.summary)).toEqual(['msg-5', 'msg-6', 'msg-7', 'msg-8', 'msg-9']);
    } finally {
      await harness.shutdown();
    }
  });
});
