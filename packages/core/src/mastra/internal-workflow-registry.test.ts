import { describe, expect, it, vi } from 'vitest';
import { z } from 'zod/v4';
import { createWorkflow } from '../workflows/create';
import { createStep } from '../workflows/workflow';
import { Mastra } from './index';

/**
 * Tests for the run-scoped internal workflow registry.
 *
 * The registry supports two kinds of entries:
 * 1. Unscoped (singleton): keyed by `${id}` — used for background tasks,
 *    score-traces, etc.  These live forever.
 * 2. Run-scoped: stored as structured workflow-id/run-id maps and removed by
 *    the execution lifecycle. Suspended runs remain registered until resume.
 */

const dummyStep = createStep({
  id: 'noop',
  inputSchema: z.object({}),
  outputSchema: z.object({}),
  execute: async () => ({}),
});

function makeWorkflow(id: string) {
  return createWorkflow({ id, inputSchema: z.object({}), outputSchema: z.object({}) })
    .then(dummyStep)
    .commit();
}

function makeMastra() {
  return new Mastra({ logger: false });
}

describe('internal workflow registry', () => {
  describe('unscoped (singleton) registration', () => {
    it('registers and retrieves a workflow by id', () => {
      const m = makeMastra();
      const wf = makeWorkflow('bg-task');
      m.__registerInternalWorkflow(wf);

      expect(m.__hasInternalWorkflow('bg-task')).toBe(true);
      expect(m.__getInternalWorkflow('bg-task')).toBe(wf);
    });

    it('throws when retrieving an unregistered id', () => {
      const m = makeMastra();
      expect(() => m.__getInternalWorkflow('missing')).toThrow(/not found/i);
    });

    it('returns false for __hasInternalWorkflow on missing id', () => {
      const m = makeMastra();
      expect(m.__hasInternalWorkflow('missing')).toBe(false);
    });

    it('does not treat Object prototype keys as registered workflows', () => {
      const m = makeMastra();
      expect(m.__hasInternalWorkflow('toString')).toBe(false);
      expect(m.__hasInternalWorkflow('constructor')).toBe(false);
      expect(() => m.__getInternalWorkflow('toString')).toThrow(/not found/i);
    });
  });

  describe('run-scoped registration', () => {
    it('registers and retrieves a workflow by id+runId', () => {
      const m = makeMastra();
      const wf = makeWorkflow('agentic-loop');
      m.__registerInternalWorkflow(wf, 'run-1');

      expect(m.__hasInternalWorkflow('agentic-loop', 'run-1')).toBe(true);
      expect(m.__getInternalWorkflow('agentic-loop', 'run-1')).toBe(wf);
    });

    it('does not collide with another runId for the same workflow id', () => {
      const m = makeMastra();
      const wf1 = makeWorkflow('agentic-loop');
      const wf2 = makeWorkflow('agentic-loop');
      m.__registerInternalWorkflow(wf1, 'run-1');
      m.__registerInternalWorkflow(wf2, 'run-2');

      expect(m.__getInternalWorkflow('agentic-loop', 'run-1')).toBe(wf1);
      expect(m.__getInternalWorkflow('agentic-loop', 'run-2')).toBe(wf2);
    });

    it('does not alias delimiter-bearing workflow and run ids', () => {
      const m = makeMastra();
      const first = makeWorkflow('a:b');
      const second = makeWorkflow('a');
      m.__registerInternalWorkflow(first, 'c');
      m.__registerInternalWorkflow(second, 'b:c');

      expect(m.__getInternalWorkflow('a:b', 'c')).toBe(first);
      expect(m.__getInternalWorkflow('a', 'b:c')).toBe(second);
    });

    it('falls back to unscoped entry when run-scoped is missing', () => {
      const m = makeMastra();
      const singleton = makeWorkflow('shared-wf');
      m.__registerInternalWorkflow(singleton); // unscoped

      // Lookup with a runId that was never registered should fall back
      expect(m.__hasInternalWorkflow('shared-wf', 'any-run')).toBe(true);
      expect(m.__getInternalWorkflow('shared-wf', 'any-run')).toBe(singleton);
    });

    it('prefers run-scoped entry over unscoped when both exist', () => {
      const m = makeMastra();
      const singleton = makeWorkflow('wf');
      const scoped = makeWorkflow('wf');
      m.__registerInternalWorkflow(singleton);
      m.__registerInternalWorkflow(scoped, 'run-1');

      expect(m.__getInternalWorkflow('wf', 'run-1')).toBe(scoped);
      // Unscoped lookup still returns the singleton
      expect(m.__getInternalWorkflow('wf')).toBe(singleton);
    });

    it('unregisters only the run-scoped entry, leaving unscoped intact', () => {
      const m = makeMastra();
      const singleton = makeWorkflow('wf');
      const scoped = makeWorkflow('wf');
      m.__registerInternalWorkflow(singleton);
      m.__registerInternalWorkflow(scoped, 'run-1');

      m.__unregisterInternalWorkflow('wf', 'run-1');

      // Run-scoped gone — falls back to singleton
      expect(m.__getInternalWorkflow('wf', 'run-1')).toBe(singleton);
      expect(m.__getInternalWorkflow('wf')).toBe(singleton);
    });

    it('unregister on missing key is a no-op', () => {
      const m = makeMastra();
      expect(() => m.__unregisterInternalWorkflow('nope', 'run-1')).not.toThrow();
    });
  });

  describe('lifecycle cleanup', () => {
    it('keeps a live run registered until explicit terminal cleanup', () => {
      const m = makeMastra();
      const live = makeWorkflow('loop');
      m.__registerInternalWorkflow(live, 'suspended-run');
      m.__registerInternalWorkflow(makeWorkflow('unrelated'), 'other-run');

      expect(m.__getInternalWorkflow('loop', 'suspended-run')).toBe(live);

      m.__unregisterInternalWorkflow('loop', 'suspended-run');
      expect(m.__hasInternalWorkflow('loop', 'suspended-run')).toBe(false);
    });
  });

  describe('TTL compatibility', () => {
    it('retains the documented legacy TTL reference', () => {
      expect(Mastra.INTERNAL_WORKFLOW_TTL_MS).toBe(30 * 60 * 1000);
    });

    it('does not infer abandonment from registration age', () => {
      vi.useFakeTimers();
      try {
        const m = makeMastra();
        const longRunning = makeWorkflow('long-running-loop');
        const fresh = makeWorkflow('fresh-loop');
        m.__registerInternalWorkflow(longRunning, 'long-running-run');

        vi.advanceTimersByTime(Mastra.INTERNAL_WORKFLOW_TTL_MS + 1);
        m.__registerInternalWorkflow(fresh, 'fresh-run');

        expect(m.__getInternalWorkflow('long-running-loop', 'long-running-run')).toBe(longRunning);
        expect(m.__getInternalWorkflow('fresh-loop', 'fresh-run')).toBe(fresh);
      } finally {
        vi.useRealTimers();
      }
    });
  });
});
