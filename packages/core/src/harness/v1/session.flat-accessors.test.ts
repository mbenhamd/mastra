/**
 * Harness v1 — §4.2d Session flat accessors: `setThreadSetting` and `subscribeDisplayState`.
 *
 * `setThreadSetting` writes ONLY to `metadata.app[key]` with key-grammar / reserved-name / JSON
 * validation; `subscribeDisplayState` is an in-process convenience over `getDisplayState()` +
 * `subscribe()` (immediate initial emit, recompute-and-dedupe per event, windowed coalescing).
 */

import { describe, expect, it } from 'vitest';

import { setupHarness } from './__test-utils__';
import type { HarnessDisplayStateSnapshotV1 } from './display-state';
import { HarnessValidationError } from './errors';
import { createHarnessOperatorThreadController } from './harness';

describe('Session.setThreadSetting (§4.2d)', () => {
  it('writes to metadata.app[key] and merges across calls', async () => {
    const { harness } = setupHarness();
    const ops = createHarnessOperatorThreadController(harness);
    const thread = await ops.create({ resourceId: 'u1', title: 't' });
    const session = await harness.session({ resourceId: 'u1', threadId: thread.id });

    await session.setThreadSetting({ key: 'theme', value: 'dark' });
    await session.setThreadSetting({ key: 'fontSize', value: 14 });

    const settings = await ops.getSettings({ resourceId: 'u1', threadId: thread.id });
    expect(settings.app).toEqual({ theme: 'dark', fontSize: 14 });
  });

  it('rejects keys that violate the storage-safe grammar', async () => {
    const { harness } = setupHarness();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    for (const key of ['1leading', 'has-dash', 'has space', '']) {
      await expect(session.setThreadSetting({ key, value: 1 })).rejects.toThrow(HarnessValidationError);
    }
  });

  it('rejects prototype-pollution and reserved-namespace keys', async () => {
    const { harness } = setupHarness();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    for (const key of ['__proto__', 'prototype', 'constructor', '__mastraInternal', 'mastra__x']) {
      await expect(session.setThreadSetting({ key, value: 1 })).rejects.toThrow(HarnessValidationError);
    }
  });

  it('rejects non-JSON values before touching storage', async () => {
    const { harness } = setupHarness();
    const ops = createHarnessOperatorThreadController(harness);
    const thread = await ops.create({ resourceId: 'u1', title: 't' });
    const session = await harness.session({ resourceId: 'u1', threadId: thread.id });
    await expect(session.setThreadSetting({ key: 'fn', value: (() => 0) as never })).rejects.toThrow(
      HarnessValidationError,
    );
    // Nothing was written (validation rejects before the storage read-merge-write).
    const settings = await ops.getSettings({ resourceId: 'u1', threadId: thread.id });
    expect(settings.app).toBeUndefined();
  });
});

describe('Session.subscribeDisplayState (§4.2d)', () => {
  it('emits the current snapshot immediately, again on change, and stops after unsubscribe', async () => {
    const { harness } = setupHarness();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });

    const snaps: HarnessDisplayStateSnapshotV1[] = [];
    const off = session.subscribeDisplayState(s => {
      snaps.push(s);
    });

    // Immediate initial emit.
    expect(snaps).toHaveLength(1);
    expect(snaps[0]).toBeDefined();

    await session.message({ content: 'hi' });
    const afterMessage = snaps.length;
    expect(afterMessage).toBeGreaterThan(1); // the run projection changed during the turn

    off();
    await session.message({ content: 'again' });
    expect(snaps).toHaveLength(afterMessage); // no further emits after unsubscribe
  });

  it('does not emit consecutive identical snapshots', async () => {
    const { harness } = setupHarness();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });

    const serialized: string[] = [];
    const off = session.subscribeDisplayState(s => {
      serialized.push(JSON.stringify(s));
    });
    await session.message({ content: 'hi' });
    off();

    for (let i = 1; i < serialized.length; i += 1) {
      expect(serialized[i]).not.toBe(serialized[i - 1]);
    }
  });
});
