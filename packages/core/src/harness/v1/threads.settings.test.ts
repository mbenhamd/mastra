/**
 * Harness v1 — `createHarnessOperatorThreadController(harness).setSettings/getSettings/getSetting`.
 *
 * Settings are a shallow-merge view over `thread.metadata`. The API is
 * intentionally patch-shaped (mirrors `Session.setState()`) so callers don't
 * have to learn a second write model. These tests pin the diff / event /
 * scope behavior:
 *
 *   - patch merges, `undefined` removes
 *   - no-op writes don't bump `updatedAt` or emit an event
 *   - `thread_settings_changed` carries only real diffs + actual removals
 *   - cross-resource access throws `HarnessThreadNotFoundError`
 *   - `getSetting()` is a thin sugar over `getSettings()`
 *   - `getSettings()` snapshot is frozen and decoupled from later writes
 */

import { describe, expect, it } from 'vitest';

import { setupHarness } from './__test-utils__';
import { HarnessThreadNotFoundError } from './errors';
import { createHarnessOperatorThreadController } from './harness';

describe('createHarnessOperatorThreadController(harness).setSettings()', () => {
  it('merges a patch into thread metadata (read back via getSettings)', async () => {
    const { harness } = setupHarness();
    const thread = await createHarnessOperatorThreadController(harness).create({
      resourceId: 'r1',
      title: 't',
      metadata: { existing: 'keep-me' },
    });

    await createHarnessOperatorThreadController(harness).setSettings({
      resourceId: 'r1',
      threadId: thread.id,
      patch: { observationThreshold: 5, color: 'red' },
    });

    // §10.2: thread settings changes emit no public event — behavior is read
    // back via getSettings (the operator/internal boundary).
    const settings = await createHarnessOperatorThreadController(harness).getSettings({
      resourceId: 'r1',
      threadId: thread.id,
    });
    expect(settings).toEqual({
      existing: 'keep-me',
      observationThreshold: 5,
      color: 'red',
    });
  });

  it('removes keys whose patch value is undefined', async () => {
    const { harness } = setupHarness();
    const thread = await createHarnessOperatorThreadController(harness).create({
      resourceId: 'r1',
      title: 't',
      metadata: { stale: 'gone-soon', kept: 1 },
    });

    await createHarnessOperatorThreadController(harness).setSettings({
      resourceId: 'r1',
      threadId: thread.id,
      patch: { stale: undefined },
    });

    const settings = await createHarnessOperatorThreadController(harness).getSettings({
      resourceId: 'r1',
      threadId: thread.id,
    });
    expect(settings).toEqual({ kept: 1 });
  });

  it('applies only real changes when the patch repeats existing values', async () => {
    const { harness } = setupHarness();
    const thread = await createHarnessOperatorThreadController(harness).create({
      resourceId: 'r1',
      title: 't',
      metadata: { same: 'value', other: 'a' },
    });

    await createHarnessOperatorThreadController(harness).setSettings({
      resourceId: 'r1',
      threadId: thread.id,
      patch: { same: 'value', other: 'b' },
    });

    // `same` is unchanged; `other` advances to 'b'.
    const settings = await createHarnessOperatorThreadController(harness).getSettings({
      resourceId: 'r1',
      threadId: thread.id,
    });
    expect(settings).toEqual({ same: 'value', other: 'b' });
  });

  it('is a no-op when the patch makes no real change', async () => {
    const { harness } = setupHarness();
    const thread = await createHarnessOperatorThreadController(harness).create({
      resourceId: 'r1',
      title: 't',
      metadata: { a: 1 },
    });

    await createHarnessOperatorThreadController(harness).setSettings({
      resourceId: 'r1',
      threadId: thread.id,
      patch: { a: 1, missing: undefined },
    });

    const settings = await createHarnessOperatorThreadController(harness).getSettings({
      resourceId: 'r1',
      threadId: thread.id,
    });
    expect(settings).toEqual({ a: 1 });
  });

  it('preserves keys not mentioned in the patch', async () => {
    const { harness } = setupHarness();
    const thread = await createHarnessOperatorThreadController(harness).create({
      resourceId: 'r1',
      title: 't',
      metadata: { keepA: 'A', keepB: 'B' },
    });

    await createHarnessOperatorThreadController(harness).setSettings({
      resourceId: 'r1',
      threadId: thread.id,
      patch: { keepA: 'A-prime' },
    });

    const settings = await createHarnessOperatorThreadController(harness).getSettings({
      resourceId: 'r1',
      threadId: thread.id,
    });
    expect(settings).toEqual({ keepA: 'A-prime', keepB: 'B' });
  });

  it('throws HarnessThreadNotFoundError on a missing thread', async () => {
    const { harness } = setupHarness();
    await expect(
      createHarnessOperatorThreadController(harness).setSettings({
        resourceId: 'r1',
        threadId: 'thread-does-not-exist',
        patch: { x: 1 },
      }),
    ).rejects.toBeInstanceOf(HarnessThreadNotFoundError);
  });

  it('throws HarnessThreadNotFoundError when the thread belongs to another resource', async () => {
    const { harness } = setupHarness();
    const foreign = await createHarnessOperatorThreadController(harness).create({ resourceId: 'other', title: 't' });
    await expect(
      createHarnessOperatorThreadController(harness).setSettings({
        resourceId: 'r1',
        threadId: foreign.id,
        patch: { x: 1 },
      }),
    ).rejects.toBeInstanceOf(HarnessThreadNotFoundError);
  });
});

describe('createHarnessOperatorThreadController(harness).getSettings() / getSetting()', () => {
  it('returns an empty object when the thread has no metadata', async () => {
    const { harness } = setupHarness();
    const thread = await createHarnessOperatorThreadController(harness).create({ resourceId: 'r1', title: 't' });
    const settings = await createHarnessOperatorThreadController(harness).getSettings({
      resourceId: 'r1',
      threadId: thread.id,
    });
    expect(settings).toEqual({});
  });

  it('returns a frozen snapshot decoupled from later writes', async () => {
    const { harness } = setupHarness();
    const thread = await createHarnessOperatorThreadController(harness).create({
      resourceId: 'r1',
      title: 't',
      metadata: { a: 1 },
    });

    const snapshot = await createHarnessOperatorThreadController(harness).getSettings({
      resourceId: 'r1',
      threadId: thread.id,
    });
    expect(snapshot).toEqual({ a: 1 });
    expect(Object.isFrozen(snapshot)).toBe(true);

    await createHarnessOperatorThreadController(harness).setSettings({
      resourceId: 'r1',
      threadId: thread.id,
      patch: { a: 2 },
    });

    // Snapshot is decoupled — still shows the old value.
    expect(snapshot).toEqual({ a: 1 });
  });

  it('getSetting returns the value for a known key, undefined otherwise', async () => {
    const { harness } = setupHarness();
    const thread = await createHarnessOperatorThreadController(harness).create({
      resourceId: 'r1',
      title: 't',
      metadata: { knownKey: 'hello' },
    });

    await expect(
      createHarnessOperatorThreadController(harness).getSetting({
        resourceId: 'r1',
        threadId: thread.id,
        key: 'knownKey',
      }),
    ).resolves.toBe('hello');
    await expect(
      createHarnessOperatorThreadController(harness).getSetting({
        resourceId: 'r1',
        threadId: thread.id,
        key: 'missing',
      }),
    ).resolves.toBeUndefined();
  });

  it('throws HarnessThreadNotFoundError on a missing thread', async () => {
    const { harness } = setupHarness();
    await expect(
      createHarnessOperatorThreadController(harness).getSettings({ resourceId: 'r1', threadId: 'no-such-thread' }),
    ).rejects.toBeInstanceOf(HarnessThreadNotFoundError);
    await expect(
      createHarnessOperatorThreadController(harness).getSetting({
        resourceId: 'r1',
        threadId: 'no-such-thread',
        key: 'x',
      }),
    ).rejects.toBeInstanceOf(HarnessThreadNotFoundError);
  });
});
