import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { MockLanguageModelV1 } from '@internal/ai-sdk-v4/test';
import { MockLanguageModelV2, convertArrayToReadableStream } from '@internal/ai-sdk-v5/test';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { z } from 'zod/v4';

import { EventEmitterPubSub } from '../../events/event-emitter';
import { PubSub } from '../../events/pubsub';
import type { EventCallback, SubscribeOptions } from '../../events/types';
import { UnixSocketPubSub } from '../../events/unix-socket-pubsub';
import { buildFakeOutput } from '../../harness/v1/__test-utils__/fake-output';
import { Mastra } from '../../mastra';
import { MockMemory } from '../../memory/mock';
import { dispatchDueNotifications } from '../../notifications/dispatcher';
import { InMemoryNotificationsStorage } from '../../notifications/storage';
import { createNotificationInboxTool } from '../../notifications/tool';
import { MASTRA_RESOURCE_ID_KEY, MASTRA_THREAD_ID_KEY, RequestContext } from '../../request-context';
import { MastraCompositeStore } from '../../storage/base';
import { Agent } from '../agent';
import {
  createMessageSignal,
  createSignal,
  dataPartToSignal,
  mastraDBMessageToSignal,
  resolveDeliveryAttributes,
  signalToDataPartFormat,
  signalToMastraDBMessage,
} from '../signals';
import { AgentThreadStreamRuntime, agentThreadStreamRuntime } from '../thread-stream-runtime';

function createTextStreamModel(responseText: string) {
  return new MockLanguageModelV2({
    doStream: async () => ({
      rawCall: { rawPrompt: null, rawSettings: {} },
      warnings: [],
      stream: convertArrayToReadableStream([
        { type: 'stream-start', warnings: [] },
        { type: 'response-metadata', id: 'id-0', modelId: 'mock-model-id', timestamp: new Date(0) },
        { type: 'text-start', id: 'text-1' },
        { type: 'text-delta', id: 'text-1', delta: responseText },
        { type: 'text-end', id: 'text-1' },
        {
          type: 'finish',
          finishReason: 'stop',
          usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
        },
      ]),
    }),
  });
}

function nextTick() {
  return new Promise(resolve => setTimeout(resolve, 0));
}

class AsyncFanoutPubSub extends PubSub {
  #inner = new EventEmitterPubSub();

  async publish(topic: string, event: Parameters<PubSub['publish']>[1]): Promise<void> {
    await nextTick();
    await this.#inner.publish(topic, event);
  }

  async subscribe(topic: string, cb: EventCallback, options?: SubscribeOptions): Promise<void> {
    await this.#inner.subscribe(topic, cb, options);
  }

  async unsubscribe(topic: string, cb: EventCallback): Promise<void> {
    await this.#inner.unsubscribe(topic, cb);
  }

  async flush(): Promise<void> {
    await this.#inner.flush();
  }
}

class AsyncCallbackPubSub extends PubSub {
  #subscribers = new Map<string, Set<EventCallback>>();
  #index = 0;
  #pending = new Set<Promise<void>>();

  async publish(topic: string, event: any, _options?: { localOnly?: boolean }): Promise<void> {
    const subscribers = [...(this.#subscribers.get(topic) ?? [])];
    const envelope = {
      ...event,
      id: `event-${this.#index}`,
      createdAt: new Date(),
      index: this.#index++,
    };
    const pending = new Promise<void>(resolve => {
      setTimeout(() => {
        try {
          for (const subscriber of subscribers) subscriber(envelope);
        } finally {
          resolve();
        }
      }, 0);
    });
    this.#pending.add(pending);
    pending.finally(() => this.#pending.delete(pending));
  }

  async subscribe(topic: string, cb: EventCallback): Promise<void> {
    const subscribers = this.#subscribers.get(topic) ?? new Set<EventCallback>();
    subscribers.add(cb);
    this.#subscribers.set(topic, subscribers);
  }

  async unsubscribe(topic: string, cb: EventCallback): Promise<void> {
    this.#subscribers.get(topic)?.delete(cb);
  }

  async flush(): Promise<void> {
    await Promise.all([...this.#pending]);
  }
}

async function readNextRunWithParts(iterator: AsyncIterator<any>) {
  let runId: string | undefined;
  let text = '';
  const parts: any[] = [];

  while (true) {
    const next = await iterator.next();
    if (next.done) return next;

    const part = next.value;
    parts.push(part);
    runId ??= part.runId;
    if (part.type === 'text-delta') {
      text += part.payload.text;
    }
    if (part.type === 'finish' || part.type === 'error' || part.type === 'abort') {
      return { value: { runId, text, part, parts }, done: false };
    }
  }
}

class BlockingRunCompletedPubSub extends EventEmitterPubSub {
  #unblockRunCompleted!: () => void;
  readonly blockedRunCompleted = new Promise<void>(resolve => {
    this.#unblockRunCompleted = resolve;
  });
  sawRunCompleted = false;

  override async publish(topic: string, event: Parameters<PubSub['publish']>[1]): Promise<void> {
    if ((event as { data?: { type?: string } }).data?.type === 'run-completed') {
      this.sawRunCompleted = true;
      await this.blockedRunCompleted;
    }
    await super.publish(topic, event);
  }

  unblockRunCompleted() {
    this.#unblockRunCompleted();
  }
}

class DeliverThenRejectRegistrationPubSub extends EventEmitterPubSub {
  #rejected = false;

  override async publish(topic: string, event: Parameters<PubSub['publish']>[1]): Promise<void> {
    await super.publish(topic, event);
    if (!this.#rejected && (event as { data?: { type?: string } }).data?.type === 'run-registered') {
      this.#rejected = true;
      throw new Error('injected registration failure after subscriber delivery');
    }
  }
}

class RejectFirstRunCompletedPubSub extends EventEmitterPubSub {
  #rejected = false;

  override async publish(topic: string, event: Parameters<PubSub['publish']>[1]): Promise<void> {
    if (!this.#rejected && (event as { data?: { type?: string } }).data?.type === 'run-completed') {
      this.#rejected = true;
      throw new Error('injected first terminal publication failure');
    }
    await super.publish(topic, event);
  }
}

async function readNextRun(iterator: AsyncIterator<any>) {
  let runId: string | undefined;
  let text = '';

  while (true) {
    const next = await iterator.next();
    if (next.done) return next;

    const part = next.value;
    runId ??= part.runId;
    if (part.type === 'text-delta') {
      text += part.payload.text;
    }
    if (part.type === 'finish' || part.type === 'error' || part.type === 'abort') {
      return { value: { runId, text, part }, done: false };
    }
  }
}

async function waitForActiveRun(subscription: { activeRunId: () => string | null }, timeoutMs = 500) {
  const startedAt = Date.now();
  let runId = subscription.activeRunId();
  while (!runId) {
    if (Date.now() - startedAt > timeoutMs) {
      throw new Error('Timed out waiting for active run');
    }
    await nextTick();
    runId = subscription.activeRunId();
  }
  return runId;
}

async function waitForCondition(predicate: () => boolean, timeoutMs = 500) {
  const startedAt = Date.now();
  while (!predicate()) {
    if (Date.now() - startedAt > timeoutMs) {
      throw new Error('Timed out waiting for condition');
    }
    await nextTick();
  }
}

async function withTimeout<T>(promise: Promise<T>, message: string, timeoutMs = 500): Promise<T> {
  let timeout: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<T>((_, reject) => {
        timeout = setTimeout(() => reject(new Error(message)), timeoutMs);
      }),
    ]);
  } finally {
    if (timeout) clearTimeout(timeout);
  }
}

describe('Agent signals', () => {
  beforeEach(() => {
    agentThreadStreamRuntime.resetForTests();
  });

  it('converts signals between DB, LLM, and data part formats', () => {
    const signal = createSignal({
      id: 'signal-1',
      type: 'user-message',
      contents: 'Signal contents',
      createdAt: new Date('2026-01-01T00:00:00.000Z'),
      attributes: { priority: 'high' },
      metadata: { source: 'test', signal: { userProvided: true } },
    });

    expect(signal.toLLMMessage()).toEqual({
      role: 'user',
      content: '<user priority="high">Signal contents</user>',
    });
    expect(signal.toDataPart()).toEqual({
      type: 'data-user-message',
      data: {
        id: 'signal-1',
        type: 'user',
        tagName: 'user',
        contents: 'Signal contents',
        createdAt: '2026-01-01T00:00:00.000Z',
        attributes: { priority: 'high' },
        metadata: { source: 'test', signal: { userProvided: true } },
      },
      transient: true,
    });

    const dbMessage = signal.toDBMessage({ threadId: 'thread-1', resourceId: 'resource-1' });
    expect(dbMessage.role).toBe('signal');
    expect(dbMessage.content.metadata).toEqual({
      signal: {
        id: 'signal-1',
        type: 'user',
        tagName: 'user',
        createdAt: '2026-01-01T00:00:00.000Z',
        attributes: { priority: 'high' },
        metadata: { source: 'test', signal: { userProvided: true } },
      },
    });
    expect(signalToMastraDBMessage(signal).role).toBe('signal');
    expect(mastraDBMessageToSignal(dbMessage).contents).toBe('Signal contents');
    expect(mastraDBMessageToSignal(dbMessage).attributes).toEqual({ priority: 'high' });
    expect(mastraDBMessageToSignal(dbMessage).metadata).toEqual({ source: 'test', signal: { userProvided: true } });
    expect(dataPartToSignal(signalToDataPartFormat(signal)).contents).toBe('Signal contents');

    const reminderSignal = createSignal({
      id: 'signal-2',
      type: 'system-reminder',
      contents: 'Use <safe> content & continue',
      createdAt: new Date('2026-01-01T00:00:00.000Z'),
      attributes: { type: 'dynamic-agents-md', path: '/tmp/AGENTS.md', enabled: true, ignored: null },
    });

    expect(reminderSignal.toLLMMessage()).toEqual({
      role: 'user',
      content:
        '<system-reminder type="dynamic-agents-md" path="/tmp/AGENTS.md" enabled="true">Use &lt;safe&gt; content &amp; continue</system-reminder>',
    });
    expect(reminderSignal.toDataPart().data.attributes).toEqual({
      type: 'dynamic-agents-md',
      path: '/tmp/AGENTS.md',
      enabled: true,
      ignored: null,
    });
    expect(mastraDBMessageToSignal(reminderSignal.toDBMessage()).attributes).toEqual({
      type: 'dynamic-agents-md',
      path: '/tmp/AGENTS.md',
      enabled: true,
      ignored: null,
    });

    const fileContents = [
      { type: 'text' as const, text: 'Review this file' },
      {
        type: 'file' as const,
        data: 'data:text/plain;base64,aGVsbG8=',
        mediaType: 'text/plain',
        filename: 'note.txt',
      },
    ];
    const fileSignal = createSignal({
      id: 'signal-3',
      type: 'user-message',
      contents: fileContents,
      createdAt: new Date('2026-01-01T00:00:00.000Z'),
    });

    // toLLMMessage emits the v5 UserModelMessage shape (uses mediaType for FilePart).
    expect(fileSignal.toLLMMessage()).toEqual({
      role: 'user',
      content: [
        { type: 'text', text: 'Review this file' },
        {
          type: 'file',
          data: 'data:text/plain;base64,aGVsbG8=',
          mediaType: 'text/plain',
          filename: 'note.txt',
        },
      ],
    });
    expect(fileSignal.toDataPart().data.contents).toEqual(fileContents);
    expect(mastraDBMessageToSignal(fileSignal.toDBMessage()).contents).toEqual(fileContents);
  });

  it('normalizes message signals and legacy signal types', () => {
    const messageSignal = createMessageSignal({
      contents: 'Hello',
      attributes: { sentFrom: 'test' },
    });
    expect(messageSignal.type).toBe('user');
    expect(messageSignal.tagName).toBe('user');
    expect(messageSignal.toLLMMessage()).toEqual({ role: 'user', content: '<user sentFrom="test">Hello</user>' });

    const legacyMessage = createSignal({ type: 'user-message', contents: 'Legacy message' });
    expect(legacyMessage.type).toBe('user');
    expect(legacyMessage.tagName).toBe('user');
    expect(legacyMessage.toLLMMessage()).toEqual({ role: 'user', content: 'Legacy message' });

    const legacyReminder = createSignal({ type: 'system-reminder', contents: 'Remember this' });
    expect(legacyReminder.type).toBe('reactive');
    expect(legacyReminder.tagName).toBe('system-reminder');
    expect(legacyReminder.toLLMMessage()).toEqual({
      role: 'user',
      content: '<system-reminder>Remember this</system-reminder>',
    });

    const reactiveReminder = createSignal({ type: 'reactive', contents: 'Default reminder tag' });
    expect(reactiveReminder.type).toBe('reactive');
    expect(reactiveReminder.tagName).toBe('system-reminder');
    expect(reactiveReminder.toLLMMessage()).toEqual({
      role: 'user',
      content: '<system-reminder>Default reminder tag</system-reminder>',
    });

    const customTaggedReminder = createSignal({
      type: 'reactive',
      tagName: 'custom-reminder',
      contents: 'Custom tag',
    });
    expect(customTaggedReminder.type).toBe('reactive');
    expect(customTaggedReminder.tagName).toBe('custom-reminder');
    expect(() => createSignal({ type: 'custom-reminder' as any, contents: 'Legacy custom' })).toThrow(
      'Invalid signal type: custom-reminder',
    );
  });

  it('renders user-message attributes inline-wrapped for text and multimodal contents', () => {
    const stringSignal = createSignal({
      type: 'user-message',
      contents: 'Hello',
      attributes: { messageId: 'm-1', userId: 'u-1' },
    });
    expect(stringSignal.toLLMMessage()).toEqual({
      role: 'user',
      content: '<user messageId="m-1" userId="u-1">Hello</user>',
    });

    const partsTextSignal = createSignal({
      type: 'user-message',
      contents: [{ type: 'text', text: 'Hello again' }],
      attributes: { messageId: 'm-1b' },
    });
    expect(partsTextSignal.toLLMMessage()).toEqual({
      role: 'user',
      content: '<user messageId="m-1b">Hello again</user>',
    });

    const fileContents = [
      { type: 'text' as const, text: 'Look at this' },
      {
        type: 'file' as const,
        data: 'data:image/png;base64,aGVsbG8=',
        mediaType: 'image/png',
      },
    ];
    const multimodalSignal = createSignal({
      type: 'user-message',
      contents: fileContents,
      attributes: { messageId: 'm-2' },
    });
    // Multimodal: text part is inline-wrapped, file part is preserved.
    const multimodalResult = multimodalSignal.toLLMMessage();
    expect(multimodalResult.role).toBe('user');
    expect(multimodalResult.content).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          type: 'text',
          text: '<user messageId="m-2">Look at this</user>',
        }),
        expect.objectContaining({
          type: 'file',
          data: 'data:image/png;base64,aGVsbG8=',
        }),
      ]),
    );

    // file-only: no text part exists, so the marker is prepended as a synthetic text part on
    // the same message so the attributes still surface alongside the file payload.
    const fileOnlyContents = [
      { type: 'file' as const, data: 'data:image/png;base64,aGVsbG8=', mediaType: 'image/png' },
    ];
    const fileOnlySignal = createSignal({
      type: 'user-message',
      contents: fileOnlyContents,
      attributes: { messageId: 'm-2d' },
    });
    const fileOnlyResult = fileOnlySignal.toLLMMessage();
    expect(fileOnlyResult.role).toBe('user');
    expect(fileOnlyResult.content).toEqual([
      expect.objectContaining({ type: 'text', text: '<user messageId="m-2d" />' }),
      expect.objectContaining({ type: 'file', data: 'data:image/png;base64,aGVsbG8=' }),
    ]);

    const noAttributeSignal = createSignal({
      type: 'user-message',
      contents: 'Plain message',
    });
    expect(noAttributeSignal.toLLMMessage()).toEqual({ role: 'user', content: 'Plain message' });

    const onlyNullAttributesSignal = createSignal({
      type: 'user-message',
      contents: 'Plain message',
      attributes: { ignored: null, alsoIgnored: undefined },
    });
    expect(onlyNullAttributesSignal.toLLMMessage()).toEqual({ role: 'user', content: 'Plain message' });
  });

  it('renders system-reminder signals with multimodal contents the same way as user-message attributes', () => {
    // Text-only system-reminder still wraps even without attributes (the wrapper is the signal).
    const plainReminder = createSignal({
      type: 'system-reminder',
      contents: 'Be concise.',
    });
    expect(plainReminder.toLLMMessage()).toEqual({
      role: 'user',
      content: '<system-reminder>Be concise.</system-reminder>',
    });

    // System-reminder with multimodal contents: text part is inline-wrapped with the marker,
    // file part is preserved alongside it on the same logical turn.
    const screenshotContents = [
      { type: 'text' as const, text: 'The user is looking at this screen.' },
      {
        type: 'file' as const,
        data: 'data:image/png;base64,aGVsbG8=',
        mediaType: 'image/png',
      },
    ];
    const screenshotReminder = createSignal({
      type: 'system-reminder',
      contents: screenshotContents,
      attributes: { kind: 'screenshot' },
    });
    const screenshotResult = screenshotReminder.toLLMMessage();
    expect(screenshotResult.role).toBe('user');
    expect(screenshotResult.content).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          type: 'text',
          text: '<system-reminder kind="screenshot">The user is looking at this screen.</system-reminder>',
        }),
        expect.objectContaining({
          type: 'file',
          data: 'data:image/png;base64,aGVsbG8=',
        }),
      ]),
    );

    // System-reminder with only file parts has no text to inline-wrap, so the marker is
    // prepended as a synthetic text part on the same message.
    const fileOnlyReminderContents = [
      { type: 'file' as const, data: 'data:image/png;base64,aGVsbG8=', mediaType: 'image/png' },
    ];
    const fileOnlyReminder = createSignal({
      type: 'system-reminder',
      contents: fileOnlyReminderContents,
      attributes: { kind: 'reference-image' },
    });
    const fileOnlyResult = fileOnlyReminder.toLLMMessage();
    expect(fileOnlyResult.role).toBe('user');
    expect(fileOnlyResult.content).toEqual([
      expect.objectContaining({ type: 'text', text: '<system-reminder kind="reference-image" />' }),
      expect.objectContaining({ type: 'file', data: 'data:image/png;base64,aGVsbG8=' }),
    ]);

    // System-reminder with mixed text + file parts: the marker is inlined into the very first
    // text part, subsequent parts pass through untouched on the same logical turn.
    const mixedReminderContents = [
      { type: 'text' as const, text: 'Step one of the screen.' },
      { type: 'text' as const, text: 'Step two has this attachment.' },
      { type: 'file' as const, data: 'data:image/png;base64,aGVsbG8=', mediaType: 'image/png' },
    ];
    const mixedReminder = createSignal({
      type: 'system-reminder',
      contents: mixedReminderContents,
      attributes: { kind: 'walkthrough' },
    });
    const mixedResult = mixedReminder.toLLMMessage();
    expect(mixedResult.content).toEqual([
      expect.objectContaining({
        type: 'text',
        text: '<system-reminder kind="walkthrough">Step one of the screen.</system-reminder>',
      }),
      expect.objectContaining({ type: 'text', text: 'Step two has this attachment.' }),
      expect.objectContaining({ type: 'file', data: 'data:image/png;base64,aGVsbG8=' }),
    ]);
  });

  it('persists multimodal signal contents as faithful DB parts so UIs can render them', () => {
    const fileContents = [
      { type: 'text' as const, text: 'Look at this' },
      { type: 'file' as const, data: 'data:image/png;base64,aGVsbG8=', mediaType: 'image/png' },
    ];

    const userMessage = createSignal({
      type: 'user-message',
      contents: fileContents,
      attributes: { messageId: 'm-1' },
    });
    const userDb = userMessage.toDBMessage();
    expect(userDb.content.parts).toEqual([
      expect.objectContaining({ type: 'text', text: 'Look at this' }),
      expect.objectContaining({ type: 'file', data: 'data:image/png;base64,aGVsbG8=' }),
    ]);
    // Stash is dropped — metadata.signal carries only envelope fields (id/type/attributes/createdAt).
    const signalMeta = (userDb.content.metadata as { signal: Record<string, unknown> }).signal;
    expect(signalMeta).not.toHaveProperty('contents');
    expect(signalMeta).toMatchObject({ type: 'user', tagName: 'user', attributes: { messageId: 'm-1' } });

    const reminder = createSignal({
      type: 'system-reminder',
      contents: fileContents,
      attributes: { kind: 'screenshot' },
    });
    const reminderDb = reminder.toDBMessage();
    expect(reminderDb.content.parts).toEqual([
      expect.objectContaining({ type: 'text', text: 'Look at this' }),
      expect.objectContaining({ type: 'file', data: 'data:image/png;base64,aGVsbG8=' }),
    ]);

    // Empty contents still produce a single empty text part so consumers that assume non-empty parts stay happy.
    const emptyReminder = createSignal({ type: 'system-reminder', contents: '' });
    expect(emptyReminder.toDBMessage().content.parts).toEqual([{ type: 'text', text: '' }]);
  });

  it('round-trips multimodal non-user-message signals through DB without dropping file parts', () => {
    const screenshotContents = [
      { type: 'text' as const, text: 'The user is looking at this screen.' },
      { type: 'file' as const, data: 'data:image/png;base64,aGVsbG8=', mediaType: 'image/png' },
    ];
    const reminder = createSignal({
      type: 'system-reminder',
      contents: screenshotContents,
      attributes: { kind: 'screenshot' },
    });
    const rehydrated = mastraDBMessageToSignal(reminder.toDBMessage());
    expect(rehydrated.type).toBe('reactive');
    expect(rehydrated.tagName).toBe('system-reminder');
    expect(rehydrated.contents).toEqual(screenshotContents);
    expect(rehydrated.attributes).toEqual({ kind: 'screenshot' });

    // dataPart round-trip preserves the multimodal shape too.
    const fromDataPart = dataPartToSignal(reminder.toDataPart());
    expect(fromDataPart.contents).toEqual(screenshotContents);
  });

  it('threads providerOptions through LLM message, DB storage, and rehydration', () => {
    const providerOptions = {
      openai: { reasoningEffort: 'high' },
      anthropic: { cacheControl: { type: 'ephemeral' } },
    };
    const signal = createSignal({
      type: 'user-message',
      contents: 'hello',
      providerOptions,
    });

    // LLM message: providerOptions on the CoreMessage so it flows to the model.
    const llmMessage = signal.toLLMMessage();
    expect(llmMessage).toMatchObject({ role: 'user', content: 'hello', providerOptions });

    // DB storage: content.providerMetadata (canonical location, also surfaces to useChat).
    const db = signal.toDBMessage();
    expect(db.content.providerMetadata).toEqual(providerOptions);

    // Round-trip: rehydrated signal carries providerOptions and re-emits it.
    const rehydrated = mastraDBMessageToSignal(db);
    expect(rehydrated.providerOptions).toEqual(providerOptions);
    expect(rehydrated.toLLMMessage()).toMatchObject({ providerOptions });
  });

  it('omits providerOptions on LLM / DB output when not provided', () => {
    const signal = createSignal({ type: 'user-message', contents: 'hi' });
    const llmMessage = signal.toLLMMessage();
    expect((llmMessage as { providerOptions?: unknown }).providerOptions).toBeUndefined();
    expect(signal.toDBMessage().content.providerMetadata).toBeUndefined();
  });

  it('threads per-part providerOptions through LLM, DB, and rehydration', () => {
    const partProviderOptions = { anthropic: { cacheControl: { type: 'ephemeral' } } };
    const signal = createSignal({
      type: 'user-message',
      contents: [
        { type: 'text', text: 'hello', providerOptions: partProviderOptions },
        { type: 'file', data: 'AAA=', mediaType: 'image/png' },
      ],
    });

    // LLM: parts array carries per-part providerOptions (not collapsed to bare string).
    const llmMessage = signal.toLLMMessage();
    expect(llmMessage.role).toBe('user');
    expect(Array.isArray(llmMessage.content)).toBe(true);
    const llmParts = llmMessage.content as Array<{ type: string; providerOptions?: unknown }>;
    expect(llmParts[0]).toMatchObject({ type: 'text', text: 'hello', providerOptions: partProviderOptions });
    expect(llmParts[1]).toMatchObject({ type: 'file', data: 'AAA=', mediaType: 'image/png' });

    // DB: per-part providerMetadata persisted alongside the storage part.
    const db = signal.toDBMessage();
    const textPart = db.content.parts[0] as { type: string; providerMetadata?: unknown };
    expect(textPart).toMatchObject({ type: 'text', text: 'hello', providerMetadata: partProviderOptions });

    // Round-trip: rehydrated signal restores per-part providerOptions.
    const rehydrated = mastraDBMessageToSignal(db);
    const rehydratedContents = rehydrated.contents as Array<{ type: string; providerOptions?: unknown }>;
    expect(rehydratedContents[0]).toMatchObject({ type: 'text', text: 'hello', providerOptions: partProviderOptions });
  });

  it('preserves per-part providerOptions on a single-text user-message (no bare-string collapse)', () => {
    const partProviderOptions = { anthropic: { cacheControl: { type: 'ephemeral' } } };
    const signal = createSignal({
      type: 'user-message',
      contents: [{ type: 'text', text: 'hello', providerOptions: partProviderOptions }],
    });

    const llmMessage = signal.toLLMMessage();
    // Must keep parts array — collapsing to a bare string would drop providerOptions.
    expect(Array.isArray(llmMessage.content)).toBe(true);
    const llmParts = llmMessage.content as Array<{ type: string; providerOptions?: unknown }>;
    expect(llmParts[0]).toMatchObject({ type: 'text', text: 'hello', providerOptions: partProviderOptions });
  });

  describe('legacy metadata.signal.contents rehydration', () => {
    function buildLegacyDBRow(legacyContents: unknown) {
      const row = createSignal({
        id: 'signal-legacy',
        createdAt: '2026-01-01T00:00:00.000Z',
        type: 'user-message',
        contents: 'placeholder',
      }).toDBMessage();
      row.content.metadata = {
        ...row.content.metadata,
        signal: {
          ...(row.content.metadata?.signal as Record<string, unknown>),
          contents: legacyContents,
        },
      };
      return row;
    }

    it('recovers a bare string stash', () => {
      const rehydrated = mastraDBMessageToSignal(buildLegacyDBRow('hello world'));
      expect(rehydrated.contents).toBe('hello world');
    });

    it('recovers an Array<TextPart | FilePart> stash with mediaType', () => {
      const rehydrated = mastraDBMessageToSignal(
        buildLegacyDBRow([
          { type: 'text', text: 'caption' },
          { type: 'file', data: 'BASE64', mediaType: 'image/png', filename: 'photo.png' },
        ]),
      );
      expect(rehydrated.contents).toEqual([
        { type: 'text', text: 'caption' },
        { type: 'file', data: 'BASE64', mediaType: 'image/png', filename: 'photo.png' },
      ]);
    });

    it('recovers a CoreUserMessage wrapper with text-only content', () => {
      const rehydrated = mastraDBMessageToSignal(buildLegacyDBRow({ role: 'user', content: 'hello world' }));
      expect(rehydrated.contents).toBe('hello world');
    });

    it('recovers a CoreUserMessage wrapper with mixed text + image parts', () => {
      const rehydrated = mastraDBMessageToSignal(
        buildLegacyDBRow({
          role: 'user',
          content: [
            { type: 'text', text: 'what is this?' },
            { type: 'image', image: 'BASE64', mediaType: 'image/png' },
          ],
        }),
      );
      expect(rehydrated.contents).toEqual([
        { type: 'text', text: 'what is this?' },
        { type: 'file', data: 'BASE64', mediaType: 'image/png' },
      ]);
    });

    it('recovers a CoreUserMessage[] stash from the React hook', () => {
      const rehydrated = mastraDBMessageToSignal(
        buildLegacyDBRow([
          { role: 'user', content: 'first' },
          { role: 'user', content: [{ type: 'text', text: 'second' }] },
        ]),
      );
      expect(rehydrated.contents).toEqual([
        { type: 'text', text: 'first' },
        { type: 'text', text: 'second' },
      ]);
    });

    it('falls back to canonical content.parts when the stash is unrecognisable', () => {
      const row = buildLegacyDBRow({ totally: 'unrelated' });
      row.content.parts = [{ type: 'text', text: 'from canonical parts' }];
      const rehydrated = mastraDBMessageToSignal(row);
      expect(rehydrated.contents).toBe('from canonical parts');
    });

    it('prefers a valid multimodal stash over flattened-text content.parts (main-era rows)', () => {
      // Main wrote the full original input to metadata.signal.contents and a flattened text
      // projection to content.parts. If we preferred parts here we'd silently drop the file
      // payload on rehydrate.
      const row = buildLegacyDBRow([
        { type: 'text', text: 'caption' },
        { type: 'file', data: 'BASE64', mediaType: 'image/png', filename: 'photo.png' },
      ]);
      row.content.parts = [{ type: 'text', text: 'caption' }];
      const rehydrated = mastraDBMessageToSignal(row);
      expect(rehydrated.contents).toEqual([
        { type: 'text', text: 'caption' },
        { type: 'file', data: 'BASE64', mediaType: 'image/png', filename: 'photo.png' },
      ]);
    });
  });

  it('rejects invalid XML names for contextual signal markup', () => {
    expect(() =>
      createSignal({
        type: 'reactive',
        tagName: 'system reminder',
        contents: 'invalid tag name',
      }).toLLMMessage(),
    ).toThrow('Invalid signal XML tag name: system reminder');

    expect(() =>
      createSignal({
        type: 'system-reminder',
        contents: 'invalid attribute name',
        attributes: { 'bad attr': 'value' },
      }).toLLMMessage(),
    ).toThrow('Invalid signal XML attribute name: bad attr');
  });

  it('subscribes to a future thread run', async () => {
    const agent = new Agent({
      id: 'future-thread-agent',
      name: 'Future Thread Agent',
      instructions: 'Test',
      model: createTextStreamModel('future response'),
    });

    const subscription = await agent.subscribeToThread({
      threadId: 'future-thread',
      resourceId: 'future-user',
    });
    const nextRun = readNextRun(subscription.stream[Symbol.asyncIterator]());

    const stream = await agent.stream('Hello', {
      memory: { thread: 'future-thread', resource: 'future-user' },
    });

    const subscribedRun = await nextRun;
    expect(subscribedRun.value.runId).toBe(stream.runId);
    expect(subscribedRun.value.text).toBe('future response');

    subscription.unsubscribe();
  });

  it('does not reject a partially delivered registration until its enqueued stream is drained', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new DeliverThenRejectRegistrationPubSub();
    const agent = { id: 'partial-registration-agent' } as Agent<any, any, any, any>;
    const threadId = 'partial-registration-thread';
    const resourceId = 'partial-registration-user';
    const runId = 'partial-registration-run';
    const parts = [
      { type: 'start', runId },
      { type: 'tool-call', runId, payload: { toolCallId: 'late-tool', toolName: 'lookup', args: {} } },
      {
        type: 'finish',
        runId,
        payload: { usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 }, finishReason: 'stop' },
      },
    ];
    const output = {
      runId,
      status: 'running',
      fullStream: new ReadableStream({
        start(controller) {
          for (const part of parts) controller.enqueue(part);
          controller.close();
        },
      }),
      _waitUntilFinished: () => Promise.resolve(),
    } as any;
    const subscription = await runtime.subscribeToThread(agent, { threadId, resourceId }, pubsub);

    const completion = runtime.registerRun(
      agent,
      output,
      { memory: { thread: threadId, resource: resourceId } } as any,
      pubsub,
    );
    void completion?.catch(() => {});
    const outputDrain = subscription._waitForOutputDrain!(output)!;
    let settled = false;
    void outputDrain.finally(() => (settled = true)).catch(() => {});

    await nextTick();
    expect(settled).toBe(false);

    const iterator = subscription.stream[Symbol.asyncIterator]();
    const received = [];
    for (let index = 0; index < parts.length; index++) {
      received.push((await iterator.next()).value);
    }
    // Advance once more so the per-output generator observes the source close
    // and acknowledges that no buffered chunk can surface after rejection.
    const waitingForNextRun = iterator.next();

    await expect(outputDrain).rejects.toMatchObject({
      name: 'AgentThreadOutputDrainError',
      reason: 'registration-publish-failed',
    });
    expect(received).toEqual(parts);

    subscription.unsubscribe();
    await waitingForNextRun;
  });

  it('removes a failed terminal without a drain waiter before the same run id is reused', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new RejectFirstRunCompletedPubSub();
    const agent = { id: 'failed-terminal-reuse-agent' } as Agent<any, any, any, any>;
    const threadId = 'failed-terminal-reuse-thread';
    const resourceId = 'failed-terminal-reuse-user';
    const runId = 'failed-terminal-reuse-run';
    const target = { memory: { thread: threadId, resource: resourceId } } as any;
    const createOutput = (text: string) =>
      ({
        runId,
        status: 'running',
        fullStream: new ReadableStream({
          start(controller) {
            controller.enqueue({ type: 'start', runId });
            controller.enqueue({ type: 'text-delta', runId, payload: { id: 'text-1', text } });
            controller.enqueue({
              type: 'finish',
              runId,
              payload: { usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 }, finishReason: 'stop' },
            });
            controller.close();
          },
        }),
        _waitUntilFinished: () => Promise.resolve(),
      }) as any;
    const subscription = await runtime.subscribeToThread(agent, { threadId, resourceId }, pubsub);
    const iterator = subscription.stream[Symbol.asyncIterator]();

    try {
      const firstOutput = createOutput('first');
      const firstCompletion = runtime.registerRun(agent, firstOutput, target, pubsub)!;
      // Deliberately do not call `_waitForOutputDrain(firstOutput)`: terminal
      // rejection cleanup must be owned by the subscription itself.
      await expect(firstCompletion).rejects.toMatchObject({
        name: 'AgentThreadOutputDrainError',
        reason: 'terminal-publish-failed',
      });
      await expect(
        withTimeout(readNextRun(iterator), 'Timed out draining the failed first output'),
      ).resolves.toMatchObject({ value: { runId, text: 'first' } });

      runtime.reserveRun({ ...target, runId }, pubsub, agent.id);
      const secondOutput = createOutput('second');
      const secondCompletion = runtime.registerRun(agent, secondOutput, target, pubsub)!;
      const secondDrain = subscription._waitForOutputDrain!(secondOutput)!;
      const secondRun = readNextRun(iterator);

      await expect(secondCompletion).resolves.toBeUndefined();
      await expect(withTimeout(secondRun, 'Timed out draining the reused run id')).resolves.toMatchObject({
        value: { runId, text: 'second' },
      });
      // Advance the generator through the stream close so the second output's
      // own terminal can satisfy its drain barrier. A stale first-output entry
      // would leave this promise unresolved.
      const waitingForNextRun = iterator.next();
      await expect(withTimeout(secondDrain, 'Reused run id terminal was correlated to the wrong output')).resolves.toBe(
        undefined,
      );
      subscription.unsubscribe();
      await waitingForNextRun;
    } finally {
      subscription.unsubscribe();
    }
  });

  it('delivers each thread run to multiple same-runtime subscribers', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const agent = { id: 'multi-subscriber-thread-agent' } as Agent<any, any, any, any>;
    const threadId = 'multi-subscriber-thread';
    const resourceId = 'multi-subscriber-user';

    const registerRun = (runNumber: number) => {
      const runId = `multi-subscriber-run-${runNumber}`;
      let finish!: () => void;
      const finished = new Promise<void>(resolve => {
        finish = resolve;
      });
      const parts = [
        { type: 'start', runId },
        { type: 'text-start', runId, payload: { id: `text-${runNumber}` } },
        { type: 'text-delta', runId, payload: { id: `text-${runNumber}`, text: `response ${runNumber}` } },
        { type: 'text-end', runId, payload: { id: `text-${runNumber}` } },
        {
          type: 'finish',
          runId,
          payload: { usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 }, finishReason: 'stop' },
        },
      ];
      const fullStream = new ReadableStream({
        start(controller) {
          setTimeout(() => {
            for (const part of parts) controller.enqueue(part);
            controller.close();
            finish();
          }, 25);
        },
      });

      runtime.registerRun(
        agent,
        {
          runId,
          status: 'running',
          fullStream,
          _waitUntilFinished: () => finished,
        } as any,
        { memory: { thread: threadId, resource: resourceId } } as any,
      );
      return runId;
    };

    const firstSubscription = await runtime.subscribeToThread(agent, { threadId, resourceId });
    const secondSubscription = await runtime.subscribeToThread(agent, { threadId, resourceId });
    const firstIterator = firstSubscription.stream[Symbol.asyncIterator]();
    const secondIterator = secondSubscription.stream[Symbol.asyncIterator]();

    try {
      const firstSubscriberRun1 = readNextRun(firstIterator);
      const secondSubscriberRun1 = readNextRun(secondIterator);
      const runId1 = registerRun(1);

      const [run1a, run1b] = await Promise.all([
        withTimeout(firstSubscriberRun1, 'Timed out waiting for first subscriber to receive run 1'),
        withTimeout(secondSubscriberRun1, 'Timed out waiting for second subscriber to receive run 1'),
      ]);
      expect(run1a.value).toMatchObject({ runId: runId1, text: 'response 1' });
      expect(run1b.value).toMatchObject({ runId: runId1, text: 'response 1' });

      const firstSubscriberRun2 = readNextRun(firstIterator);
      const secondSubscriberRun2 = readNextRun(secondIterator);
      const runId2 = registerRun(2);

      const [run2a, run2b] = await Promise.all([
        withTimeout(firstSubscriberRun2, 'Timed out waiting for first subscriber to receive run 2'),
        withTimeout(secondSubscriberRun2, 'Timed out waiting for second subscriber to receive run 2'),
      ]);
      expect(run2a.value).toMatchObject({ runId: runId2, text: 'response 2' });
      expect(run2b.value).toMatchObject({ runId: runId2, text: 'response 2' });
    } finally {
      firstSubscription.unsubscribe();
      secondSubscription.unsubscribe();
    }
  });

  it('delivers resumed runs with the same run id to thread subscribers', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const agent = { id: 'resumed-thread-agent' } as Agent<any, any, any, any>;
    const threadId = 'resumed-thread';
    const resourceId = 'resumed-user';
    const runId = 'resumed-run';

    const createRun = (parts: any[]) => {
      let finish!: () => void;
      const finished = new Promise<void>(resolve => {
        finish = resolve;
      });
      const fullStream = new ReadableStream({
        start(controller) {
          setTimeout(() => {
            for (const part of parts) controller.enqueue(part);
            controller.close();
            finish();
          }, 5);
        },
      });

      runtime.registerRun(
        agent,
        {
          runId,
          status: 'running',
          fullStream,
          _waitUntilFinished: () => finished,
        } as any,
        { memory: { thread: threadId, resource: resourceId } } as any,
      );
    };

    const subscription = await runtime.subscribeToThread(agent, { threadId, resourceId });
    const iterator = subscription.stream[Symbol.asyncIterator]();

    try {
      createRun([
        { type: 'start', runId },
        {
          type: 'tool-call-suspended',
          runId,
          payload: { toolCallId: 'tool-call-1', toolName: 'testTool' },
        },
      ]);

      await withTimeout(iterator.next(), 'Timed out waiting for initial resumed-run start');
      const suspended = await withTimeout(iterator.next(), 'Timed out waiting for suspended chunk');
      expect(suspended.value).toMatchObject({ type: 'tool-call-suspended', runId });
      await waitForCondition(() => subscription.activeRunId() === null);

      const resumedRun = readNextRun(iterator);
      createRun([
        { type: 'start', runId },
        { type: 'text-start', runId, payload: { id: 'text-1' } },
        { type: 'text-delta', runId, payload: { id: 'text-1', text: 'approved response' } },
        { type: 'text-end', runId, payload: { id: 'text-1' } },
        {
          type: 'finish',
          runId,
          payload: { usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 }, finishReason: 'stop' },
        },
      ]);

      await expect(withTimeout(resumedRun, 'Timed out waiting for resumed run')).resolves.toMatchObject({
        value: { runId, text: 'approved response' },
      });
    } finally {
      subscription.unsubscribe();
    }
  });

  it('keeps subscriber streams open across tool-call finish boundaries until tool results arrive', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const agent = { id: 'tool-call-boundary-agent' } as Agent<any, any, any, any>;
    const threadId = 'tool-call-boundary-thread';
    const resourceId = 'tool-call-boundary-user';
    const runId = 'tool-call-boundary-run';
    let finish!: () => void;
    const finished = new Promise<void>(resolve => {
      finish = resolve;
    });
    const fullStream = new ReadableStream({
      start(controller) {
        controller.enqueue({ type: 'start', runId });
        controller.enqueue({ type: 'tool-call', runId, payload: { toolCallId: 'tool-1', toolName: 'testTool' } });
        controller.enqueue({
          type: 'finish',
          runId,
          payload: { finishReason: 'tool-calls', usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 } },
        });
        controller.enqueue({ type: 'tool-result', runId, payload: { toolCallId: 'tool-1', result: 'tool output' } });
        controller.enqueue({
          type: 'finish',
          runId,
          payload: { finishReason: 'stop', usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 } },
        });
        controller.close();
        finish();
      },
    });

    const subscription = await runtime.subscribeToThread(agent, { threadId, resourceId });
    const iterator = subscription.stream[Symbol.asyncIterator]();

    try {
      runtime.registerRun(
        agent,
        {
          runId,
          status: 'running',
          fullStream,
          _waitUntilFinished: () => finished,
        } as any,
        { memory: { thread: threadId, resource: resourceId } } as any,
      );

      await expect(withTimeout(iterator.next(), 'Timed out waiting for boundary start')).resolves.toMatchObject({
        value: { type: 'start', runId },
      });
      await expect(withTimeout(iterator.next(), 'Timed out waiting for boundary tool call')).resolves.toMatchObject({
        value: { type: 'tool-call', runId },
      });
      await expect(withTimeout(iterator.next(), 'Timed out waiting for tool-call finish')).resolves.toMatchObject({
        value: { type: 'finish', runId, payload: expect.objectContaining({ finishReason: 'tool-calls' }) },
      });
      await expect(withTimeout(iterator.next(), 'Timed out waiting for live tool result')).resolves.toMatchObject({
        value: { type: 'tool-result', runId, payload: expect.objectContaining({ toolCallId: 'tool-1' }) },
      });
      await expect(withTimeout(iterator.next(), 'Timed out waiting for final finish')).resolves.toMatchObject({
        value: { type: 'finish', runId, payload: expect.objectContaining({ finishReason: 'stop' }) },
      });
    } finally {
      subscription.unsubscribe();
    }
  });

  it('assigns a new stream identity to same-run registrations without stale cleanup clearing the active stream', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new BlockingRunCompletedPubSub();
    const agent = { id: 'stream-identity-agent' } as Agent<any, any, any, any>;
    const threadId = 'stream-identity-thread';
    const resourceId = 'stream-identity-resource';
    const runId = 'stream-identity-run';
    const topic = `agent.thread-stream.${encodeURIComponent(`${resourceId}\u0000${threadId}`)}`;
    const publishedEvents: any[] = [];
    await pubsub.subscribe(topic, event => publishedEvents.push(event.data));

    const createRun = (text: string, finished: Promise<void>) => {
      const fullStream = new ReadableStream({
        start(controller) {
          controller.enqueue({ type: 'start', runId });
          controller.enqueue({ type: 'text-delta', runId, payload: { text } });
          controller.enqueue({ type: 'finish', runId, payload: { finishReason: 'stop' } });
          controller.close();
        },
      });

      return runtime.registerRun(
        agent,
        {
          runId,
          status: 'running',
          fullStream,
          _waitUntilFinished: () => finished,
        } as any,
        { memory: { thread: threadId, resource: resourceId } } as any,
        pubsub,
      );
    };

    let finishInitial!: () => void;
    const initialFinished = new Promise<void>(resolve => {
      finishInitial = resolve;
    });
    let finishResumed!: () => void;
    const resumedFinished = new Promise<void>(resolve => {
      finishResumed = resolve;
    });

    const subscription = await runtime.subscribeToThread(agent, { threadId, resourceId }, pubsub);
    const iterator = subscription.stream[Symbol.asyncIterator]();

    try {
      const initialRun = readNextRun(iterator);
      const initialCompletion = createRun('initial response', initialFinished)!;
      await expect(withTimeout(initialRun, 'Timed out waiting for initial stream identity run')).resolves.toMatchObject(
        {
          value: { runId, text: 'initial response' },
        },
      );
      expect(subscription.activeRunId()).toBe(runId);

      finishInitial();
      await waitForCondition(() => pubsub.sawRunCompleted);

      const resumedRun = readNextRun(iterator);
      const resumedCompletion = createRun('resumed response', resumedFinished)!;
      await expect(withTimeout(resumedRun, 'Timed out waiting for resumed stream identity run')).resolves.toMatchObject(
        {
          value: { runId, text: 'resumed response' },
        },
      );

      const registeredEvents = publishedEvents.filter(event => event?.type === 'run-registered');
      expect(registeredEvents).toHaveLength(2);
      expect(registeredEvents.map(event => event.runId)).toEqual([runId, runId]);
      expect(registeredEvents.map(event => event.streamSeq)).toEqual([1, 2]);
      expect(registeredEvents[0].streamId).toEqual(expect.any(String));
      expect(registeredEvents[1].streamId).toEqual(expect.any(String));
      expect(registeredEvents[1].streamId).not.toBe(registeredEvents[0].streamId);

      pubsub.unblockRunCompleted();
      await initialCompletion;
      await nextTick();
      expect(subscription.activeRunId()).toBe(runId);

      finishResumed();
      await resumedCompletion;
      expect(subscription.activeRunId()).toBeNull();
    } finally {
      subscription.unsubscribe();
    }
  });

  it('keeps multicast thread streams alive when one subscriber unsubscribes mid-run', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const agent = { id: 'subscriber-cancel-agent' } as Agent<any, any, any, any>;
    const threadId = 'subscriber-cancel-thread';
    const resourceId = 'subscriber-cancel-user';
    const runId = 'subscriber-cancel-run';
    let finish!: () => void;
    const finished = new Promise<void>(resolve => {
      finish = resolve;
    });
    const parts = [
      { type: 'start', runId },
      { type: 'text-start', runId, payload: { id: 'text-1' } },
      { type: 'text-delta', runId, payload: { id: 'text-1', text: 'still running' } },
      { type: 'text-end', runId, payload: { id: 'text-1' } },
      {
        type: 'finish',
        runId,
        payload: { usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 }, finishReason: 'stop' },
      },
    ];
    const fullStream = new ReadableStream({
      async start(controller) {
        for (const part of parts) {
          await new Promise(resolve => setTimeout(resolve, 5));
          controller.enqueue(part);
        }
        controller.close();
        finish();
      },
    });

    const firstSubscription = await runtime.subscribeToThread(agent, { threadId, resourceId });
    const secondSubscription = await runtime.subscribeToThread(agent, { threadId, resourceId });
    const firstIterator = firstSubscription.stream[Symbol.asyncIterator]();
    const secondIterator = secondSubscription.stream[Symbol.asyncIterator]();

    try {
      const secondRun = readNextRun(secondIterator);
      runtime.registerRun(
        agent,
        {
          runId,
          status: 'running',
          fullStream,
          _waitUntilFinished: () => finished,
        } as any,
        { memory: { thread: threadId, resource: resourceId } } as any,
      );

      const firstPart = await withTimeout(firstIterator.next(), 'Timed out waiting for first subscriber part');
      expect(firstPart.value).toMatchObject({ type: 'start', runId });
      await firstIterator.return?.();
      firstSubscription.unsubscribe();

      await expect(
        withTimeout(secondRun, 'Timed out waiting for second subscriber to finish run'),
      ).resolves.toMatchObject({
        value: { runId, text: 'still running' },
        done: false,
      });
    } finally {
      firstSubscription.unsubscribe();
      secondSubscription.unsubscribe();
    }
  });

  it('starts an idle thread run when a user-message signal is sent', async () => {
    const agent = new Agent({
      id: 'idle-signal-agent',
      name: 'Idle Signal Agent',
      instructions: 'Test',
      model: createTextStreamModel('signal response'),
    });

    const subscription = await agent.subscribeToThread({
      threadId: 'idle-thread',
      resourceId: 'idle-user',
    });
    const nextRun = readNextRun(subscription.stream[Symbol.asyncIterator]());

    const signalResult = await agent.sendSignal(
      { type: 'user-message', contents: 'Hello from signal' },
      {
        resourceId: 'idle-user',
        threadId: 'idle-thread',
        ifIdle: { streamOptions: { memory: { resource: 'idle-user', thread: 'idle-thread' } } },
      },
    );

    const subscribedRun = await nextRun;
    await expect(signalResult.accepted).resolves.toMatchObject({ action: 'wake', runId: subscribedRun.value.runId });
    expect(signalResult.signal.id).toBeDefined();
    expect(subscribedRun.value.text).toBe('signal response');

    subscription.unsubscribe();
  });

  it('starts an idle thread run when sendMessage is called', async () => {
    const agent = new Agent({
      id: 'idle-message-agent',
      name: 'Idle Message Agent',
      instructions: 'Test',
      model: createTextStreamModel('message response'),
    });

    const subscription = await agent.subscribeToThread({
      threadId: 'idle-message-thread',
      resourceId: 'idle-message-user',
    });
    const nextRun = readNextRunWithParts(subscription.stream[Symbol.asyncIterator]());

    const result = await agent.sendMessage(
      { contents: 'Hello from sendMessage', attributes: { sentFrom: 'test' } },
      {
        resourceId: 'idle-message-user',
        threadId: 'idle-message-thread',
        ifIdle: { streamOptions: { memory: { resource: 'idle-message-user', thread: 'idle-message-thread' } } },
      },
    );

    const subscribedRun = await nextRun;
    await expect(result.accepted).resolves.toMatchObject({ action: 'wake', runId: subscribedRun.value.runId });
    expect(result.signal).toMatchObject({ type: 'user', tagName: 'user', contents: 'Hello from sendMessage' });
    const signalPart = subscribedRun.value.parts.find((part: any) => part.type === 'data-user-message');
    expect(signalPart?.data).toMatchObject({
      id: result.signal.id,
      type: 'user',
      tagName: 'user',
      contents: 'Hello from sendMessage',
      attributes: { sentFrom: 'test' },
    });
    expect(subscribedRun.value.text).toBe('message response');

    subscription.unsubscribe();
  });

  it('persists external state signals with cache-key tracking', async () => {
    const memory = new MockMemory();
    await memory.createThread({ threadId: 'state-thread', resourceId: 'state-user' });
    const agent = new Agent({
      id: 'state-agent',
      name: 'State Agent',
      instructions: 'Test',
      model: createTextStreamModel('state response'),
      memory,
    });

    const result = await agent.sendStateSignal(
      {
        id: 'browser',
        cacheKey: 'browser:v1',
        mode: 'snapshot',
        contents: 'Browser is open on https://example.com',
        value: { activeUrl: 'https://example.com' },
      },
      { resourceId: 'state-user', threadId: 'state-thread', ifIdle: { behavior: 'persist' } },
    );
    if (result.skipped) throw new Error('expected state signal to be persisted, not skipped');
    await expect(result.accepted).resolves.toMatchObject({ action: 'persist' });
    expect(result.signal).toBeDefined();

    expect(result.signal).toMatchObject({
      type: 'state',
      tagName: 'state',
      metadata: expect.objectContaining({
        state: expect.objectContaining({ id: 'browser', cacheKey: 'browser:v1', mode: 'snapshot', version: 1 }),
        value: { activeUrl: 'https://example.com' },
      }),
    });
    await expect(
      agent.sendStateSignal(
        { id: 'browser', cacheKey: 'browser:v1', contents: 'unchanged' },
        { resourceId: 'state-user', threadId: 'state-thread', ifIdle: { behavior: 'persist' } },
      ),
    ).resolves.toEqual({ skipped: true, reason: 'unchanged' });
    const thread = await memory.getThreadById({ threadId: 'state-thread' });
    expect(thread?.metadata?.mastra).toEqual(
      expect.objectContaining({
        stateSignals: expect.objectContaining({
          browser: expect.objectContaining({
            currentCacheKey: 'browser:v1',
            version: 1,
            lastSnapshotSignalId: result.signal!.id,
          }),
        }),
      }),
    );
  });

  it('delivers medium-priority notification records while idle', async () => {
    const notifications = new InMemoryNotificationsStorage();
    const storage = new MastraCompositeStore({ id: 'notification-storage', domains: { notifications } });
    const agent = new Agent({
      id: 'notification-agent',
      name: 'Notification Agent',
      instructions: 'Test',
      model: createTextStreamModel('notification response'),
    });
    new Mastra({ agents: { notificationAgent: agent }, storage, logger: false });

    const subscription = await agent.subscribeToThread({
      threadId: 'notification-thread',
      resourceId: 'notification-user',
    });
    const nextRun = readNextRunWithParts(subscription.stream[Symbol.asyncIterator]());

    const result = await agent.sendNotificationSignal(
      {
        source: 'github',
        kind: 'ci-status',
        priority: 'medium',
        summary: 'CI failed on main',
        dedupeKey: 'main-ci',
      },
      {
        resourceId: 'notification-user',
        threadId: 'notification-thread',
        ifIdle: { streamOptions: { memory: { resource: 'notification-user', thread: 'notification-thread' } } },
      },
    );

    const subscribedRun = await nextRun;
    expect(result).toEqual(expect.objectContaining({ runId: subscribedRun.value.runId }));
    await expect(result.accepted).resolves.toMatchObject({ action: 'wake', runId: subscribedRun.value.runId });
    expect(result.decision).toMatchObject({ action: 'deliver' });
    expect(result.record).toMatchObject({
      agentId: 'notification-agent',
      resourceId: 'notification-user',
      threadId: 'notification-thread',
      status: 'delivered',
      deliveredSignalId: result.signal?.id,
    });
    const signalPart = subscribedRun.value.parts.find((part: any) => part.type === 'data-signal');
    expect(signalPart?.data).toMatchObject({
      id: result.signal?.id,
      type: 'notification',
      tagName: 'notification',
      contents: 'CI failed on main',
      attributes: { source: 'github', kind: 'ci-status', priority: 'medium', status: 'delivered' },
    });
    await expect(
      notifications.getNotification({ threadId: 'notification-thread', id: result.record.id }),
    ).resolves.toMatchObject({ status: 'delivered', deliveredSignalId: result.signal?.id });

    subscription.unsubscribe();
  });

  it('delivers batched idle notifications using one initial thread-state decision', async () => {
    const notifications = new InMemoryNotificationsStorage();
    const storage = new MastraCompositeStore({ id: 'notification-batch-storage', domains: { notifications } });
    const agent = new Agent({
      id: 'notification-batch-agent',
      name: 'Notification Batch Agent',
      instructions: 'Test',
      model: createTextStreamModel('notification batch response'),
    });
    new Mastra({ agents: { notificationBatchAgent: agent }, storage, logger: false });

    const results = await agent.sendNotificationSignal(
      [
        { source: 'github', kind: 'pull-request-ci-failure', priority: 'high', summary: 'CI failed' },
        { source: 'github', kind: 'pull-request-activity', priority: 'high', summary: 'Devin commented' },
      ],
      { resourceId: 'notification-batch-user', threadId: 'notification-batch-thread' },
    );

    expect(results).toHaveLength(2);
    expect(results[0]).toMatchObject({ decision: { action: 'deliver', reason: 'idle-high' } });
    await expect(results[0]?.accepted).resolves.toMatchObject({ action: expect.stringMatching(/wake|deliver/) });
    expect(results[0]?.signal).toMatchObject({ type: 'notification', tagName: 'notification' });
    expect(results[0]?.record).toMatchObject({ status: 'delivered', deliveredSignalId: results[0]?.signal?.id });
    expect(results[1]).toMatchObject({ decision: { action: 'deliver', reason: 'idle-high' } });
    await expect(results[1]?.accepted).resolves.toMatchObject({ action: expect.stringMatching(/wake|deliver/) });
    expect(results[1]?.signal).toMatchObject({ type: 'notification', tagName: 'notification' });
    expect(results[1]?.record).toMatchObject({ status: 'delivered', deliveredSignalId: results[1]?.signal?.id });
  });

  it('wakes idle threads for immediate medium-priority notification summaries', async () => {
    let streamCount = 0;
    const notifications = new InMemoryNotificationsStorage();
    const storage = new MastraCompositeStore({ id: 'medium-summary-wake-storage', domains: { notifications } });
    const agent = new Agent({
      id: 'medium-summary-wake-agent',
      name: 'Medium Summary Wake Agent',
      instructions: 'Test',
      model: new MockLanguageModelV2({
        doStream: async () => {
          streamCount += 1;
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: convertArrayToReadableStream([
              { type: 'stream-start', warnings: [] },
              { type: 'finish', finishReason: 'stop', usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 } },
            ]),
          };
        },
      }),
      notifications: {
        deliveryPolicy: {
          decide: ({ now }) => ({ action: 'summarize', summaryAt: now, reason: 'test-medium-summary-now' }),
        },
      },
    });
    new Mastra({ agents: { mediumSummaryWakeAgent: agent }, storage, logger: false });
    const subscription = await agent.subscribeToThread({
      threadId: 'medium-summary-wake-thread',
      resourceId: 'medium-summary-wake-user',
    });
    const nextRun = readNextRunWithParts(subscription.stream[Symbol.asyncIterator]());

    const result = await agent.sendNotificationSignal(
      { source: 'github', kind: 'pull-request-activity', priority: 'medium', summary: 'Devin commented' },
      { resourceId: 'medium-summary-wake-user', threadId: 'medium-summary-wake-thread' },
    );
    const subscribedRun = await withTimeout(nextRun, 'Timed out waiting for medium notification summary wake');
    const signalPart = subscribedRun.value.parts.find((part: any) => part.type === 'data-signal');

    expect(result.signal).toMatchObject({ type: 'notification', tagName: 'notification-summary' });
    expect(result.decision).toMatchObject({ action: 'summarize', reason: 'test-medium-summary-now' });
    expect(result.record).toMatchObject({
      status: 'pending',
      summaryAt: undefined,
      summarySignalId: result.signal?.id,
    });
    expect(signalPart?.data).toMatchObject({
      id: result.signal?.id,
      type: 'notification',
      tagName: 'notification-summary',
      contents: 'github: 1',
      attributes: { pending: 1 },
    });
    expect(streamCount).toBe(1);

    subscription.unsubscribe();
  });

  it('keeps immediate notification records pending when runtime rejects delivery', async () => {
    const notifications = new InMemoryNotificationsStorage();
    const storage = new MastraCompositeStore({ id: 'rejected-notification-storage', domains: { notifications } });
    const agent = new Agent({
      id: 'rejected-notification-agent',
      name: 'Rejected Notification Agent',
      instructions: 'Test',
      model: createTextStreamModel('unused'),
    });
    new Mastra({ agents: { rejectedNotificationAgent: agent }, storage, logger: false });
    const rejectedAccepted = Promise.reject(new Error('signal rejected'));
    // Attach a no-op catch so the rejection is considered handled and never surfaces as an
    // unhandled rejection; the dispatcher attaches its own awaiting handler in try/catch.
    rejectedAccepted.catch(() => {});
    const sendSignal = vi.spyOn(agentThreadStreamRuntime, 'sendSignal').mockReturnValue({
      accepted: rejectedAccepted,
      signal: createSignal({ type: 'notification', tagName: 'notification', contents: 'Rejected' }),
    } as any);

    try {
      const result = await agent.sendNotificationSignal(
        { source: 'github', kind: 'ci-status', priority: 'medium', summary: 'Rejected notification' },
        { resourceId: 'notification-user', threadId: 'notification-thread' },
      );

      expect(result.accepted).toBeUndefined();
      expect(result.record).toMatchObject({
        status: 'pending',
        deliveryAttempts: 1,
        lastDeliveryError: 'signal rejected',
      });
      expect(result.record.deliveredSignalId).toBeUndefined();
      const stored = await notifications.getNotification({ threadId: 'notification-thread', id: result.record.id });
      expect(stored).toMatchObject({ status: 'pending', deliveryAttempts: 1 });
      expect(stored?.deliveredSignalId).toBeUndefined();
    } finally {
      sendSignal.mockRestore();
    }
  });

  it('batches active high notifications for full delivery and active medium or low notifications for summaries', async () => {
    let releaseFirst!: () => void;
    const firstFinished = new Promise<void>(resolve => {
      releaseFirst = resolve;
    });
    let streamCount = 0;
    const notifications = new InMemoryNotificationsStorage();
    const storage = new MastraCompositeStore({
      id: 'active-priority-notification-storage',
      domains: { notifications },
    });
    const responseText = 'active response';
    const model = new MockLanguageModelV2({
      doStream: async () => {
        streamCount += 1;
        const currentStream = streamCount;
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: new ReadableStream({
            async start(controller) {
              controller.enqueue({ type: 'stream-start', warnings: [] });
              controller.enqueue({ type: 'text-start', id: `text-${currentStream}` });
              controller.enqueue({ type: 'text-delta', id: `text-${currentStream}`, delta: responseText });
              controller.enqueue({ type: 'text-end', id: `text-${currentStream}` });
              if (currentStream === 1) await firstFinished;
              controller.enqueue({
                type: 'finish',
                finishReason: 'stop',
                usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
              });
              controller.close();
            },
          }),
        };
      },
    });
    const agent = new Agent({
      id: 'active-priority-notification-agent',
      name: 'Active Priority Notification Agent',
      instructions: 'Test',
      model,
    });
    new Mastra({ agents: { activePriorityNotificationAgent: agent }, storage, logger: false });
    const subscription = await agent.subscribeToThread({
      threadId: 'active-priority-notification-thread',
      resourceId: 'active-priority-notification-user',
    });

    const stream = await agent.stream('Hello', {
      memory: { thread: 'active-priority-notification-thread', resource: 'active-priority-notification-user' },
    });
    await expect(waitForActiveRun(subscription)).resolves.toBe(stream.runId);

    const high = await agent.sendNotificationSignal(
      { source: 'github', kind: 'ci-status', priority: 'high', summary: 'CI failed' },
      { resourceId: 'active-priority-notification-user', threadId: 'active-priority-notification-thread' },
    );
    const medium = await agent.sendNotificationSignal(
      { source: 'slack', kind: 'mention', priority: 'medium', summary: 'Jane mentioned you' },
      { resourceId: 'active-priority-notification-user', threadId: 'active-priority-notification-thread' },
    );
    const low = await agent.sendNotificationSignal(
      { source: 'calendar', kind: 'event-reminder', priority: 'low', summary: 'Standup starts soon' },
      { resourceId: 'active-priority-notification-user', threadId: 'active-priority-notification-thread' },
    );

    expect(high.signal).toMatchObject({ type: 'notification', tagName: 'notification-summary' });
    expect(high.decision).toMatchObject({ action: 'summarize', reason: 'active-high-summary-then-full' });
    expect(high.record).toMatchObject({
      status: 'pending',
      deliveryReason: 'active-high-summary-then-full',
      summarySignalId: high.signal?.id,
    });
    expect(high.record.summaryAt).toBeUndefined();
    expect(high.record.deliverAt).toBeInstanceOf(Date);
    expect(medium.signal).toMatchObject({ type: 'notification', tagName: 'notification-summary' });
    expect(medium.decision).toMatchObject({ action: 'summarize', reason: 'active-batch-summary' });
    expect(medium.record).toMatchObject({
      status: 'pending',
      deliveryReason: 'active-batch-summary',
      summarySignalId: medium.signal?.id,
    });
    expect(medium.record.summaryAt).toBeUndefined();
    expect(low.signal).toBeUndefined();
    expect(low.decision).toMatchObject({ action: 'summarize', reason: 'active-batch-summary' });
    expect(low.record).toMatchObject({ status: 'pending', deliveryReason: 'active-batch-summary' });
    expect(low.record.summaryAt).toBeInstanceOf(Date);

    releaseFirst();
    // The high-priority summary signal is delivered to the active run, which the
    // agentic loop picks up and processes with an additional model iteration.
    await expect(stream.text).resolves.toBe('active responseactive response');
    expect(streamCount).toBe(2);
    subscription.unsubscribe();
  });

  it('summarizes active high-priority notifications immediately, then delivers full notifications when idle', async () => {
    let releaseFirst!: () => void;
    const firstFinished = new Promise<void>(resolve => {
      releaseFirst = resolve;
    });
    let streamCount = 0;
    const notifications = new InMemoryNotificationsStorage();
    const storage = new MastraCompositeStore({ id: 'high-active-integration-storage', domains: { notifications } });
    const agent = new Agent({
      id: 'high-active-integration-agent',
      name: 'High Active Integration Agent',
      instructions: 'Test',
      model: new MockLanguageModelV2({
        doStream: async () => {
          streamCount += 1;
          const responseText = `response ${streamCount}`;
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: new ReadableStream({
              async start(controller) {
                controller.enqueue({ type: 'stream-start', warnings: [] });
                controller.enqueue({ type: 'text-start', id: 'text-1' });
                controller.enqueue({ type: 'text-delta', id: 'text-1', delta: responseText });
                controller.enqueue({ type: 'text-end', id: 'text-1' });
                if (streamCount === 1) {
                  await firstFinished;
                }
                controller.enqueue({
                  type: 'finish',
                  finishReason: 'stop',
                  usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
                });
                controller.close();
              },
            }),
          };
        },
      }),
    });
    const mastra = new Mastra({ agents: { highActiveIntegrationAgent: agent }, storage, logger: false });
    const subscription = await agent.subscribeToThread({
      threadId: 'high-active-thread',
      resourceId: 'high-active-user',
    });
    const iterator = subscription.stream[Symbol.asyncIterator]();
    const firstRun = readNextRunWithParts(iterator);

    const stream = await agent.stream('Hello', {
      memory: { thread: 'high-active-thread', resource: 'high-active-user' },
    });
    const streamText = stream.text;
    await expect(waitForActiveRun(subscription)).resolves.toBe(stream.runId);

    const result = await agent.sendNotificationSignal(
      { source: 'github', kind: 'ci-status', priority: 'high', summary: 'CI failed on main' },
      { resourceId: 'high-active-user', threadId: 'high-active-thread' },
    );

    expect(result.signal).toMatchObject({ type: 'notification', tagName: 'notification-summary' });
    expect(result.decision).toMatchObject({ action: 'summarize', reason: 'active-high-summary-then-full' });
    expect(result.record).toMatchObject({
      status: 'pending',
      summarySignalId: result.signal?.id,
      deliveryReason: 'active-high-summary-then-full',
    });
    expect(result.record.summaryAt).toBeUndefined();
    expect(result.record.deliverAt).toBeInstanceOf(Date);

    releaseFirst();
    const subscribedSummary = await withTimeout(firstRun, 'Timed out waiting for high-priority summary signal');
    const summaryPart = subscribedSummary.value.parts.find((part: any) => part.type === 'data-signal');
    expect(summaryPart?.data).toMatchObject({
      id: result.signal?.id,
      type: 'notification',
      tagName: 'notification-summary',
      contents: 'github: 1',
      attributes: { pending: 1 },
    });
    // The summary signal is delivered to the active run, triggering an additional model iteration.
    expect(streamCount).toBe(2);
    await expect(
      notifications.getNotification({ threadId: 'high-active-thread', id: result.record.id }),
    ).resolves.toMatchObject({
      status: 'pending',
      summarySignalId: result.signal?.id,
      summaryAt: undefined,
      deliverAt: result.record.deliverAt,
    });

    const deliveryRun = readNextRunWithParts(iterator);
    const dispatchResult = await dispatchDueNotifications({ mastra, storage: notifications, now: new Date() });
    const subscribedDelivery = await withTimeout(deliveryRun, 'Timed out waiting for full high-priority delivery');
    const deliveryPart = subscribedDelivery.value.parts.find((part: any) => part.type === 'data-signal');

    expect(dispatchResult.failed).toEqual([]);
    expect(dispatchResult.signals[0]).toMatchObject({ type: 'notification', tagName: 'notification' });
    expect(deliveryPart?.data).toMatchObject({
      id: dispatchResult.signals[0]?.id,
      type: 'notification',
      tagName: 'notification',
      contents: 'CI failed on main',
      attributes: { source: 'github', kind: 'ci-status', priority: 'high', status: 'delivered' },
    });
    await expect(
      notifications.getNotification({ threadId: 'high-active-thread', id: result.record.id }),
    ).resolves.toMatchObject({
      status: 'delivered',
      deliveredSignalId: dispatchResult.signals[0]?.id,
    });
    await streamText;

    subscription.unsubscribe();
  });

  it('plans due notifications by thread so medium summaries cannot starve high full delivery', async () => {
    let releaseRun!: () => void;
    const runFinished = new Promise<void>(resolve => {
      releaseRun = resolve;
    });
    const notifications = new InMemoryNotificationsStorage();
    const storage = new MastraCompositeStore({ id: 'priority-dispatch-storage', domains: { notifications } });
    const agent = new Agent({
      id: 'priority-dispatch-agent',
      name: 'Priority Dispatch Agent',
      instructions: 'Test',
      model: new MockLanguageModelV2({
        doStream: async () => ({
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: new ReadableStream({
            async start(controller) {
              controller.enqueue({ type: 'stream-start', warnings: [] });
              controller.enqueue({ type: 'text-start', id: 'text-1' });
              controller.enqueue({ type: 'text-delta', id: 'text-1', delta: 'notification response' });
              controller.enqueue({ type: 'text-end', id: 'text-1' });
              await runFinished;
              controller.enqueue({
                type: 'finish',
                finishReason: 'stop',
                usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
              });
              controller.close();
            },
          }),
        }),
      }),
    });
    const mastra = new Mastra({ agents: { priorityDispatchAgent: agent }, storage, logger: false });
    const dueAt = new Date('2026-06-05T22:56:00Z');
    await notifications.createNotification({
      id: 'medium-ci-pending',
      agentId: 'priority-dispatch-agent',
      resourceId: 'priority-dispatch-user',
      threadId: 'priority-dispatch-thread',
      source: 'github',
      kind: 'pull-request-ci-pending',
      priority: 'medium',
      summary: 'CI is still pending',
      summaryAt: dueAt,
      createdAt: new Date('2026-06-05T22:55:00Z'),
    });
    const high = await notifications.createNotification({
      id: 'high-comment',
      agentId: 'priority-dispatch-agent',
      resourceId: 'priority-dispatch-user',
      threadId: 'priority-dispatch-thread',
      source: 'github',
      kind: 'pull-request-activity',
      priority: 'high',
      summary: 'Devin commented',
      deliverAt: dueAt,
      deliveryReason: 'active-high-summary-then-full',
      createdAt: new Date('2026-06-05T22:55:01Z'),
    });
    await notifications.updateNotification({
      id: high.id,
      threadId: high.threadId,
      summarySignalId: 'previous-summary-signal',
    });

    const dispatchResult = await dispatchDueNotifications({ mastra, storage: notifications, now: dueAt });

    expect(dispatchResult.failed).toEqual([]);
    expect(dispatchResult.signals.map(signal => signal.contents)).toEqual(['Devin commented', 'github: 1']);
    await expect(
      notifications.getNotification({ threadId: 'priority-dispatch-thread', id: 'high-comment' }),
    ).resolves.toMatchObject({
      status: 'delivered',
      deliveredSignalId: dispatchResult.signals[0]?.id,
    });
    await expect(
      notifications.getNotification({ threadId: 'priority-dispatch-thread', id: 'medium-ci-pending' }),
    ).resolves.toMatchObject({
      status: 'pending',
      summaryAt: undefined,
      summarySignalId: dispatchResult.signals[1]?.id,
    });

    releaseRun();
    await nextTick();
  });

  it('dispatches medium-priority active summaries through agent subscriptions without marking records delivered', async () => {
    let releaseFirst!: () => void;
    const firstFinished = new Promise<void>(resolve => {
      releaseFirst = resolve;
    });
    let streamCount = 0;
    const notifications = new InMemoryNotificationsStorage();
    const storage = new MastraCompositeStore({ id: 'medium-active-dispatch-storage', domains: { notifications } });
    const agent = new Agent({
      id: 'medium-active-dispatch-agent',
      name: 'Medium Active Dispatch Agent',
      instructions: 'Test',
      model: new MockLanguageModelV2({
        doStream: async () => {
          streamCount += 1;
          const responseText = `medium response ${streamCount}`;
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: new ReadableStream({
              async start(controller) {
                controller.enqueue({ type: 'stream-start', warnings: [] });
                controller.enqueue({ type: 'text-start', id: 'text-1' });
                controller.enqueue({ type: 'text-delta', id: 'text-1', delta: responseText });
                controller.enqueue({ type: 'text-end', id: 'text-1' });
                if (streamCount === 1) await firstFinished;
                controller.enqueue({
                  type: 'finish',
                  finishReason: 'stop',
                  usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
                });
                controller.close();
              },
            }),
          };
        },
      }),
    });
    const mastra = new Mastra({ agents: { mediumActiveDispatchAgent: agent }, storage, logger: false });
    const subscription = await agent.subscribeToThread({
      threadId: 'medium-active-thread',
      resourceId: 'medium-active-user',
    });
    const iterator = subscription.stream[Symbol.asyncIterator]();
    const firstRun = readNextRunWithParts(iterator);

    await notifications.createNotification({
      id: 'medium-active-notification',
      agentId: 'medium-active-dispatch-agent',
      resourceId: 'medium-active-user',
      threadId: 'medium-active-thread',
      source: 'slack',
      kind: 'mention',
      priority: 'medium',
      summary: 'Jane mentioned you',
      summaryAt: new Date('2026-05-30T12:00:00Z'),
    });
    const stream = await agent.stream('Hello', {
      memory: { thread: 'medium-active-thread', resource: 'medium-active-user' },
    });
    const streamText = stream.text;
    await expect(waitForActiveRun(subscription)).resolves.toBe(stream.runId);

    const dispatchResult = await dispatchDueNotifications({
      mastra,
      storage: notifications,
      now: new Date('2026-05-30T12:00:01Z'),
    });
    expect(dispatchResult.failed).toEqual([]);
    expect(dispatchResult.signals[0]).toMatchObject({ type: 'notification', tagName: 'notification-summary' });

    releaseFirst();
    const subscribedSummary = await withTimeout(firstRun, 'Timed out waiting for medium active summary signal');
    const summaryPart = subscribedSummary.value.parts.find((part: any) => part.type === 'data-signal');
    expect(summaryPart?.data).toMatchObject({
      id: dispatchResult.signals[0]?.id,
      type: 'notification',
      tagName: 'notification-summary',
      contents: 'slack: 1',
      attributes: { pending: 1 },
    });
    // The summary signal is delivered to the active run, triggering an additional model iteration.
    expect(streamCount).toBe(2);
    await expect(
      notifications.getNotification({ threadId: 'medium-active-thread', id: 'medium-active-notification' }),
    ).resolves.toMatchObject({
      status: 'pending',
      summaryAt: undefined,
      summarySignalId: dispatchResult.signals[0]?.id,
    });
    await streamText;

    subscription.unsubscribe();
  });

  it('saves and dispatches low-priority idle notification summaries through agent subscriptions without starting a run', async () => {
    let streamCount = 0;
    const pubsub = new AsyncCallbackPubSub();
    const notifications = new InMemoryNotificationsStorage();
    const storage = new MastraCompositeStore({ id: 'low-priority-notification-storage', domains: { notifications } });
    const agent = new Agent({
      id: 'low-priority-notification-agent',
      name: 'Low Priority Notification Agent',
      instructions: 'Test',
      model: new MockLanguageModelV2({
        doStream: async () => {
          streamCount += 1;
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: convertArrayToReadableStream([{ type: 'stream-start', warnings: [] }]),
          };
        },
      }),
    });
    const mastra = new Mastra({ agents: { lowPriorityNotificationAgent: agent }, storage, logger: false, pubsub });
    const subscription = await agent.subscribeToThread({
      threadId: 'notification-thread',
      resourceId: 'notification-user',
    });

    const result = await agent.sendNotificationSignal(
      { source: 'mastracode', kind: 'manual', priority: 'low', summary: 'Read when you have time' },
      { resourceId: 'notification-user', threadId: 'notification-thread' },
    );

    await nextTick();
    expect(streamCount).toBe(0);
    expect(result.signal).toBeUndefined();
    expect(result.accepted).toBeUndefined();
    expect(result).toMatchObject({
      decision: { action: 'summarize', reason: 'idle-low-summary' },
      record: { status: 'pending', deliveryReason: 'idle-low-summary' },
    });
    expect(result.record.deliverAt).toBeUndefined();
    expect(result.record.summaryAt).toBeInstanceOf(Date);

    const dispatchNow = new Date((result.record.summaryAt?.getTime() ?? Date.now()) + 1);
    const nextRun = readNextRunWithParts(subscription.stream[Symbol.asyncIterator]());
    const dispatchResult = await dispatchDueNotifications({ mastra, storage: notifications, now: dispatchNow });
    const subscribedRun = await withTimeout(
      nextRun,
      'Timed out waiting for low-priority notification summary broadcast',
    );

    expect(dispatchResult.failed).toEqual([]);
    expect(dispatchResult.signals[0]).toMatchObject({ type: 'notification', tagName: 'notification-summary' });
    expect(streamCount).toBe(0);
    const signalPart = subscribedRun.value.parts.find((part: any) => part.type === 'data-signal');
    expect(signalPart?.data).toMatchObject({
      id: dispatchResult.signals[0]?.id,
      type: 'notification',
      tagName: 'notification-summary',
      contents: 'mastracode: 1',
      attributes: { pending: 1 },
    });
    await expect(
      notifications.getNotification({ threadId: 'notification-thread', id: result.record.id }),
    ).resolves.toMatchObject({
      status: 'pending',
      summaryAt: undefined,
      summarySignalId: dispatchResult.signals[0]?.id,
    });

    subscription.unsubscribe();
  });

  it('notification inbox read injects a real notification signal through agent subscriptions', async () => {
    let streamCount = 0;
    const notifications = new InMemoryNotificationsStorage();
    const storage = new MastraCompositeStore({ id: 'inbox-read-delivery-storage', domains: { notifications } });
    const agent = new Agent({
      id: 'inbox-read-delivery-agent',
      name: 'Inbox Read Delivery Agent',
      instructions: 'Test',
      model: new MockLanguageModelV2({
        doStream: async () => {
          streamCount += 1;
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: convertArrayToReadableStream([
              { type: 'stream-start', warnings: [] },
              { type: 'finish', finishReason: 'stop', usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 } },
            ]),
          };
        },
      }),
    });
    const mastra = new Mastra({ agents: { inboxReadDeliveryAgent: agent }, storage, logger: false });
    const tool = createNotificationInboxTool({ storage: notifications });
    await notifications.createNotification({
      id: 'inbox-read-notification',
      agentId: 'inbox-read-delivery-agent',
      resourceId: 'inbox-read-user',
      threadId: 'inbox-read-thread',
      source: 'github',
      kind: 'ci-status',
      priority: 'medium',
      summary: 'CI failed on main',
    });
    const subscription = await agent.subscribeToThread({
      threadId: 'inbox-read-thread',
      resourceId: 'inbox-read-user',
    });
    const nextRun = readNextRunWithParts(subscription.stream[Symbol.asyncIterator]());

    const result = await tool.execute?.({ action: 'read', id: 'inbox-read-notification' }, {
      agent: { agentId: 'inbox-read-delivery-agent', threadId: 'inbox-read-thread', resourceId: 'inbox-read-user' },
      mastra,
    } as any);
    const subscribedRun = await withTimeout(nextRun, 'Timed out waiting for inbox read notification delivery');
    const signalPart = subscribedRun.value.parts.find((part: any) => part.type === 'data-signal');

    expect(result).toMatchObject({ message: '1 notification will now be delivered.', delivered: 1 });
    expect(signalPart?.data).toMatchObject({
      type: 'notification',
      tagName: 'notification',
      contents: 'CI failed on main',
      attributes: { source: 'github', kind: 'ci-status', priority: 'medium', status: 'delivered' },
    });
    expect(streamCount).toBe(1);
    await expect(
      notifications.getNotification({ threadId: 'inbox-read-thread', id: 'inbox-read-notification' }),
    ).resolves.toMatchObject({
      status: 'seen',
      deliveredSignalId: signalPart?.data.id,
    });

    subscription.unsubscribe();
  });

  it('notification inbox read marks already-delivered notifications seen without injecting another signal', async () => {
    let streamCount = 0;
    const notifications = new InMemoryNotificationsStorage();
    const storage = new MastraCompositeStore({
      id: 'inbox-read-already-delivered-storage',
      domains: { notifications },
    });
    const agent = new Agent({
      id: 'inbox-read-already-delivered-agent',
      name: 'Inbox Read Already Delivered Agent',
      instructions: 'Test',
      model: new MockLanguageModelV2({
        doStream: async () => {
          streamCount += 1;
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: convertArrayToReadableStream([{ type: 'stream-start', warnings: [] }]),
          };
        },
      }),
    });
    const mastra = new Mastra({ agents: { inboxReadAlreadyDeliveredAgent: agent }, storage, logger: false });
    const tool = createNotificationInboxTool({ storage: notifications });
    await notifications.createNotification({
      id: 'already-delivered-notification',
      agentId: 'inbox-read-already-delivered-agent',
      resourceId: 'already-delivered-user',
      threadId: 'already-delivered-thread',
      source: 'github',
      kind: 'ci-status',
      priority: 'high',
      summary: 'CI failed earlier',
    });
    await notifications.updateNotification({
      threadId: 'already-delivered-thread',
      id: 'already-delivered-notification',
      status: 'delivered',
      deliveredSignalId: 'existing-signal-id',
    });

    const result = await tool.execute?.({ action: 'read', id: 'already-delivered-notification' }, {
      agent: {
        agentId: 'inbox-read-already-delivered-agent',
        threadId: 'already-delivered-thread',
        resourceId: 'already-delivered-user',
      },
      mastra,
    } as any);

    expect(result).toMatchObject({ delivered: 0, markedSeen: 1, message: 'No unread notifications needed delivery.' });
    expect(streamCount).toBe(0);
    await expect(
      notifications.getNotification({ threadId: 'already-delivered-thread', id: 'already-delivered-notification' }),
    ).resolves.toMatchObject({
      status: 'seen',
      deliveredSignalId: 'existing-signal-id',
    });
  });

  it('dispatches low-priority idle notification summaries without subscribers', async () => {
    let streamCount = 0;
    const pubsub = new AsyncCallbackPubSub();
    const notifications = new InMemoryNotificationsStorage();
    const storage = new MastraCompositeStore({ id: 'no-subscriber-notification-storage', domains: { notifications } });
    const agent = new Agent({
      id: 'no-subscriber-notification-agent',
      name: 'No Subscriber Notification Agent',
      instructions: 'Test',
      model: new MockLanguageModelV2({
        doStream: async () => {
          streamCount += 1;
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: convertArrayToReadableStream([{ type: 'stream-start', warnings: [] }]),
          };
        },
      }),
    });
    const mastra = new Mastra({ agents: { noSubscriberNotificationAgent: agent }, storage, logger: false, pubsub });

    const result = await agent.sendNotificationSignal(
      { source: 'mastracode', kind: 'manual', priority: 'low', summary: 'No one is watching' },
      { resourceId: 'notification-user', threadId: 'notification-thread' },
    );
    const dispatchNow = new Date((result.record.summaryAt?.getTime() ?? Date.now()) + 1);
    const dispatchResult = await withTimeout(
      dispatchDueNotifications({ mastra, storage: notifications, now: dispatchNow }),
      'Timed out dispatching low-priority notification summary without subscribers',
    );

    expect(dispatchResult.failed).toEqual([]);
    expect(dispatchResult.signals[0]).toMatchObject({ type: 'notification', tagName: 'notification-summary' });
    expect(streamCount).toBe(0);
    await expect(
      notifications.getNotification({ threadId: 'notification-thread', id: result.record.id }),
    ).resolves.toMatchObject({
      status: 'pending',
      summaryAt: undefined,
      summarySignalId: dispatchResult.signals[0]?.id,
    });
  });

  it('defers notification records without starting an idle run', async () => {
    let streamCount = 0;
    const notifications = new InMemoryNotificationsStorage();
    const storage = new MastraCompositeStore({ id: 'deferred-notification-storage', domains: { notifications } });
    const deliverAt = new Date('2026-05-30T12:00:00Z');
    const agent = new Agent({
      id: 'deferred-notification-agent',
      name: 'Deferred Notification Agent',
      instructions: 'Test',
      model: new MockLanguageModelV2({
        doStream: async () => {
          streamCount += 1;
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: convertArrayToReadableStream([{ type: 'stream-start', warnings: [] }]),
          };
        },
      }),
      notifications: {
        deliveryPolicy: {
          decide: () => ({ action: 'defer', deliverAt, reason: 'after-hours' }),
        },
      },
    });
    new Mastra({ agents: { deferredNotificationAgent: agent }, storage, logger: false });

    const result = await agent.sendNotificationSignal(
      {
        source: 'calendar',
        kind: 'event-reminder',
        summary: 'Planning starts tomorrow',
      },
      { resourceId: 'notification-user', threadId: 'notification-thread' },
    );

    await nextTick();
    expect(streamCount).toBe(0);
    expect(result).toMatchObject({
      decision: { action: 'defer', reason: 'after-hours' },
      record: { status: 'pending', deliveryReason: 'after-hours' },
    });
    expect(result.signal).toBeUndefined();
    expect(result.accepted).toBeUndefined();
    expect(result.record.deliverAt?.toISOString()).toBe(deliverAt.toISOString());
  });

  it('coalesces pending notification records through sendNotificationSignal', async () => {
    const notifications = new InMemoryNotificationsStorage();
    const storage = new MastraCompositeStore({ id: 'coalesced-notification-storage', domains: { notifications } });
    const agent = new Agent({
      id: 'coalesced-notification-agent',
      name: 'Coalesced Notification Agent',
      instructions: 'Test',
      model: createTextStreamModel('notification response'),
      notifications: {
        deliveryPolicy: {
          default: { action: 'summarize', summaryAt: new Date('2026-05-30T12:00:00Z') },
        },
      },
    });
    new Mastra({ agents: { coalescedNotificationAgent: agent }, storage, logger: false });

    const first = await agent.sendNotificationSignal(
      {
        source: 'github',
        kind: 'ci-status',
        summary: 'CI failed: one test',
        dedupeKey: 'main-ci',
      },
      { resourceId: 'notification-user', threadId: 'notification-thread' },
    );
    const second = await agent.sendNotificationSignal(
      {
        source: 'github',
        kind: 'ci-status',
        summary: 'CI failed: three tests',
        dedupeKey: 'main-ci',
      },
      { resourceId: 'notification-user', threadId: 'notification-thread' },
    );

    expect(second.record.id).toBe(first.record.id);
    expect(second.record).toMatchObject({ status: 'pending', summary: 'CI failed: three tests', coalescedCount: 2 });
    await expect(notifications.listNotifications({ threadId: 'notification-thread' })).resolves.toHaveLength(1);
  });

  it('throws a clear error when notification storage is missing', async () => {
    const agent = new Agent({
      id: 'missing-notification-storage-agent',
      name: 'Missing Notification Storage Agent',
      instructions: 'Test',
      model: createTextStreamModel('notification response'),
    });

    await expect(
      agent.sendNotificationSignal(
        { source: 'github', kind: 'ci-status', summary: 'CI failed' },
        { resourceId: 'notification-user', threadId: 'notification-thread' },
      ),
    ).rejects.toThrow('sendNotificationSignal requires a notifications storage domain');
  });

  it('delivers sendMessage into an active same-agent run', async () => {
    let releaseFirst!: () => void;
    const firstFinished = new Promise<void>(resolve => {
      releaseFirst = resolve;
    });
    let streamCount = 0;
    const prompts: any[][] = [];
    const model = new MockLanguageModelV2({
      doStream: async ({ prompt }) => {
        streamCount += 1;
        prompts.push(prompt);
        const responseText = streamCount === 1 ? 'first response' : 'message response';
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: new ReadableStream({
            async start(controller) {
              controller.enqueue({ type: 'stream-start', warnings: [] });
              controller.enqueue({
                type: 'response-metadata',
                id: `send-message-${streamCount}`,
                modelId: 'mock-model-id',
                timestamp: new Date(0),
              });
              controller.enqueue({ type: 'text-start', id: 'text-1' });
              controller.enqueue({ type: 'text-delta', id: 'text-1', delta: responseText });
              controller.enqueue({ type: 'text-end', id: 'text-1' });
              if (streamCount === 1) {
                await firstFinished;
              }
              controller.enqueue({
                type: 'finish',
                finishReason: 'stop',
                usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
              });
              controller.close();
            },
          }),
        };
      },
    });
    const agent = new Agent({ id: 'active-message-agent', name: 'Active Message Agent', instructions: 'Test', model });
    const subscription = await agent.subscribeToThread({
      threadId: 'active-message-thread',
      resourceId: 'active-message-user',
    });

    const stream = await agent.stream('Hello', {
      memory: { thread: 'active-message-thread', resource: 'active-message-user' },
    });
    await expect(waitForActiveRun(subscription)).resolves.toBe(stream.runId);
    const result = agent.sendMessage('Hello while active', {
      resourceId: 'active-message-user',
      threadId: 'active-message-thread',
    });

    await expect(result.accepted).resolves.toMatchObject({ action: 'deliver', runId: stream.runId });
    releaseFirst();
    await expect(stream.text).resolves.toBe('first responsemessage response');
    expect(streamCount).toBe(2);
    expect(JSON.stringify(prompts[1])).toContain('Hello while active');

    subscription.unsubscribe();
  });

  it('queues sendMessage behind a suspended same-agent approval run', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const agent = {
      id: 'suspended-message-agent',
      stream: vi.fn(),
    } as unknown as Agent<any, any, any, any>;
    const runId = 'suspended-message-run';
    const threadId = 'suspended-message-thread';
    const resourceId = 'suspended-message-user';
    let finishRun!: () => void;
    const finished = new Promise<void>(resolve => {
      finishRun = resolve;
    });
    const subscription = await runtime.subscribeToThread(agent, { threadId, resourceId });
    const iterator = subscription.stream[Symbol.asyncIterator]();

    try {
      runtime.registerRun(
        agent,
        {
          runId,
          status: 'suspended',
          fullStream: new ReadableStream({
            start(controller) {
              controller.enqueue({ type: 'start', runId });
              controller.enqueue({
                type: 'tool-call-approval',
                runId,
                payload: { toolCallId: 'tool-call-1', toolName: 'testTool' },
              });
            },
          }),
          _waitUntilFinished: () => finished,
        } as any,
        { memory: { thread: threadId, resource: resourceId } } as any,
      );

      await withTimeout(iterator.next(), 'Timed out waiting for approval run start');
      await withTimeout(iterator.next(), 'Timed out waiting for approval chunk');
      expect(runtime.getThreadState({ resourceId, threadId })).toBe('active');

      const result = runtime.sendMessage(agent, 'Queued behind approval', { resourceId, threadId });

      await expect(result.accepted).resolves.toMatchObject({ action: 'deliver', runId });
      expect((agent as any).stream).not.toHaveBeenCalled();
      const [signal] = runtime.drainPendingSignals(runId);
      expect(signal).toMatchObject({ type: 'user', contents: 'Queued behind approval' });
    } finally {
      finishRun();
      subscription.unsubscribe();
    }
  });

  it('re-registers an approval-suspended run on resume without rejecting the retained record', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const agent = { id: 'approval-resume-agent' } as Agent<any, any, any, any>;
    const threadId = 'approval-resume-thread';
    const resourceId = 'approval-resume-user';
    const runId = 'approval-resume-run';

    const subscription = await runtime.subscribeToThread(agent, { threadId, resourceId });
    const iterator = subscription.stream[Symbol.asyncIterator]();

    try {
      // Register the suspended run that emits a tool-call-approval part. The
      // completion finalizer surfaces run-suspended and retains its records so
      // the thread stays blocked awaiting approval.
      let finishSuspended!: () => void;
      const suspendedFinished = new Promise<void>(resolve => {
        finishSuspended = resolve;
      });
      const suspendedCompletion = runtime.registerRun(
        agent,
        {
          runId,
          status: 'suspended',
          fullStream: new ReadableStream({
            start(controller) {
              controller.enqueue({ type: 'start', runId });
              controller.enqueue({
                type: 'tool-call-approval',
                runId,
                payload: { toolCallId: 'tool-call-1', toolName: 'testTool' },
              });
              controller.close();
            },
          }),
          _waitUntilFinished: () => suspendedFinished,
        } as any,
        { memory: { thread: threadId, resource: resourceId } } as any,
      );

      await withTimeout(iterator.next(), 'Timed out waiting for approval run start');
      await withTimeout(iterator.next(), 'Timed out waiting for approval chunk');
      // Drive and await the finalizer so run-suspended has propagated to the
      // subscriber; the approval marker must keep the thread blocked (active).
      finishSuspended();
      await withTimeout(suspendedCompletion ?? Promise.resolve(), 'Timed out waiting for approval-suspended finalizer');
      await nextTick();
      expect(runtime.getThreadState({ resourceId, threadId })).toBe('active');

      // The resume reuses the same runId. Without the fix this throws
      // "already registered" / "already reserved" and the resume wedges.
      const resumedRun = readNextRun(iterator);
      let finishResumed!: () => void;
      const resumedFinished = new Promise<void>(resolve => {
        finishResumed = resolve;
      });
      expect(() =>
        runtime.registerRun(
          agent,
          {
            runId,
            status: 'running',
            fullStream: new ReadableStream({
              start(controller) {
                setTimeout(() => {
                  controller.enqueue({ type: 'start', runId });
                  controller.enqueue({ type: 'text-start', runId, payload: { id: 'text-1' } });
                  controller.enqueue({
                    type: 'text-delta',
                    runId,
                    payload: { id: 'text-1', text: 'approved response' },
                  });
                  controller.enqueue({ type: 'text-end', runId, payload: { id: 'text-1' } });
                  controller.enqueue({
                    type: 'finish',
                    runId,
                    payload: { usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 }, finishReason: 'stop' },
                  });
                  controller.close();
                  finishResumed();
                }, 5);
              },
            }),
            _waitUntilFinished: () => resumedFinished,
          } as any,
          { memory: { thread: threadId, resource: resourceId } } as any,
        ),
      ).not.toThrow();

      await expect(withTimeout(resumedRun, 'Timed out waiting for resumed run')).resolves.toMatchObject({
        value: { runId, text: 'approved response' },
      });
    } finally {
      subscription.unsubscribe();
    }
  });

  it('lets the same agent reserve a fresh turn while retaining a suspended run for explicit resume', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new EventEmitterPubSub();
    const agent = { id: 'suspended-fresh-turn-agent' } as Agent<any, any, any, any>;
    const threadId = 'suspended-fresh-turn-thread';
    const resourceId = 'suspended-fresh-turn-user';
    const suspendedRunId = 'suspended-fresh-turn-old-run';

    const suspendedCompletion = runtime.registerRun(
      agent,
      {
        runId: suspendedRunId,
        status: 'suspended',
        fullStream: new ReadableStream({
          start(controller) {
            controller.enqueue({ type: 'start', runId: suspendedRunId });
            controller.enqueue({
              type: 'tool-call-approval',
              runId: suspendedRunId,
              payload: { toolCallId: 'fresh-turn-tool-call', toolName: 'freshTurnTool' },
            });
            controller.close();
          },
        }),
        _waitUntilFinished: () => Promise.resolve(),
      } as any,
      {
        runId: suspendedRunId,
        memory: { thread: threadId, resource: resourceId },
      } as any,
      pubsub,
    );
    await withTimeout(suspendedCompletion ?? Promise.resolve(), 'Timed out finalizing retained suspended run');

    expect(runtime.getActiveThreadRunId({ resourceId, threadId }, pubsub)).toBe(suspendedRunId);

    const freshOptions = {
      runId: 'suspended-fresh-turn-new-run',
      memory: { thread: threadId, resource: resourceId },
    } as any;
    await runtime.waitForThreadRunReservation(freshOptions, pubsub, agent.id);
    const releaseFreshReservation = runtime.reserveRun(freshOptions, pubsub, agent.id);

    expect(releaseFreshReservation).toBeDefined();
    expect(runtime.getActiveThreadRunId({ resourceId, threadId }, pubsub)).toBe(freshOptions.runId);
    expect(runtime.getRunOutput(suspendedRunId, pubsub)).toBeDefined();
    expect(
      runtime.getResumableThreadRun(
        { resourceId, threadId, runId: suspendedRunId, toolCallId: 'fresh-turn-tool-call' },
        pubsub,
      ),
    ).toEqual({ runId: suspendedRunId, toolCallId: 'fresh-turn-tool-call' });

    releaseFreshReservation?.();
  });

  it.each(['request_access', 'ask_user'])('keeps %s suspensions discoverable and blocks idle wake', async toolName => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new EventEmitterPubSub();
    const agent = {
      id: `generic-suspended-${toolName}`,
      stream: vi.fn(),
    } as unknown as Agent<any, any, any, any>;
    const idleAgent = {
      id: `idle-agent-${toolName}`,
      stream: vi.fn(),
    } as unknown as Agent<any, any, any, any>;
    const runId = `generic-suspended-run-${toolName}`;
    const threadId = `generic-suspended-thread-${toolName}`;
    const resourceId = `generic-suspended-user-${toolName}`;
    const topic = `agent.thread-stream.${encodeURIComponent(`${resourceId}\u0000${threadId}`)}`;
    const events: any[] = [];
    let finishRun!: () => void;
    const finished = new Promise<void>(resolve => {
      finishRun = resolve;
    });
    await pubsub.subscribe(topic, event => events.push(event.data));
    const subscription = await runtime.subscribeToThread(agent, { threadId, resourceId }, pubsub);
    const iterator = subscription.stream[Symbol.asyncIterator]();

    try {
      runtime.registerRun(
        agent,
        {
          runId,
          status: 'suspended',
          fullStream: new ReadableStream({
            start(controller) {
              controller.enqueue({ type: 'start', runId });
              controller.enqueue({
                type: 'tool-call-suspended',
                runId,
                payload: { toolCallId: `tool-call-${toolName}`, toolName },
              });
              controller.close();
            },
          }),
          _waitUntilFinished: () => finished,
        } as any,
        { memory: { thread: threadId, resource: resourceId } } as any,
        pubsub,
      );

      await withTimeout(iterator.next(), 'Timed out waiting for generic suspended run start');
      await withTimeout(iterator.next(), 'Timed out waiting for generic suspended chunk');
      expect(runtime.getThreadState({ resourceId, threadId }, pubsub)).toBe('active');
      expect(runtime.getActiveThreadRunId({ resourceId, threadId }, pubsub)).toBe(runId);

      const queuedForSuspendedRun = runtime.sendMessage(
        agent,
        'Resume-adjacent input',
        { resourceId, threadId },
        pubsub,
      );
      await expect(queuedForSuspendedRun.accepted).resolves.toMatchObject({ action: 'deliver', runId });
      expect((agent as any).stream).not.toHaveBeenCalled();
      expect(runtime.drainPendingSignals(runId, pubsub)[0]).toMatchObject({
        type: 'user',
        contents: 'Resume-adjacent input',
      });

      finishRun();
      await waitForCondition(() => events.some(event => event?.type === 'run-suspended' && event.runId === runId));
      expect(runtime.getThreadState({ resourceId, threadId }, pubsub)).toBe('active');
      expect(runtime.getActiveThreadRunId({ resourceId, threadId }, pubsub)).toBe(runId);

      const idleWake = runtime.sendSignal(
        idleAgent,
        createSignal({ type: 'user-message', contents: 'Unrelated idle wake' }),
        { resourceId, threadId, ifIdle: { streamOptions: { memory: { resource: resourceId, thread: threadId } } } },
        pubsub,
      );
      await expect(idleWake.accepted).resolves.toMatchObject({ action: 'blocked', reason: 'thread-blocked', runId });
      expect((idleAgent as any).stream).not.toHaveBeenCalled();
      expect(runtime.getThreadState({ resourceId, threadId }, pubsub)).toBe('active');
    } finally {
      finishRun();
      subscription.unsubscribe();
    }
  });

  it('marks a caller-only approval-suspended run without any thread subscribers', async () => {
    // Regression: the local multicast drain detects the approval marker. With no
    // thread subscribers nothing pulls a subscriber stream, so the drain must be
    // kicked eagerly at registration — otherwise #markApprovalSuspendedFromPart
    // never runs and the finalizer wrongly treats the suspended run as completed,
    // clearing its record. Here there is NO subscribeToThread caller at all.
    const runtime = new AgentThreadStreamRuntime();
    const agent = { id: 'caller-only-approval-agent' } as Agent<any, any, any, any>;
    const threadId = 'caller-only-approval-thread';
    const resourceId = 'caller-only-approval-user';
    const runId = 'caller-only-approval-run';

    let finishSuspended!: () => void;
    const suspendedFinished = new Promise<void>(resolve => {
      finishSuspended = resolve;
    });
    const suspendedCompletion = runtime.registerRun(
      agent,
      {
        runId,
        status: 'suspended',
        fullStream: new ReadableStream({
          start(controller) {
            controller.enqueue({ type: 'start', runId });
            controller.enqueue({
              type: 'tool-call-approval',
              runId,
              payload: { toolCallId: 'tool-call-1', toolName: 'testTool' },
            });
            controller.close();
          },
        }),
        _waitUntilFinished: () => suspendedFinished,
      } as any,
      { memory: { thread: threadId, resource: resourceId } } as any,
    );

    // Drive and await the finalizer with no subscriber ever attached.
    finishSuspended();
    await withTimeout(
      suspendedCompletion ?? Promise.resolve(),
      'Timed out waiting for caller-only approval-suspended finalizer',
    );
    await nextTick();

    // The approval marker must have been set by the eager drain: the thread stays
    // blocked (active) and the run record is retained rather than finalized-as-
    // completed and deleted.
    expect(runtime.getThreadState({ resourceId, threadId })).toBe('active');
    expect(runtime.getRunOutput(runId)).toBeDefined();

    // A subsequent resume reuses the same runId and must re-register cleanly
    // (the retained record is dropped via #clearApprovalSuspendedRunForResume).
    let finishResumed!: () => void;
    const resumedFinished = new Promise<void>(resolve => {
      finishResumed = resolve;
    });
    const resumedCompletion = runtime.registerRun(
      agent,
      {
        runId,
        status: 'running',
        fullStream: new ReadableStream({
          start(controller) {
            controller.enqueue({ type: 'start', runId });
            controller.enqueue({ type: 'text-start', runId, payload: { id: 'text-1' } });
            controller.enqueue({
              type: 'text-delta',
              runId,
              payload: { id: 'text-1', text: 'approved response' },
            });
            controller.enqueue({ type: 'text-end', runId, payload: { id: 'text-1' } });
            controller.enqueue({
              type: 'finish',
              runId,
              payload: { usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 }, finishReason: 'stop' },
            });
            controller.close();
            finishResumed();
          },
        }),
        _waitUntilFinished: () => resumedFinished,
      } as any,
      { memory: { thread: threadId, resource: resourceId } } as any,
    );

    await withTimeout(resumedCompletion ?? Promise.resolve(), 'Timed out waiting for resumed run completion');
    await nextTick();

    // The resumed run completed normally: the thread is idle again and the record
    // is cleared.
    expect(runtime.getThreadState({ resourceId, threadId })).toBe('idle');
    expect(runtime.getRunOutput(runId)).toBeUndefined();
  });

  it('queues queueMessage until the active run completes', async () => {
    let releaseFirst!: () => void;
    const firstFinished = new Promise<void>(resolve => {
      releaseFirst = resolve;
    });
    let streamCount = 0;
    const prompts: any[][] = [];
    const model = new MockLanguageModelV2({
      doStream: async ({ prompt }) => {
        streamCount += 1;
        prompts.push(prompt);
        const responseText = streamCount === 1 ? 'first response' : 'queued response';
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: new ReadableStream({
            async start(controller) {
              controller.enqueue({ type: 'stream-start', warnings: [] });
              controller.enqueue({
                type: 'response-metadata',
                id: `queue-message-${streamCount}`,
                modelId: 'mock-model-id',
                timestamp: new Date(0),
              });
              controller.enqueue({ type: 'text-start', id: 'text-1' });
              controller.enqueue({ type: 'text-delta', id: 'text-1', delta: responseText });
              controller.enqueue({ type: 'text-end', id: 'text-1' });
              if (streamCount === 1) {
                await firstFinished;
              }
              controller.enqueue({
                type: 'finish',
                finishReason: 'stop',
                usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
              });
              controller.close();
            },
          }),
        };
      },
    });
    const agent = new Agent({ id: 'queue-message-agent', name: 'Queue Message Agent', instructions: 'Test', model });
    const subscription = await agent.subscribeToThread({
      threadId: 'queue-message-thread',
      resourceId: 'queue-message-user',
    });
    const iterator = subscription.stream[Symbol.asyncIterator]();
    const firstRun = readNextRunWithParts(iterator);

    const stream = await agent.stream('Hello', {
      memory: { thread: 'queue-message-thread', resource: 'queue-message-user' },
    });
    await expect(waitForActiveRun(subscription)).resolves.toBe(stream.runId);
    const result = agent.queueMessage('Queued follow-up', {
      resourceId: 'queue-message-user',
      threadId: 'queue-message-thread',
    });

    const settled = await result.accepted;
    const queuedRunId = 'runId' in settled ? settled.runId : undefined;
    expect(settled.action).toBe('deliver');
    expect(queuedRunId).not.toBe(stream.runId);
    await nextTick();
    expect(streamCount).toBe(1);

    releaseFirst();
    await expect(stream.text).resolves.toBe('first response');
    await firstRun;
    const secondRun = await readNextRunWithParts(iterator);
    expect(secondRun.value.runId).toBe(queuedRunId);
    expect(secondRun.value.text).toBe('queued response');
    expect(streamCount).toBe(2);
    expect(JSON.stringify(prompts[1])).toContain('Queued follow-up');

    subscription.unsubscribe();
  });

  it('fans out sequential idle signal runs to many same-thread subscribers', async () => {
    const resourceId = 'share-resource';
    const threadId = 'share-thread';
    const subscriberCount = 100;
    const runCount = 5;
    let streamCount = 0;
    const agent = new Agent({
      id: 'share-signal-agent',
      name: 'Share Signal Agent',
      instructions: 'Test',
      model: new MockLanguageModelV2({
        doStream: async () => {
          streamCount += 1;
          const text = `signal response ${streamCount}`;
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: new ReadableStream({
              async start(controller) {
                const parts = [
                  { type: 'stream-start', warnings: [] },
                  {
                    type: 'response-metadata',
                    id: `id-${streamCount}`,
                    modelId: 'mock-model-id',
                    timestamp: new Date(0),
                  },
                  { type: 'text-start', id: 'text-1' },
                  { type: 'text-delta', id: 'text-1', delta: text },
                  { type: 'text-end', id: 'text-1' },
                  {
                    type: 'finish',
                    finishReason: 'stop',
                    usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
                  },
                ] as any[];
                for (const part of parts) {
                  await nextTick();
                  controller.enqueue(part);
                }
                controller.close();
              },
            }),
          };
        },
      }),
    });

    const subscriptions = await Promise.all(
      Array.from({ length: subscriberCount }, () => agent.subscribeToThread({ threadId, resourceId })),
    );
    const iterators = subscriptions.map(subscription => subscription.stream[Symbol.asyncIterator]());

    try {
      for (let runIndex = 1; runIndex <= runCount; runIndex += 1) {
        const nextRuns = iterators.map(iterator => readNextRunWithParts(iterator));
        const contents = `Hello from signal ${runIndex}`;

        const signalResult = await agent.sendSignal(
          { type: 'user-message', contents },
          { resourceId, threadId, ifIdle: { streamOptions: { memory: { resource: resourceId, thread: threadId } } } },
        );

        const runs = await withTimeout(
          Promise.all(nextRuns),
          `Timed out waiting for ${subscriberCount} subscribers to receive idle signal run ${runIndex}`,
        );
        const [firstRun] = runs;

        await expect(signalResult.accepted).resolves.toMatchObject({ action: 'wake', runId: firstRun.value.runId });
        expect(firstRun.value.text).toBe(`signal response ${runIndex}`);

        for (const run of runs) {
          expect(run.value.runId).toBe(firstRun.value.runId);
          expect(run.value.text).toBe(`signal response ${runIndex}`);
          const signalPart = run.value.parts.find((part: any) => part.type === 'data-user-message');
          expect(signalPart?.data).toMatchObject({
            id: signalResult.signal.id,
            contents,
            acceptedAt: signalResult.signal.acceptedAt?.toISOString(),
          });
          expect(signalPart?.data.createdAt).toBeDefined();
          expect(signalPart?.transient).toBe(true);
        }
      }

      expect(streamCount).toBe(runCount);
    } finally {
      for (const subscription of subscriptions) {
        subscription.unsubscribe();
      }
    }
  });

  it('starts an idle thread run by default when a thread-targeted signal is sent', async () => {
    const agent = new Agent({
      id: 'idle-signal-without-options-agent',
      name: 'Idle Signal Without Options Agent',
      instructions: 'Test',
      model: createTextStreamModel('signal response'),
    });

    const result = await agent.sendSignal(
      { type: 'user-message', contents: 'Hello from signal' },
      { resourceId: 'idle-user', threadId: 'idle-thread' },
    );

    await expect(result.accepted).resolves.toMatchObject({ action: 'wake' });
  });

  it('reports the reserved runId as active before registerRun populates the stream record', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const threadId = 'reservation-gap-thread';
    const resourceId = 'reservation-gap-user';

    // agent.stream is awaited inside the idle-wake path before registerRun fires. Returning
    // a never-resolving promise pins the runtime in the gap where sendSignal has reserved
    // activeThreadRunIds + threadKeysByRunId but threadRunsById is still empty.
    const agent = {
      id: 'reservation-gap-agent',
      stream: () => new Promise(() => {}),
    } as unknown as Agent<any, any, any, any>;

    const subscription = await runtime.subscribeToThread(agent, { threadId, resourceId });
    expect(subscription.activeRunId()).toBeNull();

    const result = runtime.sendSignal(agent, createSignal({ type: 'user-message', contents: 'hello' }), {
      resourceId,
      threadId,
      ifIdle: { streamOptions: { memory: { resource: resourceId, thread: threadId } } as any },
    });

    // accepted never settles here because agent.stream is pinned; the reserved runId is
    // observable via the subscription's active run id before registerRun populates the stream.
    expect(result.accepted).toBeInstanceOf(Promise);
    expect(subscription.activeRunId()).not.toBeNull();

    subscription.unsubscribe();
  });

  it('persists an idle signal without waking the agent when idle behavior is persist', async () => {
    let streamCount = 0;
    const memory = new MockMemory();
    await memory.createThread({ threadId: 'idle-persist-thread', resourceId: 'idle-persist-user' });
    const agent = new Agent({
      id: 'idle-persist-agent',
      name: 'Idle Persist Agent',
      instructions: 'Test',
      model: new MockLanguageModelV2({
        doStream: async () => {
          streamCount += 1;
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: convertArrayToReadableStream([{ type: 'stream-start', warnings: [] }]),
          };
        },
      }),
      memory,
    });

    const subscription = await agent.subscribeToThread({
      resourceId: 'idle-persist-user',
      threadId: 'idle-persist-thread',
    });
    const nextRun = readNextRunWithParts(subscription.stream[Symbol.asyncIterator]());

    try {
      const result = agent.sendSignal(
        { type: 'user-message', contents: 'persist without waking' },
        { resourceId: 'idle-persist-user', threadId: 'idle-persist-thread', ifIdle: { behavior: 'persist' } },
      );
      await expect(result.persisted).resolves.toBeUndefined();

      const subscribedRun = await withTimeout(nextRun, 'Timed out waiting for persisted signal broadcast');
      expect(subscribedRun.value.parts).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            type: 'data-user-message',
            data: expect.objectContaining({ contents: 'persist without waking' }),
          }),
        ]),
      );

      const recalled = await memory.recall({ threadId: 'idle-persist-thread', resourceId: 'idle-persist-user' });
      expect(streamCount).toBe(0);
      expect(recalled.messages).toHaveLength(1);
      // Stash dropped; payload lives in content.parts now.
      expect(recalled.messages[0]?.content.metadata?.signal).toMatchObject({ type: 'user', tagName: 'user' });
      expect(recalled.messages[0]?.content.parts).toEqual(
        expect.arrayContaining([expect.objectContaining({ type: 'text', text: 'persist without waking' })]),
      );
    } finally {
      subscription.unsubscribe();
    }
  });

  it('discards an active signal when active behavior is discard', async () => {
    let releaseFirst!: () => void;
    const firstFinished = new Promise<void>(resolve => {
      releaseFirst = resolve;
    });
    let streamCount = 0;
    const prompts: any[][] = [];

    const agent = new Agent({
      id: 'active-discard-agent',
      name: 'Active Discard Agent',
      instructions: 'Test',
      model: new MockLanguageModelV2({
        doStream: async ({ prompt }) => {
          streamCount += 1;
          prompts.push(prompt);
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: new ReadableStream({
              async start(controller) {
                controller.enqueue({ type: 'stream-start', warnings: [] });
                controller.enqueue({
                  type: 'response-metadata',
                  id: `discard-${streamCount}`,
                  modelId: 'mock-model-id',
                  timestamp: new Date(0),
                });
                controller.enqueue({ type: 'text-start', id: 'text-1' });
                controller.enqueue({ type: 'text-delta', id: 'text-1', delta: 'first response' });
                controller.enqueue({ type: 'text-end', id: 'text-1' });
                if (streamCount === 1) {
                  await firstFinished;
                }
                controller.enqueue({
                  type: 'finish',
                  finishReason: 'stop',
                  usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
                });
                controller.close();
              },
            }),
          };
        },
      }),
    });

    const stream = await agent.stream('Hello', {
      memory: { thread: 'active-discard-thread', resource: 'active-discard-user' },
    });
    await agent.sendSignal(
      { type: 'user-message', contents: 'discard while running' },
      { resourceId: 'active-discard-user', threadId: 'active-discard-thread', ifActive: { behavior: 'discard' } },
    );

    releaseFirst();
    await expect(stream.text).resolves.toBe('first response');
    expect(streamCount).toBe(1);
    expect(JSON.stringify(prompts)).not.toContain('discard while running');
  });

  it('routes active-run signals across runtime instances through PubSub', async () => {
    const pubsub = new EventEmitterPubSub();
    const ownerRuntime = new AgentThreadStreamRuntime();
    const senderRuntime = new AgentThreadStreamRuntime();
    const owner = new Agent({
      id: 'remote-signal-agent',
      name: 'Remote Signal Owner Agent',
      instructions: 'Test',
      model: createTextStreamModel('owner response'),
    });
    const sender = new Agent({
      id: 'remote-signal-agent',
      name: 'Remote Signal Sender Agent',
      instructions: 'Test',
      model: createTextStreamModel('sender response'),
    });
    let finishRun!: () => void;
    const output = {
      runId: 'remote-run-1',
      status: 'running',
      fullStream: (async function* () {})(),
      _waitUntilFinished: () => new Promise<void>(resolve => (finishRun = resolve)),
    } as any;

    const ownerSubscription = await ownerRuntime.subscribeToThread(
      owner,
      {
        resourceId: 'remote-resource',
        threadId: 'remote-thread',
      },
      pubsub,
    );
    const senderSubscription = await senderRuntime.subscribeToThread(
      sender,
      {
        resourceId: 'remote-resource',
        threadId: 'remote-thread',
      },
      pubsub,
    );

    await pubsub.acquireLease('remote-resource\u0000remote-thread', 'remote-run-1', 15000);
    ownerRuntime.registerRun(
      owner,
      output,
      { runId: 'remote-run-1', memory: { resource: 'remote-resource', thread: 'remote-thread' } } as any,
      pubsub,
    );
    await waitForCondition(() => senderSubscription.activeRunId() === 'remote-run-1');

    let waitResolved = false;
    const waitForRemoteRun = senderRuntime
      .waitForCrossAgentThreadRun(
        new Agent({
          id: 'remote-other-agent',
          name: 'Remote Other Agent',
          instructions: 'Test',
          model: createTextStreamModel('other response'),
        }),
        { memory: { resource: 'remote-resource', thread: 'remote-thread' } },
        pubsub,
      )
      .then(() => {
        waitResolved = true;
      });
    await nextTick();
    expect(waitResolved).toBe(false);

    const result = senderRuntime.sendSignal(
      sender,
      { type: 'user-message', contents: 'remote follow-up' },
      { resourceId: 'remote-resource', threadId: 'remote-thread' },
      pubsub,
    );

    await expect(result.accepted).resolves.toMatchObject({ action: 'deliver' });
    await waitForCondition(() => ownerRuntime.drainPendingSignals('remote-run-1', pubsub).length === 1);

    finishRun();
    await waitForRemoteRun;
    await pubsub.releaseLease('remote-resource\u0000remote-thread', 'remote-run-1');
    ownerSubscription.unsubscribe();
    senderSubscription.unsubscribe();
  });

  it('wakes a new run instead of delivering to a stale remote active run id', async () => {
    const pubsub = new EventEmitterPubSub();
    const ownerRuntime = new AgentThreadStreamRuntime();
    const senderRuntime = new AgentThreadStreamRuntime();
    const owner = new Agent({
      id: 'stale-remote-signal-agent',
      name: 'Stale Remote Signal Owner Agent',
      instructions: 'Test',
      model: createTextStreamModel('owner response'),
    });
    const sender = new Agent({
      id: 'stale-remote-signal-agent',
      name: 'Stale Remote Signal Sender Agent',
      instructions: 'Test',
      model: createTextStreamModel('sender response'),
    });
    let finishRun!: () => void;
    const output = {
      runId: 'stale-remote-run-1',
      status: 'running',
      fullStream: (async function* () {})(),
      _waitUntilFinished: () => new Promise<void>(resolve => (finishRun = resolve)),
    } as any;

    const senderSubscription = await senderRuntime.subscribeToThread(
      sender,
      {
        resourceId: 'stale-remote-resource',
        threadId: 'stale-remote-thread',
      },
      pubsub,
    );
    await pubsub.acquireLease('stale-remote-resource\u0000stale-remote-thread', 'stale-remote-run-1', 15000);
    ownerRuntime.registerRun(
      owner,
      output,
      {
        runId: 'stale-remote-run-1',
        memory: { resource: 'stale-remote-resource', thread: 'stale-remote-thread' },
      } as any,
      pubsub,
    );
    await waitForCondition(() => senderSubscription.activeRunId() === 'stale-remote-run-1');

    senderSubscription.unsubscribe();
    finishRun();
    await nextTick();
    await pubsub.releaseLease('stale-remote-resource\u0000stale-remote-thread', 'stale-remote-run-1');

    const result = senderRuntime.sendSignal(
      sender,
      { type: 'user-message', contents: 'stale remote follow-up' },
      { resourceId: 'stale-remote-resource', threadId: 'stale-remote-thread' },
      pubsub,
    );

    await expect(result.accepted).resolves.toMatchObject({ action: 'wake' });
    await expect(result.accepted).resolves.not.toMatchObject({ runId: 'stale-remote-run-1' });
  });

  it('grants the wake output to exactly one runtime when two race to wake an idle thread', async () => {
    const pubsub = new EventEmitterPubSub();
    const runtimeA = new AgentThreadStreamRuntime();
    const runtimeB = new AgentThreadStreamRuntime();

    // Track which agents had their .stream invoked. Only the lease winner
    // should actually call .stream(); the loser must short-circuit.
    const streamCallsA: number[] = [];
    const streamCallsB: number[] = [];

    const makeStubAgent = (id: string, calls: number[]) => {
      let nextRunId = 0;
      return {
        id,
        stream: async () => {
          const runId = `${id}-run-${++nextRunId}`;
          calls.push(nextRunId);
          return {
            runId,
            status: 'running',
            fullStream: (async function* () {})(),
            _waitUntilFinished: () => new Promise<void>(() => {}),
          } as any;
        },
      } as any;
    };

    const agentA = makeStubAgent('race-agent-a', streamCallsA);
    const agentB = makeStubAgent('race-agent-b', streamCallsB);

    const target = {
      resourceId: 'race-resource',
      threadId: 'race-thread',
      ifIdle: {
        behavior: 'wake' as const,
        streamOptions: { memory: { resource: 'race-resource', thread: 'race-thread' } },
      },
    };

    // Fire both signals in the same microtask burst so the lease race is real.
    const resultA = runtimeA.sendSignal(agentA, { type: 'user-message', contents: 'from A' }, target, pubsub);
    const resultB = runtimeB.sendSignal(agentB, { type: 'user-message', contents: 'from B' }, target, pubsub);

    expect(resultA.accepted).toBeInstanceOf(Promise);
    expect(resultB.accepted).toBeInstanceOf(Promise);

    const [settledA, settledB] = await Promise.all([resultA.accepted, resultB.accepted]);

    // Exactly one runtime won the lease and ran the stream (`wake` + owned output); the
    // loser forwarded its signal to the winner and resolves to `deliver`.
    const ownerA = settledA.action === 'wake' ? settledA.output : undefined;
    const ownerB = settledB.action === 'wake' ? settledB.output : undefined;
    const winners = [ownerA, ownerB].filter(s => s !== undefined);
    expect(winners).toHaveLength(1);

    const actions = [settledA.action, settledB.action].sort();
    expect(actions).toEqual(['deliver', 'wake']);

    // Only the winner's agent.stream was invoked.
    const totalStreamCalls = streamCallsA.length + streamCallsB.length;
    expect(totalStreamCalls).toBe(1);
  });

  it.runIf(process.platform !== 'win32')(
    'broadcasts subscribed thread stream parts across UnixSocketPubSub runtime instances',
    async () => {
      const tempDir = await mkdtemp(join(tmpdir(), 'mastra-agent-signals-'));
      const ownerPubSub = new UnixSocketPubSub(join(tempDir, 'signals.sock'));
      const followerPubSub = new UnixSocketPubSub(join(tempDir, 'signals.sock'));
      const ownerRuntime = new AgentThreadStreamRuntime();
      const followerRuntime = new AgentThreadStreamRuntime();
      const owner = new Agent({
        id: 'unix-stream-agent',
        name: 'Unix Stream Owner Agent',
        instructions: 'Test',
        model: createTextStreamModel('owner response'),
      });
      const follower = new Agent({
        id: 'unix-stream-agent',
        name: 'Unix Stream Follower Agent',
        instructions: 'Test',
        model: createTextStreamModel('follower response'),
      });
      let finishRun!: () => void;
      const output = {
        runId: 'unix-run-1',
        status: 'running',
        fullStream: (async function* () {
          yield { type: 'text-delta', runId: 'unix-run-1', payload: { text: 'hello over uds' } };
          yield { type: 'finish', runId: 'unix-run-1', payload: {} };
        })(),
        _waitUntilFinished: () => new Promise<void>(resolve => (finishRun = resolve)),
      } as any;

      try {
        const ownerSubscription = await ownerRuntime.subscribeToThread(
          owner,
          { resourceId: 'unix-resource', threadId: 'unix-thread' },
          ownerPubSub,
        );
        const followerSubscription = await followerRuntime.subscribeToThread(
          follower,
          { resourceId: 'unix-resource', threadId: 'unix-thread' },
          followerPubSub,
        );
        const ownerRun = readNextRunWithParts(ownerSubscription.stream[Symbol.asyncIterator]());
        const followerRun = readNextRunWithParts(followerSubscription.stream[Symbol.asyncIterator]());

        ownerRuntime.registerRun(
          owner,
          output,
          { runId: 'unix-run-1', memory: { resource: 'unix-resource', thread: 'unix-thread' } } as any,
          ownerPubSub,
        );

        await expect(ownerRun).resolves.toMatchObject({ value: { text: 'hello over uds' }, done: false });
        await expect(followerRun).resolves.toMatchObject({ value: { text: 'hello over uds' }, done: false });
        finishRun();
        ownerSubscription.unsubscribe();
        followerSubscription.unsubscribe();
      } finally {
        await Promise.allSettled([ownerPubSub.close(), followerPubSub.close()]);
        await rm(tempDir, { recursive: true, force: true });
      }
    },
  );

  it.runIf(process.platform !== 'win32')(
    'lets a remote subscriber join an already-active UnixSocketPubSub run',
    async () => {
      const tempDir = await mkdtemp(join(tmpdir(), 'mastra-agent-late-subscriber-'));
      const ownerPubSub = new UnixSocketPubSub(join(tempDir, 'signals.sock'));
      const followerPubSub = new UnixSocketPubSub(join(tempDir, 'signals.sock'));
      const ownerRuntime = new AgentThreadStreamRuntime();
      const followerRuntime = new AgentThreadStreamRuntime();
      const owner = { id: 'late-subscriber-agent' } as Agent<any, any, any, any>;
      const follower = { id: 'late-subscriber-agent' } as Agent<any, any, any, any>;
      const runId = 'late-subscriber-run';
      let firstPartBroadcasted!: () => void;
      let continueRun!: () => void;
      let finishRun!: () => void;
      const firstPart = new Promise<void>(resolve => (firstPartBroadcasted = resolve));
      const continuePromise = new Promise<void>(resolve => (continueRun = resolve));
      const finished = new Promise<void>(resolve => (finishRun = resolve));
      const output = {
        runId,
        status: 'running',
        fullStream: (async function* () {
          yield { type: 'text-delta', runId, payload: { text: 'before subscriber' } };
          firstPartBroadcasted();
          await continuePromise;
          yield { type: 'text-delta', runId, payload: { text: 'after subscriber' } };
          yield { type: 'finish', runId, payload: {} };
          finishRun();
        })(),
        _waitUntilFinished: () => finished,
      } as any;

      try {
        ownerRuntime.registerRun(
          owner,
          output,
          { runId, memory: { resource: 'late-subscriber-resource', thread: 'late-subscriber-thread' } } as any,
          ownerPubSub,
        );
        await withTimeout(firstPart, 'Timed out waiting for owner run to start');

        const followerSubscription = await followerRuntime.subscribeToThread(
          follower,
          { resourceId: 'late-subscriber-resource', threadId: 'late-subscriber-thread' },
          followerPubSub,
        );
        const followerRun = readNextRun(followerSubscription.stream[Symbol.asyncIterator]());

        continueRun();

        await expect(withTimeout(followerRun, 'Timed out waiting for late subscriber')).resolves.toMatchObject({
          value: { runId, text: 'after subscriber' },
          done: false,
        });
        followerSubscription.unsubscribe();
      } finally {
        await Promise.allSettled([ownerPubSub.close(), followerPubSub.close()]);
        await rm(tempDir, { recursive: true, force: true });
      }
    },
  );

  it.runIf(process.platform !== 'win32')(
    'broadcasts to a remote subscriber without a same-runtime subscriber',
    async () => {
      const tempDir = await mkdtemp(join(tmpdir(), 'mastra-agent-remote-only-'));
      const ownerPubSub = new UnixSocketPubSub(join(tempDir, 'signals.sock'));
      const followerPubSub = new UnixSocketPubSub(join(tempDir, 'signals.sock'));
      const ownerRuntime = new AgentThreadStreamRuntime();
      const followerRuntime = new AgentThreadStreamRuntime();
      const owner = { id: 'remote-only-agent' } as Agent<any, any, any, any>;
      const follower = { id: 'remote-only-agent' } as Agent<any, any, any, any>;
      const runId = 'remote-only-run';
      let finishRun!: () => void;
      const output = {
        runId,
        status: 'running',
        fullStream: (async function* () {
          yield { type: 'text-delta', runId, payload: { text: 'remote only response' } };
          yield { type: 'finish', runId, payload: {} };
        })(),
        _waitUntilFinished: () => new Promise<void>(resolve => (finishRun = resolve)),
      } as any;

      try {
        const followerSubscription = await followerRuntime.subscribeToThread(
          follower,
          { resourceId: 'remote-only-resource', threadId: 'remote-only-thread' },
          followerPubSub,
        );
        const followerRun = readNextRun(followerSubscription.stream[Symbol.asyncIterator]());

        ownerRuntime.registerRun(
          owner,
          output,
          { runId, memory: { resource: 'remote-only-resource', thread: 'remote-only-thread' } } as any,
          ownerPubSub,
        );

        await expect(withTimeout(followerRun, 'Timed out waiting for remote-only subscriber')).resolves.toMatchObject({
          value: { runId, text: 'remote only response' },
          done: false,
        });
        finishRun();
        followerSubscription.unsubscribe();
      } finally {
        await Promise.allSettled([ownerPubSub.close(), followerPubSub.close()]);
        await rm(tempDir, { recursive: true, force: true });
      }
    },
  );

  it('supports cross-instance thread subscriptions through an injected PubSub without Mastra', async () => {
    const pubsub = new EventEmitterPubSub();
    const runner = new Agent({
      id: 'standalone-shared-agent',
      name: 'Standalone Shared Runner Agent',
      instructions: 'Test',
      model: createTextStreamModel('standalone shared response'),
      pubsub,
    });
    const observer = new Agent({
      id: 'standalone-shared-agent',
      name: 'Standalone Shared Observer Agent',
      instructions: 'Test',
      model: createTextStreamModel('standalone observer response'),
      pubsub,
    });

    const subscription = await observer.subscribeToThread({
      threadId: 'standalone-shared-thread',
      resourceId: 'standalone-shared-user',
    });
    const iterator = subscription.stream[Symbol.asyncIterator]();
    const firstRunPromise = readNextRun(iterator);

    const stream = await runner.stream('Hello', {
      memory: { thread: 'standalone-shared-thread', resource: 'standalone-shared-user' },
    });

    const subscribedRun = await firstRunPromise;
    expect(subscribedRun.value.runId).toBe(stream.runId);
    expect(subscribedRun.value.text).toBe('standalone shared response');

    const secondRunPromise = readNextRun(iterator);
    const signalResult = await runner.sendSignal(
      { type: 'user-message', contents: 'Hello from standalone shared signal' },
      {
        resourceId: 'standalone-shared-user',
        threadId: 'standalone-shared-thread',
        ifIdle: {
          streamOptions: { memory: { resource: 'standalone-shared-user', thread: 'standalone-shared-thread' } },
        },
      },
    );
    const signalRun = await secondRunPromise;
    await expect(signalResult.accepted).resolves.toMatchObject({ action: 'wake', runId: signalRun.value.runId });
    expect(signalResult.signal.id).toBeDefined();
    expect(signalRun.value.text).toBe('standalone shared response');

    subscription.unsubscribe();
  });

  it('broadcasts through async PubSub without consuming the caller fullStream', async () => {
    const pubsub = new AsyncFanoutPubSub();
    const runner = new Agent({
      id: 'async-shared-agent',
      name: 'Async Shared Runner Agent',
      instructions: 'Test',
      model: createTextStreamModel('async shared response'),
      pubsub,
    });
    const observer = new Agent({
      id: 'async-shared-agent',
      name: 'Async Shared Observer Agent',
      instructions: 'Test',
      model: createTextStreamModel('async observer response'),
      pubsub,
    });
    const subscription = await observer.subscribeToThread({
      resourceId: 'async-user',
      threadId: 'async-thread',
    });

    const stream = await runner.stream('Hello', {
      memory: { resource: 'async-user', thread: 'async-thread' },
    });

    await expect(readNextRun(stream.fullStream[Symbol.asyncIterator]())).resolves.toMatchObject({
      value: { runId: stream.runId, text: 'async shared response' },
      done: false,
    });
    await expect(readNextRun(subscription.stream[Symbol.asyncIterator]())).resolves.toMatchObject({
      value: { runId: stream.runId, text: 'async shared response' },
      done: false,
    });

    subscription.unsubscribe();
  });

  it('broadcasts async PubSub stream parts across runtime instances in order', async () => {
    const pubsub = new AsyncFanoutPubSub();
    const ownerRuntime = new AgentThreadStreamRuntime();
    const observerRuntime = new AgentThreadStreamRuntime();
    const runId = 'async-remote-run';
    const chunks = [
      { type: 'stream-start', runId, from: 'AGENT', payload: { warnings: [] } },
      { type: 'text-start', runId, from: 'AGENT', payload: { id: 'text-1' } },
      { type: 'text-delta', runId, from: 'AGENT', payload: { id: 'text-1', text: 'remote async response' } },
      { type: 'text-end', runId, from: 'AGENT', payload: { id: 'text-1' } },
      { type: 'finish', runId, from: 'AGENT', payload: {} },
    ];
    let finish!: () => void;
    const finished = new Promise<void>(resolve => {
      finish = resolve;
    });

    const subscription = await observerRuntime.subscribeToThread(
      { id: 'async-remote-observer' } as any,
      {
        resourceId: 'async-remote-user',
        threadId: 'async-remote-thread',
      },
      pubsub,
    );
    const nextRun = readNextRun(subscription.stream[Symbol.asyncIterator]());
    const output = {
      runId,
      status: 'running',
      fullStream: new ReadableStream({
        start(controller) {
          for (const chunk of chunks) controller.enqueue(chunk);
          finish();
          controller.close();
        },
      }),
      _waitUntilFinished: () => finished,
    };

    const completion = ownerRuntime.registerRun(
      { id: 'async-remote-owner' } as any,
      output as any,
      {
        runId,
        memory: { resource: 'async-remote-user', thread: 'async-remote-thread' },
      } as any,
      pubsub,
    )!;

    await expect(nextRun).resolves.toMatchObject({
      value: { runId, text: 'remote async response' },
      done: false,
    });
    await expect(completion).resolves.toBeUndefined();

    subscription.unsubscribe();
  });

  it('isolates standalone agents that use different injected pubsubs', async () => {
    const runner = new Agent({
      id: 'standalone-isolated-agent',
      name: 'Standalone Isolated Runner Agent',
      instructions: 'Test',
      model: createTextStreamModel('isolated response'),
      pubsub: new EventEmitterPubSub(),
    });
    const observer = new Agent({
      id: 'standalone-isolated-agent',
      name: 'Standalone Isolated Observer Agent',
      instructions: 'Test',
      model: createTextStreamModel('isolated observer response'),
      pubsub: new EventEmitterPubSub(),
    });

    const subscription = await observer.subscribeToThread({
      threadId: 'standalone-isolated-thread',
      resourceId: 'standalone-isolated-user',
    });
    const iterator = subscription.stream[Symbol.asyncIterator]();
    const nextRunPromise = readNextRun(iterator);

    await runner.stream('Hello', {
      memory: { thread: 'standalone-isolated-thread', resource: 'standalone-isolated-user' },
    });

    const result = await Promise.race([
      nextRunPromise.then(() => 'delivered'),
      new Promise<'timeout'>(resolve => setTimeout(() => resolve('timeout'), 20)),
    ]);
    expect(result).toBe('timeout');

    subscription.unsubscribe();
    await nextRunPromise;
  });

  it('passes parent PubSub to child agent execution without mutating shared child agents', async () => {
    const pubsub = new EventEmitterPubSub();
    const childCalls: Array<{ _pubsub?: PubSub }> = [];
    const createDelegatingModel = () => {
      let callCount = 0;
      return new MockLanguageModelV2({
        doGenerate: async () => {
          callCount += 1;
          if (callCount === 1) {
            return {
              rawCall: { rawPrompt: null, rawSettings: {} },
              finishReason: 'tool-calls' as const,
              usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
              text: '',
              content: [
                {
                  type: 'tool-call' as const,
                  toolCallId: `call-${callCount}`,
                  toolName: 'agent-child',
                  input: JSON.stringify({ prompt: 'ask child' }),
                },
              ],
              warnings: [],
            };
          }
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            finishReason: 'stop' as const,
            usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
            text: 'parent response',
            content: [{ type: 'text' as const, text: 'parent response' }],
            warnings: [],
          };
        },
      });
    };

    class CapturingChildAgent extends Agent {
      override async generate(_messages: any, options?: any) {
        childCalls.push(options ?? {});
        return {
          text: 'child response',
          finishReason: 'stop',
          runId: 'child-run',
          response: { dbMessages: [] },
        } as any;
      }

      override async stream(_messages: any, options?: any) {
        childCalls.push(options ?? {});
        const output = buildFakeOutput({
          runId: options?.runId ?? 'child-stream-run',
          fullOutput: {
            text: 'child response',
            finishReason: 'stop',
            response: { dbMessages: [] },
          },
        }) as any;
        return {
          ...output,
          messageList: {
            get: {
              response: {
                db: () => [],
              },
            },
          },
          toolResults: Promise.resolve([]),
        } as any;
      }
    }

    const child = new CapturingChildAgent({
      id: 'standalone-child-agent',
      name: 'Standalone Child Agent',
      instructions: 'Test',
      model: createTextStreamModel('child response'),
    });
    const parent = new Agent({
      id: 'standalone-parent-agent',
      name: 'Standalone Parent Agent',
      instructions: 'Test',
      model: createDelegatingModel(),
      pubsub,
      agents: { child },
    });

    await parent.generate('delegate to child', {
      runId: 'parent-run',
      maxSteps: 3,
    });

    expect(childCalls.at(-1)?._pubsub).toBe(pubsub);
    expect(child.getPubSub()).toBeUndefined();

    const secondPubSub = new EventEmitterPubSub();
    const secondParent = new Agent({
      id: 'second-standalone-parent-agent',
      name: 'Second Standalone Parent Agent',
      instructions: 'Test',
      model: createDelegatingModel(),
      pubsub: secondPubSub,
      agents: { child },
    });

    await secondParent.generate('delegate to child again', {
      runId: 'second-parent-run',
      maxSteps: 3,
    });

    expect(childCalls.at(-1)?._pubsub).toBe(secondPubSub);
    expect(child.getPubSub()).toBeUndefined();
    expect(child.hasOwnPubSub()).toBe(false);
  });

  it('preserves an injected PubSub when forking an agent', () => {
    const pubsub = new AsyncFanoutPubSub();
    const agent = new Agent({
      id: 'forked-agent-pubsub',
      name: 'Forked Agent PubSub',
      instructions: 'Test',
      model: createTextStreamModel('forked response'),
    });

    agent.__setPubSub(pubsub);
    const fork = agent.__fork();

    expect(fork.getPubSub()).toBe(pubsub);
    expect(fork.hasOwnPubSub()).toBe(false);
  });

  it('keeps one PubSub for a stream run when the agent gets a PubSub during execution setup', async () => {
    const swappedPubSub = new EventEmitterPubSub();
    let markDefaultOptionsStarted!: () => void;
    let releaseDefaultOptions!: () => void;
    const defaultOptionsStarted = new Promise<void>(resolve => {
      markDefaultOptionsStarted = resolve;
    });
    const defaultOptionsReleased = new Promise<void>(resolve => {
      releaseDefaultOptions = resolve;
    });
    let releaseFirst!: () => void;
    const firstFinished = new Promise<void>(resolve => {
      releaseFirst = resolve;
    });
    let streamCount = 0;
    const prompts: any[][] = [];

    const runner = new Agent({
      id: 'pubsub-snapshot-agent',
      name: 'PubSub Snapshot Runner',
      instructions: 'Test',
      defaultOptions: async () => {
        markDefaultOptionsStarted();
        await defaultOptionsReleased;
        return {};
      },
      model: new MockLanguageModelV2({
        doStream: async ({ prompt }) => {
          streamCount += 1;
          const callIndex = streamCount;
          prompts.push(prompt);
          const responseText = callIndex === 1 ? 'first response' : 'signal response';
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: new ReadableStream({
              async start(controller) {
                controller.enqueue({ type: 'stream-start', warnings: [] });
                controller.enqueue({
                  type: 'response-metadata',
                  id: `id-${callIndex}`,
                  modelId: 'mock-model-id',
                  timestamp: new Date(0),
                });
                controller.enqueue({ type: 'text-start', id: 'text-1' });
                controller.enqueue({ type: 'text-delta', id: 'text-1', delta: responseText });
                controller.enqueue({ type: 'text-end', id: 'text-1' });
                if (callIndex === 1) {
                  await firstFinished;
                }
                controller.enqueue({
                  type: 'finish',
                  finishReason: 'stop',
                  usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
                });
                controller.close();
              },
            }),
          };
        },
      }),
    });
    const initialObserver = new Agent({
      id: 'pubsub-snapshot-agent',
      name: 'Initial PubSub Observer',
      instructions: 'Test',
      model: createTextStreamModel('initial observer response'),
    });
    const initialSubscription = await initialObserver.subscribeToThread({
      threadId: 'pubsub-snapshot-thread',
      resourceId: 'pubsub-snapshot-user',
    });
    const initialIterator = initialSubscription.stream[Symbol.asyncIterator]();
    const initialNextRun = readNextRun(initialIterator);

    const streamPromise = runner.stream('Hello', {
      memory: { thread: 'pubsub-snapshot-thread', resource: 'pubsub-snapshot-user' },
    });
    await defaultOptionsStarted;
    runner.__setPubSub(swappedPubSub);
    const signalResult = await runner.sendSignal(
      { type: 'user-message', contents: 'Hello while running' },
      { resourceId: 'pubsub-snapshot-user', threadId: 'pubsub-snapshot-thread' },
    );
    expect(signalResult.runId).toEqual(expect.any(String));
    releaseDefaultOptions();

    const stream = await streamPromise;
    await expect(waitForActiveRun(initialSubscription)).resolves.toBe(stream.runId);
    expect(runner.getRunOutput(stream.runId)).toBe(stream);
    expect(signalResult.runId).toBe(stream.runId);
    await expect(signalResult.accepted).resolves.toMatchObject({ action: 'deliver', runId: stream.runId });

    releaseFirst();
    const initialRun = await Promise.race([
      initialNextRun,
      new Promise<'timeout'>(resolve => setTimeout(() => resolve('timeout'), 500)),
    ]);
    expect(initialRun).not.toBe('timeout');
    expect(initialRun).toMatchObject({
      value: { runId: stream.runId, text: 'first response' },
      done: false,
    });
    await expect(stream.text).resolves.toBe('first response');
    expect(JSON.stringify(prompts)).toContain('Hello while running');

    initialSubscription.unsubscribe();
  });

  it('keeps one PubSub for a default idle wake signal when ifIdle options are omitted', async () => {
    const swappedPubSub = new EventEmitterPubSub();
    let markDefaultOptionsStarted!: () => void;
    let releaseDefaultOptions!: () => void;
    const defaultOptionsStarted = new Promise<void>(resolve => {
      markDefaultOptionsStarted = resolve;
    });
    const defaultOptionsReleased = new Promise<void>(resolve => {
      releaseDefaultOptions = resolve;
    });

    const runner = new Agent({
      id: 'default-idle-wake-pubsub-agent',
      name: 'Default Idle Wake PubSub Agent',
      instructions: 'Test',
      defaultOptions: async () => {
        markDefaultOptionsStarted();
        await defaultOptionsReleased;
        return {};
      },
      model: createTextStreamModel('default idle wake response'),
    });
    const initialObserver = new Agent({
      id: 'default-idle-wake-pubsub-agent',
      name: 'Default Idle Wake Initial Observer',
      instructions: 'Test',
      model: createTextStreamModel('observer response'),
    });
    const initialSubscription = await initialObserver.subscribeToThread({
      threadId: 'default-idle-wake-thread',
      resourceId: 'default-idle-wake-user',
    });
    const initialNextRun = readNextRun(initialSubscription.stream[Symbol.asyncIterator]());

    const signalResult = runner.sendSignal(
      { type: 'user-message', contents: 'Wake without explicit ifIdle' },
      { resourceId: 'default-idle-wake-user', threadId: 'default-idle-wake-thread' },
    );
    expect(signalResult.output).toBeDefined();
    await defaultOptionsStarted;
    runner.__setPubSub(swappedPubSub);
    releaseDefaultOptions();

    await expect(signalResult.output).resolves.toMatchObject({ runId: signalResult.runId });
    const initialRun = await Promise.race([
      initialNextRun,
      new Promise<'timeout'>(resolve => setTimeout(() => resolve('timeout'), 500)),
    ]);
    expect(initialRun).not.toBe('timeout');
    expect(initialRun).toMatchObject({
      value: { runId: signalResult.runId, text: 'default idle wake response' },
      done: false,
    });

    initialSubscription.unsubscribe();
  });

  it('tracks idle wake PubSub from ifIdle streamOptions memory when top-level thread target is omitted', async () => {
    const swappedPubSub = new EventEmitterPubSub();
    let markDefaultOptionsStarted!: () => void;
    let releaseDefaultOptions!: () => void;
    const defaultOptionsStarted = new Promise<void>(resolve => {
      markDefaultOptionsStarted = resolve;
    });
    const defaultOptionsReleased = new Promise<void>(resolve => {
      releaseDefaultOptions = resolve;
    });

    const runner = new Agent({
      id: 'stream-options-idle-wake-pubsub-agent',
      name: 'Stream Options Idle Wake PubSub Agent',
      instructions: 'Test',
      defaultOptions: async () => {
        markDefaultOptionsStarted();
        await defaultOptionsReleased;
        return {};
      },
      model: createTextStreamModel('stream options idle wake response'),
    });

    const signalResult = runner.sendSignal({ type: 'user-message', contents: 'Wake from streamOptions target' }, {
      ifIdle: {
        streamOptions: {
          memory: { resource: 'stream-options-idle-wake-user', thread: 'stream-options-idle-wake-thread' },
        },
      },
    } as any);
    expect(signalResult.output).toBeDefined();
    await defaultOptionsStarted;
    runner.__setPubSub(swappedPubSub);
    releaseDefaultOptions();

    const output = await signalResult.output;
    expect(output).toMatchObject({ runId: signalResult.runId });
    expect(runner.getRunOutput(signalResult.runId!)).toBe(output);
  });

  it('honors an injected PubSub for streamUntilIdle when the agent PubSub changes', async () => {
    const initialPubSub = new EventEmitterPubSub();
    const swappedPubSub = new EventEmitterPubSub();
    const runner = new Agent({
      id: 'stream-until-idle-pubsub-agent',
      name: 'Stream Until Idle PubSub Agent',
      instructions: 'Test',
      model: createTextStreamModel('stream until idle response'),
    });
    runner.__setPubSub(swappedPubSub);

    const observer = new Agent({
      id: 'stream-until-idle-pubsub-agent',
      name: 'Stream Until Idle PubSub Observer',
      instructions: 'Test',
      model: createTextStreamModel('observer response'),
    });
    observer.__setPubSub(initialPubSub);
    const subscription = await observer.subscribeToThread({
      threadId: 'stream-until-idle-thread',
      resourceId: 'stream-until-idle-user',
    });
    const nextRun = readNextRun(subscription.stream[Symbol.asyncIterator]());

    const stream = await runner.streamUntilIdle('Hello', {
      memory: { thread: 'stream-until-idle-thread', resource: 'stream-until-idle-user' },
      _pubsub: initialPubSub,
    } as any);

    await expect(stream.text).resolves.toBe('stream until idle response');
    await expect(nextRun).resolves.toMatchObject({
      value: { runId: stream.runId, text: 'stream until idle response' },
      done: false,
    });

    subscription.unsubscribe();
  });

  it('honors an injected PubSub when test agents register streams through the internal hook', async () => {
    const initialPubSub = new EventEmitterPubSub();
    const swappedPubSub = new EventEmitterPubSub();
    const agent = new Agent({
      id: 'internal-register-pubsub-agent',
      name: 'Internal Register PubSub Agent',
      instructions: 'Test',
      model: createTextStreamModel('unused'),
    });
    agent.__setPubSub(swappedPubSub);
    const observer = new Agent({
      id: 'internal-register-pubsub-agent',
      name: 'Internal Register Observer',
      instructions: 'Test',
      model: createTextStreamModel('observer response'),
    });
    observer.__setPubSub(initialPubSub);
    const subscription = await observer.subscribeToThread({
      threadId: 'internal-register-thread',
      resourceId: 'internal-register-user',
    });
    const nextRun = readNextRun(subscription.stream[Symbol.asyncIterator]());
    const output = buildFakeOutput({
      runId: 'internal-register-run',
      fullOutput: { text: 'internal response', finishReason: 'stop', usage: {} },
      chunks: [
        { runId: 'internal-register-run', type: 'text-delta', payload: { text: 'internal response' } },
        { runId: 'internal-register-run', type: 'finish', payload: {} },
      ],
    });

    agent._internalRegisterStreamRun(output, {
      runId: 'internal-register-run',
      memory: { resource: 'internal-register-user', thread: 'internal-register-thread' },
      _pubsub: initialPubSub,
    } as any);
    expect(agent.getRunOutput('internal-register-run')).toBe(output);

    await expect(nextRun).resolves.toMatchObject({
      value: { runId: 'internal-register-run', text: 'internal response' },
      done: false,
    });

    subscription.unsubscribe();
  });

  it('re-reserves a pre-default stream when default options change the request-context thread target', async () => {
    let markDefaultOptionsStarted!: () => void;
    let releaseDefaultOptions!: () => void;
    const defaultOptionsStarted = new Promise<void>(resolve => {
      markDefaultOptionsStarted = resolve;
    });
    const defaultOptionsReleased = new Promise<void>(resolve => {
      releaseDefaultOptions = resolve;
    });
    const requestContext = new RequestContext();
    requestContext.set(MASTRA_RESOURCE_ID_KEY, 'default-context-user');
    requestContext.set(MASTRA_THREAD_ID_KEY, 'default-context-thread');

    const runner = new Agent({
      id: 'default-context-reservation-agent',
      name: 'Default Context Reservation Agent',
      instructions: 'Test',
      defaultOptions: async () => {
        markDefaultOptionsStarted();
        await defaultOptionsReleased;
        return { requestContext };
      },
      model: createTextStreamModel('default context response'),
    });
    const observer = new Agent({
      id: 'default-context-reservation-agent',
      name: 'Default Context Reservation Observer',
      instructions: 'Test',
      model: createTextStreamModel('observer response'),
    });
    const subscription = await observer.subscribeToThread({
      threadId: 'default-context-thread',
      resourceId: 'default-context-user',
    });
    const nextRun = readNextRun(subscription.stream[Symbol.asyncIterator]());

    const streamPromise = runner.stream('Hello', {
      memory: { thread: 'explicit-before-default-thread', resource: 'explicit-before-default-user' },
    });
    await defaultOptionsStarted;
    releaseDefaultOptions();

    const stream = await streamPromise;
    await expect(nextRun).resolves.toMatchObject({
      value: { runId: stream.runId, text: 'default context response' },
      done: false,
    });

    subscription.unsubscribe();
  });

  it('preserves accepted setup signals when default options retarget a reserved stream', async () => {
    let markDefaultOptionsStarted!: () => void;
    let releaseDefaultOptions!: () => void;
    const defaultOptionsStarted = new Promise<void>(resolve => {
      markDefaultOptionsStarted = resolve;
    });
    const defaultOptionsReleased = new Promise<void>(resolve => {
      releaseDefaultOptions = resolve;
    });
    const requestContext = new RequestContext();
    requestContext.set(MASTRA_RESOURCE_ID_KEY, 'retarget-signal-context-user');
    requestContext.set(MASTRA_THREAD_ID_KEY, 'retarget-signal-context-thread');
    const prompts: any[][] = [];

    const runner = new Agent({
      id: 'retarget-preserve-signal-agent',
      name: 'Retarget Preserve Signal Agent',
      instructions: 'Test',
      defaultOptions: async () => {
        markDefaultOptionsStarted();
        await defaultOptionsReleased;
        return { requestContext };
      },
      model: new MockLanguageModelV2({
        doStream: async ({ prompt }) => {
          prompts.push(prompt);
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: convertArrayToReadableStream([
              { type: 'stream-start', warnings: [] },
              { type: 'response-metadata', id: 'id-0', modelId: 'mock-model-id', timestamp: new Date(0) },
              { type: 'text-start', id: 'text-1' },
              { type: 'text-delta', id: 'text-1', delta: 'retarget response' },
              { type: 'text-end', id: 'text-1' },
              { type: 'finish', finishReason: 'stop', usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 } },
            ]),
          };
        },
      }),
    });

    const streamPromise = runner.stream('Hello', {
      memory: { thread: 'retarget-signal-original-thread', resource: 'retarget-signal-original-user' },
    });
    await defaultOptionsStarted;
    const signalResult = runner.sendSignal(
      { type: 'user-message', contents: 'accepted before retarget' },
      { resourceId: 'retarget-signal-original-user', threadId: 'retarget-signal-original-thread' },
    );
    releaseDefaultOptions();

    const stream = await streamPromise;
    expect(signalResult.runId).toBe(stream.runId);
    await expect(stream.text).resolves.toBe('retarget response');
    expect(JSON.stringify(prompts)).toContain('accepted before retarget');
  });

  it('forgets a re-reserved PubSub mapping when stream setup fails before preparation', async () => {
    const swappedPubSub = new EventEmitterPubSub();
    let useUnsupportedModel = true;
    const requestContext = new RequestContext();
    requestContext.set(MASTRA_RESOURCE_ID_KEY, 'failed-rereserve-context-user');
    requestContext.set(MASTRA_THREAD_ID_KEY, 'failed-rereserve-context-thread');
    const unsupportedModel = new MockLanguageModelV1({
      doGenerate: async () => ({
        rawCall: { rawPrompt: null, rawSettings: {} },
        finishReason: 'stop',
        usage: { promptTokens: 1, completionTokens: 1 },
        text: 'unsupported',
      }),
      doStream: async () => ({
        rawCall: { rawPrompt: null, rawSettings: {} },
        stream: convertArrayToReadableStream([]),
      }),
    });
    const supportedModel = createTextStreamModel('post failure response');

    const runner = new Agent({
      id: 'failed-rereserve-pubsub-agent',
      name: 'Failed Rereserve PubSub Agent',
      instructions: 'Test',
      defaultOptions: async () => ({ requestContext }),
      model: () => (useUnsupportedModel ? unsupportedModel : supportedModel),
    });

    await expect(
      runner.stream('Hello', {
        runId: 'failed-rereserve-explicit-run',
        memory: { thread: 'failed-rereserve-original-thread', resource: 'failed-rereserve-original-user' },
      }),
    ).rejects.toThrow('not compatible with stream()');

    useUnsupportedModel = false;
    runner.__setPubSub(swappedPubSub);
    const subscription = await runner.subscribeToThread({
      threadId: 'failed-rereserve-context-thread',
      resourceId: 'failed-rereserve-context-user',
    });
    const nextRun = readNextRun(subscription.stream[Symbol.asyncIterator]());
    const stream = await runner.stream('Hello again', {
      runId: 'failed-rereserve-explicit-run',
      memory: { thread: 'failed-rereserve-context-thread', resource: 'failed-rereserve-context-user' },
    });

    expect(stream.runId).toBe('failed-rereserve-explicit-run');
    await expect(stream.text).resolves.toBe('post failure response');
    const observedRun = await Promise.race([
      nextRun,
      new Promise<'timeout'>(resolve => setTimeout(() => resolve('timeout'), 500)),
    ]);
    expect(observedRun).not.toBe('timeout');
    expect(observedRun).toMatchObject({
      value: { runId: stream.runId, text: 'post failure response' },
      done: false,
    });

    subscription.unsubscribe();
  });

  it('reserves request-context scoped streams while default options are pending', async () => {
    let markDefaultOptionsStarted!: () => void;
    let releaseDefaultOptions!: () => void;
    const defaultOptionsStarted = new Promise<void>(resolve => {
      markDefaultOptionsStarted = resolve;
    });
    const defaultOptionsReleased = new Promise<void>(resolve => {
      releaseDefaultOptions = resolve;
    });
    const requestContext = new RequestContext();
    requestContext.set(MASTRA_RESOURCE_ID_KEY, 'request-context-reserved-user');
    requestContext.set(MASTRA_THREAD_ID_KEY, 'request-context-reserved-thread');

    const runner = new Agent({
      id: 'request-context-reserved-agent',
      name: 'Request Context Reserved Agent',
      instructions: 'Test',
      defaultOptions: async () => {
        markDefaultOptionsStarted();
        await defaultOptionsReleased;
        return {};
      },
      model: createTextStreamModel('request context reserved response'),
    });

    const streamPromise = runner.stream('Hello', { requestContext });
    await defaultOptionsStarted;
    const signalResult = runner.sendSignal(
      { type: 'user-message', contents: 'Hello while request-context stream is starting' },
      { resourceId: 'request-context-reserved-user', threadId: 'request-context-reserved-thread' },
    );
    releaseDefaultOptions();

    const stream = await streamPromise;
    expect(signalResult.runId).toBe(stream.runId);
    await expect(signalResult.accepted).resolves.toMatchObject({ action: 'deliver', runId: stream.runId });
  });

  it('does not expose explicit thread streams when request context preflight fails', async () => {
    const requestContext = new RequestContext();
    requestContext.set('allowed', false);

    const runner = new Agent({
      id: 'preflight-denied-reservation-agent',
      name: 'Preflight Denied Reservation Agent',
      instructions: 'Test',
      requestContextSchema: z.object({ allowed: z.literal(true) }),
      model: createTextStreamModel('unused denied response'),
    });

    await expect(
      runner.stream('Hello', {
        runId: 'preflight-denied-run',
        memory: { resource: 'preflight-denied-user', thread: 'preflight-denied-thread' },
        requestContext,
      }),
    ).rejects.toThrow('Request context validation failed');

    const signalResult = runner.sendSignal(
      { type: 'user-message', contents: 'Should not attach to denied setup run' },
      {
        resourceId: 'preflight-denied-user',
        threadId: 'preflight-denied-thread',
        ifIdle: { behavior: 'discard' },
      },
    );

    expect(signalResult.runId).not.toBe('preflight-denied-run');
  });

  it('does not attach idle signals to explicit-context streams before preflight passes', async () => {
    const requestContext = new RequestContext();
    requestContext.set('allowed', false);

    const runner = new Agent({
      id: 'explicit-preflight-denied-reservation-agent',
      name: 'Explicit Preflight Denied Reservation Agent',
      instructions: 'Test',
      requestContextSchema: z.object({ allowed: z.literal(true) }),
      model: createTextStreamModel('unused denied explicit idle response'),
    });

    const wake = runner.sendSignal(
      { type: 'user-message', contents: 'Start denied explicit-context idle stream' },
      {
        resourceId: 'explicit-preflight-denied-user',
        threadId: 'explicit-preflight-denied-thread',
        ifIdle: { behavior: 'wake', streamOptions: { requestContext } },
      },
    );
    void wake.output?.catch(() => {});

    const followUp = runner.sendSignal(
      { type: 'user-message', contents: 'Should not attach before explicit preflight passes' },
      {
        resourceId: 'explicit-preflight-denied-user',
        threadId: 'explicit-preflight-denied-thread',
        ifIdle: { behavior: 'discard' },
      },
    );

    expect(followUp.runId).not.toBe(wake.runId);
    await expect(wake.output).rejects.toThrow('Request context validation failed');
  });

  it('does not reserve explicit-context streams before preflight passes', async () => {
    let markPreflightStarted!: () => void;
    let releasePreflight!: () => void;
    const preflightStarted = new Promise<void>(resolve => {
      markPreflightStarted = resolve;
    });
    const preflightReleased = new Promise<void>(resolve => {
      releasePreflight = resolve;
    });
    const requestContext = new RequestContext();
    requestContext.set('allowed', true);

    const runner = new Agent({
      id: 'explicit-preflight-allowed-reservation-agent',
      name: 'Explicit Preflight Allowed Reservation Agent',
      instructions: 'Test',
      requestContextSchema: z.object({ allowed: z.literal(true) }).superRefine(async () => {
        markPreflightStarted();
        await preflightReleased;
      }),
      model: createTextStreamModel('allowed explicit preflight response'),
    });

    const streamPromise = runner.stream('Hello', {
      memory: { resource: 'explicit-preflight-allowed-user', thread: 'explicit-preflight-allowed-thread' },
      requestContext,
    });
    await preflightStarted;

    const followUp = runner.sendSignal(
      { type: 'user-message', contents: 'Should not attach before explicit preflight passes' },
      {
        resourceId: 'explicit-preflight-allowed-user',
        threadId: 'explicit-preflight-allowed-thread',
        ifIdle: { behavior: 'discard' },
      },
    );
    expect(() =>
      runner.sendSignal(
        { type: 'user-message', contents: 'Thread-only signal should not see a pending reservation' },
        {
          threadId: 'explicit-preflight-allowed-thread',
        },
      ),
    ).toThrow('No active agent run found for signal target');
    const explicitActivePolicyFollowUp = runner.sendSignal(
      { type: 'user-message', contents: 'Explicit active-deliver signal should not attach before preflight passes' },
      {
        resourceId: 'explicit-preflight-allowed-user',
        threadId: 'explicit-preflight-allowed-thread',
        ifActive: { behavior: 'deliver' },
        ifIdle: { behavior: 'discard' },
      },
    );
    releasePreflight();

    const stream = await streamPromise;
    expect(followUp.runId).not.toBe(stream.runId);
    expect(explicitActivePolicyFollowUp.runId).not.toBe(stream.runId);
    await expect(stream.text).resolves.toBe('allowed explicit preflight response');
  });

  it('reserves explicit-context streams after preflight passes before defaults finish', async () => {
    let markPreflightStarted!: () => void;
    let releasePreflight!: () => void;
    let markDefaultOptionsStarted!: () => void;
    let releaseDefaultOptions!: () => void;
    const preflightStarted = new Promise<void>(resolve => {
      markPreflightStarted = resolve;
    });
    const preflightReleased = new Promise<void>(resolve => {
      releasePreflight = resolve;
    });
    const defaultOptionsStarted = new Promise<void>(resolve => {
      markDefaultOptionsStarted = resolve;
    });
    const defaultOptionsReleased = new Promise<void>(resolve => {
      releaseDefaultOptions = resolve;
    });
    const requestContext = new RequestContext();
    requestContext.set('allowed', true);

    const runner = new Agent({
      id: 'explicit-preflight-defaults-pending-agent',
      name: 'Explicit Preflight Defaults Pending Agent',
      instructions: 'Test',
      requestContextSchema: z.object({ allowed: z.literal(true) }).superRefine(async () => {
        markPreflightStarted();
        await preflightReleased;
      }),
      defaultOptions: async () => {
        markDefaultOptionsStarted();
        await defaultOptionsReleased;
        return {};
      },
      model: createTextStreamModel('allowed explicit preflight defaults response'),
    });

    const streamPromise = runner.stream('Hello', {
      runId: 'explicit-preflight-defaults-run',
      memory: { resource: 'explicit-preflight-defaults-user', thread: 'explicit-preflight-defaults-thread' },
      requestContext,
    });
    await preflightStarted;

    const beforePreflightFollowUp = runner.sendSignal(
      { type: 'user-message', contents: 'Should not attach before preflight passes' },
      {
        resourceId: 'explicit-preflight-defaults-user',
        threadId: 'explicit-preflight-defaults-thread',
        ifIdle: { behavior: 'discard' },
      },
    );
    expect(beforePreflightFollowUp.runId).not.toBe('explicit-preflight-defaults-run');

    releasePreflight();
    await defaultOptionsStarted;

    const afterPreflightFollowUp = runner.sendSignal(
      { type: 'user-message', contents: 'Should attach after preflight passes while defaults are pending' },
      {
        resourceId: 'explicit-preflight-defaults-user',
        threadId: 'explicit-preflight-defaults-thread',
        ifIdle: { behavior: 'discard' },
      },
    );
    releaseDefaultOptions();

    expect(afterPreflightFollowUp.runId).toBe('explicit-preflight-defaults-run');
    const stream = await streamPromise;
    expect(stream.runId).toBe('explicit-preflight-defaults-run');
    await expect(stream.text).resolves.toBe('allowed explicit preflight defaults response');
  });

  it('keeps direct preflight stream output waiters on the captured PubSub', async () => {
    const initialPubSub = new EventEmitterPubSub();
    const swappedPubSub = new EventEmitterPubSub();
    let markPreflightStarted!: () => void;
    let releasePreflight!: () => void;
    const preflightStarted = new Promise<void>(resolve => {
      markPreflightStarted = resolve;
    });
    const preflightReleased = new Promise<void>(resolve => {
      releasePreflight = resolve;
    });
    const requestContext = new RequestContext();
    requestContext.set('allowed', true);

    const runner = new Agent({
      id: 'direct-preflight-pubsub-agent',
      name: 'Direct Preflight PubSub Agent',
      instructions: 'Test',
      requestContextSchema: z.object({ allowed: z.literal(true) }).superRefine(async () => {
        markPreflightStarted();
        await preflightReleased;
      }),
      model: createTextStreamModel('direct preflight pubsub response'),
    });
    runner.__setPubSub(initialPubSub);

    const streamPromise = runner.stream('Hello', {
      runId: 'direct-preflight-pubsub-run',
      memory: { resource: 'direct-preflight-pubsub-user', thread: 'direct-preflight-pubsub-thread' },
      requestContext,
    });
    await preflightStarted;

    runner.__setPubSub(swappedPubSub);
    const outputPromise = runner.waitForRunOutput('direct-preflight-pubsub-run');
    releasePreflight();

    const output = await Promise.race([
      outputPromise,
      new Promise<'timeout'>(resolve => setTimeout(() => resolve('timeout'), 500)),
    ]);
    expect(output).not.toBe('timeout');
    if (output === 'timeout') return;
    expect(output.runId).toBe('direct-preflight-pubsub-run');
    await expect(output.text).resolves.toBe('direct preflight pubsub response');
    await expect(streamPromise).resolves.toBe(output);
  });

  it('does not tombstone an admitted stream when a duplicate explicit run id is rejected', async () => {
    let markModelStarted!: () => void;
    let releaseModel!: () => void;
    const modelStarted = new Promise<void>(resolve => {
      markModelStarted = resolve;
    });
    const modelReleased = new Promise<void>(resolve => {
      releaseModel = resolve;
    });
    const requestContext = new RequestContext();
    requestContext.set('allowed', true);

    const runner = new Agent({
      id: 'duplicate-preflight-run-id-agent',
      name: 'Duplicate Preflight Run Id Agent',
      instructions: 'Test',
      requestContextSchema: z.object({ allowed: z.literal(true) }),
      model: new MockLanguageModelV2({
        doStream: async () => {
          markModelStarted();
          await modelReleased;
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: convertArrayToReadableStream([
              { type: 'stream-start', warnings: [] },
              {
                type: 'response-metadata',
                id: 'duplicate-preflight',
                modelId: 'mock-model-id',
                timestamp: new Date(0),
              },
              { type: 'text-start', id: 'text-1' },
              { type: 'text-delta', id: 'text-1', delta: 'first admitted response' },
              { type: 'text-end', id: 'text-1' },
              {
                type: 'finish',
                finishReason: 'stop',
                usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
              },
            ]),
          };
        },
      }),
    });

    const firstStreamPromise = runner.stream('First', {
      runId: 'duplicate-preflight-run',
      memory: { resource: 'duplicate-preflight-user', thread: 'duplicate-preflight-thread' },
      requestContext,
    });
    await modelStarted;

    await expect(
      runner.stream('Duplicate', {
        runId: 'duplicate-preflight-run',
        memory: { resource: 'duplicate-preflight-user', thread: 'duplicate-preflight-thread' },
        requestContext,
      }),
    ).rejects.toThrow('already reserved');

    releaseModel();
    const firstStream = await firstStreamPromise;
    expect(firstStream.runId).toBe('duplicate-preflight-run');
    await expect(firstStream.text).resolves.toBe('first admitted response');
  });

  it('keeps explicit-context idle reservations when no preflight boundary is configured', async () => {
    let markDefaultOptionsStarted!: () => void;
    let releaseDefaultOptions!: () => void;
    const defaultOptionsStarted = new Promise<void>(resolve => {
      markDefaultOptionsStarted = resolve;
    });
    const defaultOptionsReleased = new Promise<void>(resolve => {
      releaseDefaultOptions = resolve;
    });
    const requestContext = new RequestContext();

    const runner = new Agent({
      id: 'explicit-context-no-preflight-agent',
      name: 'Explicit Context No Preflight Agent',
      instructions: 'Test',
      defaultOptions: async () => {
        markDefaultOptionsStarted();
        await defaultOptionsReleased;
        return {};
      },
      model: createTextStreamModel('explicit context no preflight response'),
    });

    const wake = runner.sendSignal(
      { type: 'user-message', contents: 'Start explicit-context idle stream' },
      {
        resourceId: 'explicit-context-no-preflight-user',
        threadId: 'explicit-context-no-preflight-thread',
        ifIdle: { behavior: 'wake', streamOptions: { requestContext } },
      },
    );
    await defaultOptionsStarted;

    const followUp = runner.sendSignal(
      { type: 'user-message', contents: 'Should attach when preflight cannot reject' },
      {
        resourceId: 'explicit-context-no-preflight-user',
        threadId: 'explicit-context-no-preflight-thread',
        ifIdle: { behavior: 'discard' },
      },
    );
    releaseDefaultOptions();

    expect(followUp.runId).toBe(wake.runId);
    await expect(wake.output).resolves.toMatchObject({ runId: wake.runId });
  });

  it('does not attach idle signals to default-context streams before preflight passes', async () => {
    let markDefaultOptionsStarted!: () => void;
    let releaseDefaultOptions!: () => void;
    const defaultOptionsStarted = new Promise<void>(resolve => {
      markDefaultOptionsStarted = resolve;
    });
    const defaultOptionsReleased = new Promise<void>(resolve => {
      releaseDefaultOptions = resolve;
    });
    const requestContext = new RequestContext();
    requestContext.set('allowed', false);

    const runner = new Agent({
      id: 'default-preflight-denied-reservation-agent',
      name: 'Default Preflight Denied Reservation Agent',
      instructions: 'Test',
      requestContextSchema: z.object({ allowed: z.literal(true) }),
      defaultOptions: async () => {
        markDefaultOptionsStarted();
        await defaultOptionsReleased;
        return { requestContext };
      },
      model: createTextStreamModel('unused denied idle response'),
    });

    const wake = runner.sendSignal(
      { type: 'user-message', contents: 'Start denied idle stream' },
      {
        resourceId: 'default-preflight-denied-user',
        threadId: 'default-preflight-denied-thread',
      },
    );
    await defaultOptionsStarted;
    const outputPromise = runner.waitForRunOutput(wake.runId);
    void wake.output?.catch(() => {});

    const followUp = runner.sendSignal(
      { type: 'user-message', contents: 'Should not attach before preflight passes' },
      {
        resourceId: 'default-preflight-denied-user',
        threadId: 'default-preflight-denied-thread',
        ifIdle: { behavior: 'discard' },
      },
    );
    const activePolicyResult = runner.sendSignal(
      { type: 'user-message', contents: 'Should not treat preflight-pending run as active' },
      {
        resourceId: 'default-preflight-denied-user',
        threadId: 'default-preflight-denied-thread',
        ifActive: { behavior: 'discard' },
        ifIdle: { behavior: 'discard' },
      },
    );
    const plainFollowUp = runner.sendSignal(
      { type: 'user-message', contents: 'Plain signal should not attach before preflight passes' },
      {
        resourceId: 'default-preflight-denied-user',
        threadId: 'default-preflight-denied-thread',
      },
    );
    void plainFollowUp.output?.catch(() => {});
    releaseDefaultOptions();

    expect(followUp.runId).not.toBe(wake.runId);
    expect(activePolicyResult.runId).not.toBe(wake.runId);
    expect(plainFollowUp.runId).not.toBe(wake.runId);
    await expect(outputPromise).rejects.toThrow(`Agent thread run id "${wake.runId}" was rejected`);
    await expect(wake.output).rejects.toThrow('Request context validation failed');
  });

  it('keeps idle wake output waiters pending while default-context preflight is pending', async () => {
    const initialPubSub = new EventEmitterPubSub();
    const swappedPubSub = new EventEmitterPubSub();
    let markDefaultOptionsStarted!: () => void;
    let releaseDefaultOptions!: () => void;
    const defaultOptionsStarted = new Promise<void>(resolve => {
      markDefaultOptionsStarted = resolve;
    });
    const defaultOptionsReleased = new Promise<void>(resolve => {
      releaseDefaultOptions = resolve;
    });
    const requestContext = new RequestContext();
    requestContext.set('allowed', true);

    const runner = new Agent({
      id: 'default-preflight-valid-waiter-agent',
      name: 'Default Preflight Valid Waiter Agent',
      instructions: 'Test',
      requestContextSchema: z.object({ allowed: z.literal(true) }),
      defaultOptions: async () => {
        markDefaultOptionsStarted();
        await defaultOptionsReleased;
        return { requestContext };
      },
      model: createTextStreamModel('valid default preflight response'),
    });
    runner.__setPubSub(initialPubSub);

    const wake = runner.sendSignal(
      { type: 'user-message', contents: 'Start valid idle stream' },
      {
        resourceId: 'default-preflight-valid-user',
        threadId: 'default-preflight-valid-thread',
      },
    );
    await defaultOptionsStarted;

    runner.__setPubSub(swappedPubSub);
    const outputPromise = runner.waitForRunOutput(wake.runId);
    releaseDefaultOptions();

    const output = await Promise.race([
      outputPromise,
      new Promise<'timeout'>(resolve => setTimeout(() => resolve('timeout'), 500)),
    ]);
    expect(output).not.toBe('timeout');
    if (output === 'timeout') return;
    expect(output.runId).toBe(wake.runId);
    await expect(output.text).resolves.toBe('valid default preflight response');
  });

  it('reserves thread-only streams while default options are pending', async () => {
    let markDefaultOptionsStarted!: () => void;
    let releaseDefaultOptions!: () => void;
    const defaultOptionsStarted = new Promise<void>(resolve => {
      markDefaultOptionsStarted = resolve;
    });
    const defaultOptionsReleased = new Promise<void>(resolve => {
      releaseDefaultOptions = resolve;
    });

    const runner = new Agent({
      id: 'thread-only-reserved-agent',
      name: 'Thread Only Reserved Agent',
      instructions: 'Test',
      defaultOptions: async () => {
        markDefaultOptionsStarted();
        await defaultOptionsReleased;
        return {};
      },
      model: createTextStreamModel('thread only reserved response'),
    });

    const streamPromise = runner.stream('Hello', { memory: { thread: 'thread-only-reserved-thread' } });
    await defaultOptionsStarted;
    const signalResult = runner.sendSignal(
      { type: 'user-message', contents: 'Hello while thread-only stream is starting' },
      { threadId: 'thread-only-reserved-thread' },
    );
    releaseDefaultOptions();

    const stream = await streamPromise;
    expect(signalResult.runId).toBe(stream.runId);
    await expect(signalResult.accepted).resolves.toMatchObject({ action: 'deliver', runId: stream.runId });
  });

  it('routes thread-only signals after default options add a resource target', async () => {
    let markDefaultOptionsStarted!: () => void;
    let releaseDefaultOptions!: () => void;
    let releaseStream!: () => void;
    const defaultOptionsStarted = new Promise<void>(resolve => {
      markDefaultOptionsStarted = resolve;
    });
    const defaultOptionsReleased = new Promise<void>(resolve => {
      releaseDefaultOptions = resolve;
    });
    const streamReleased = new Promise<void>(resolve => {
      releaseStream = resolve;
    });

    const runner = new Agent({
      id: 'thread-only-retargeted-agent',
      name: 'Thread Only Retargeted Agent',
      instructions: 'Test',
      defaultOptions: async () => {
        markDefaultOptionsStarted();
        await defaultOptionsReleased;
        return { memory: { resource: 'thread-only-retargeted-user' } };
      },
      model: new MockLanguageModelV2({
        doStream: async () => ({
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: new ReadableStream({
            async start(controller) {
              controller.enqueue({ type: 'stream-start', warnings: [] });
              controller.enqueue({
                type: 'response-metadata',
                id: 'thread-only-retargeted-id',
                modelId: 'mock-model-id',
                timestamp: new Date(0),
              });
              controller.enqueue({ type: 'text-start', id: 'text-1' });
              controller.enqueue({ type: 'text-delta', id: 'text-1', delta: 'thread only retargeted response' });
              controller.enqueue({ type: 'text-end', id: 'text-1' });
              await streamReleased;
              controller.enqueue({
                type: 'finish',
                finishReason: 'stop',
                usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
              });
              controller.close();
            },
          }),
        }),
      }),
    });

    const streamPromise = runner.stream('Hello', { memory: { thread: 'thread-only-retargeted-thread' } });
    await defaultOptionsStarted;
    const earlySignal = runner.sendSignal(
      { type: 'user-message', contents: 'Hello before retarget' },
      { threadId: 'thread-only-retargeted-thread' },
    );
    releaseDefaultOptions();

    const stream = await streamPromise;
    const lateSignal = runner.sendSignal(
      { type: 'user-message', contents: 'Hello after retarget' },
      { threadId: 'thread-only-retargeted-thread' },
    );

    expect(earlySignal.runId).toBe(stream.runId);
    expect(lateSignal.runId).toBe(stream.runId);
    await expect(earlySignal.accepted).resolves.toMatchObject({ action: 'deliver', runId: stream.runId });
    await expect(lateSignal.accepted).resolves.toMatchObject({ action: 'deliver', runId: stream.runId });
    releaseStream();
    await expect(stream.text).resolves.toBe('thread only retargeted responsethread only retargeted response');
  });

  it('supports cross-instance thread subscriptions through the Mastra runtime', async () => {
    const pubsub = new EventEmitterPubSub();
    const runner = new Agent({
      id: 'shared-agent',
      name: 'Shared Runner Agent',
      instructions: 'Test',
      model: createTextStreamModel('shared response'),
    });
    const observer = new Agent({
      id: 'shared-agent',
      name: 'Shared Observer Agent',
      instructions: 'Test',
      model: createTextStreamModel('observer response'),
    });
    const mastra = new Mastra({ agents: { runner, observer }, logger: false, pubsub });
    // Mastra wraps the raw pubsub in a Proxy (for localOnly tagging), so
    // reference equality against the raw instance won't hold. Verify both
    // agents share the same (proxy-wrapped) `mastra.pubsub` instead.
    expect(runner.getPubSub()).toBe(mastra.pubsub);
    expect(observer.getPubSub()).toBe(mastra.pubsub);

    const subscription = await observer.subscribeToThread({
      threadId: 'shared-thread',
      resourceId: 'shared-user',
    });
    const iterator = subscription.stream[Symbol.asyncIterator]();
    const firstRunPromise = readNextRun(iterator);

    const stream = await runner.stream('Hello', {
      memory: { thread: 'shared-thread', resource: 'shared-user' },
    });

    const subscribedRun = await firstRunPromise;
    expect(subscribedRun.value.runId).toBe(stream.runId);
    expect(subscribedRun.value.text).toBe('shared response');

    const secondRunPromise = readNextRun(iterator);
    const signalResult = await runner.sendSignal(
      { type: 'user-message', contents: 'Hello from shared signal' },
      {
        resourceId: 'shared-user',
        threadId: 'shared-thread',
        ifIdle: { streamOptions: { memory: { resource: 'shared-user', thread: 'shared-thread' } } },
      },
    );
    const signalRun = await secondRunPromise;
    await expect(signalResult.accepted).resolves.toMatchObject({ action: 'wake', runId: signalRun.value.runId });
    expect(signalResult.signal.id).toBeDefined();
    expect(signalRun.value.text).toBe('shared response');

    subscription.unsubscribe();
  });

  it('drains multiple user-message signals into an active same-agent thread run without merging them into users', async () => {
    let releaseFirst!: () => void;
    const firstFinished = new Promise<void>(resolve => {
      releaseFirst = resolve;
    });
    let releaseSecond!: () => void;
    const secondFinished = new Promise<void>(resolve => {
      releaseSecond = resolve;
    });
    let streamCount = 0;
    const prompts: any[][] = [];

    const model = new MockLanguageModelV2({
      doStream: async ({ prompt }) => {
        streamCount += 1;
        const callIndex = streamCount;
        prompts.push(prompt);
        const responseText =
          callIndex === 1 ? 'first response' : callIndex === 2 ? 'first signal response' : 'second signal response';

        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: new ReadableStream({
            async start(controller) {
              controller.enqueue({ type: 'stream-start', warnings: [] });
              controller.enqueue({
                type: 'response-metadata',
                id: `id-${callIndex}`,
                modelId: 'mock-model-id',
                timestamp: new Date(0),
              });
              controller.enqueue({ type: 'text-start', id: `text-${callIndex}` });
              controller.enqueue({ type: 'text-delta', id: `text-${callIndex}`, delta: responseText });
              controller.enqueue({ type: 'text-end', id: `text-${callIndex}` });
              if (callIndex === 1) {
                await firstFinished;
              }
              if (callIndex === 2) {
                await secondFinished;
              }
              controller.enqueue({
                type: 'finish',
                finishReason: 'stop',
                usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
              });
              controller.close();
            },
          }),
        };
      },
    });

    const memory = new MockMemory();
    const agent = new Agent({
      id: 'active-signal-agent',
      name: 'Active Signal Agent',
      instructions: 'Test',
      model,
      memory,
    });

    const subscription = await agent.subscribeToThread({
      threadId: 'active-thread',
      resourceId: 'active-user',
    });
    const iterator = subscription.stream[Symbol.asyncIterator]();
    const firstRunPromise = readNextRun(iterator);

    const stream = await agent.stream('Hello', {
      memory: { thread: 'active-thread', resource: 'active-user' },
    });
    await expect(waitForActiveRun(subscription)).resolves.toBe(stream.runId);

    const firstSignalResult = await agent.sendSignal(
      { type: 'user-message', contents: 'First signal while running' },
      { resourceId: 'active-user', threadId: 'active-thread' },
    );
    await expect(firstSignalResult.accepted).resolves.toMatchObject({ action: 'deliver', runId: stream.runId });
    expect(firstSignalResult.signal.id).toBeDefined();

    releaseFirst();
    await waitForCondition(() => streamCount === 2);

    const secondSignalResult = await agent.sendSignal(
      { type: 'user-message', contents: 'Second signal while running' },
      { resourceId: 'active-user', threadId: 'active-thread' },
    );
    await expect(secondSignalResult.accepted).resolves.toMatchObject({ action: 'deliver', runId: stream.runId });
    expect(secondSignalResult.signal.id).toBeDefined();
    expect(secondSignalResult.signal.id).not.toBe(firstSignalResult.signal.id);

    releaseSecond();
    const firstRun = await firstRunPromise;
    expect(firstRun.value.text).toBe('first responsefirst signal responsesecond signal response');
    expect(streamCount).toBe(3);
    expect(JSON.stringify(prompts[1])).toContain('First signal while running');
    expect(JSON.stringify(prompts[1])).not.toContain('Second signal while running');
    expect(JSON.stringify(prompts[2])).toContain('First signal while running');
    expect(JSON.stringify(prompts[2])).toContain('Second signal while running');

    await stream.consumeStream();
    const recalled = await memory.recall({ threadId: 'active-thread', resourceId: 'active-user' });
    expect(recalled.messages.map(message => message.role)).toEqual([
      'user',
      'assistant',
      'signal',
      'assistant',
      'signal',
      'assistant',
    ]);
    expect(recalled.messages.map(message => message.content.parts.map(part => part.type))).toEqual([
      ['text'],
      ['text'],
      ['text'],
      ['text'],
      ['text'],
      ['text'],
    ]);
    expect(
      recalled.messages.map(message =>
        message.content.parts.map(part => (part.type === 'text' ? part.text : '')).join(''),
      ),
    ).toEqual([
      'Hello',
      'first response',
      'First signal while running',
      'first signal response',
      'Second signal while running',
      'second signal response',
    ]);

    const [userMessage, firstAssistant, firstSignal, secondAssistant, secondSignal, thirdAssistant] = recalled.messages;
    expect(firstSignal.id).toBe(firstSignalResult.signal.id);
    expect(secondSignal.id).toBe(secondSignalResult.signal.id);
    expect(firstSignal.id).not.toBe(userMessage.id);
    expect(secondSignal.id).not.toBe(userMessage.id);
    expect(firstSignal.createdAt.getTime()).toBeGreaterThan(firstAssistant.createdAt.getTime());
    expect(firstSignal.createdAt.getTime()).toBeLessThanOrEqual(secondAssistant.createdAt.getTime());
    expect(secondSignal.createdAt.getTime()).toBeGreaterThan(secondAssistant.createdAt.getTime());
    expect(secondSignal.createdAt.getTime()).toBeLessThanOrEqual(thirdAssistant.createdAt.getTime());

    const firstRecalledSignal = mastraDBMessageToSignal(firstSignal);
    const secondRecalledSignal = mastraDBMessageToSignal(secondSignal);
    expect(firstRecalledSignal.createdAt).toEqual(firstSignal.createdAt);
    expect(secondRecalledSignal.createdAt).toEqual(secondSignal.createdAt);
    expect(firstRecalledSignal.acceptedAt).toEqual(firstSignalResult.signal.acceptedAt);
    expect(secondRecalledSignal.acceptedAt).toEqual(secondSignalResult.signal.acceptedAt);

    const firstSignalMetadata = firstSignal.content.metadata?.signal as { createdAt?: string; acceptedAt?: string };
    const secondSignalMetadata = secondSignal.content.metadata?.signal as { createdAt?: string; acceptedAt?: string };
    expect(firstSignalMetadata).toMatchObject({
      createdAt: firstSignal.createdAt.toISOString(),
      acceptedAt: firstSignalResult.signal.acceptedAt?.toISOString(),
    });
    expect(secondSignalMetadata).toMatchObject({
      createdAt: secondSignal.createdAt.toISOString(),
      acceptedAt: secondSignalResult.signal.acceptedAt?.toISOString(),
    });
    expect(firstAssistant.content.metadata?.mastra).toMatchObject({ responseBoundary: true });
    expect(secondAssistant.content.metadata?.mastra).toMatchObject({ responseBoundary: true });

    subscription.unsubscribe();
  });

  it('characterizes PF-802 active signal output as one aggregate run without segment markers', async () => {
    let releaseFirst!: () => void;
    const firstFinished = new Promise<void>(resolve => {
      releaseFirst = resolve;
    });
    let streamCount = 0;

    const model = new MockLanguageModelV2({
      doStream: async () => {
        const streamIndex = ++streamCount;
        const responseText = streamIndex === 1 ? 'first response' : 'signal response';

        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: new ReadableStream({
            async start(controller) {
              controller.enqueue({ type: 'stream-start', warnings: [] });
              controller.enqueue({
                type: 'response-metadata',
                id: `id-${streamIndex}`,
                modelId: 'mock-model-id',
                timestamp: new Date(0),
              });
              controller.enqueue({ type: 'text-start', id: `text-${streamIndex}` });
              controller.enqueue({ type: 'text-delta', id: `text-${streamIndex}`, delta: responseText });
              controller.enqueue({ type: 'text-end', id: `text-${streamIndex}` });
              if (streamIndex === 1) {
                await firstFinished;
              }
              controller.enqueue({
                type: 'finish',
                finishReason: 'stop',
                usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
              });
              controller.close();
            },
          }),
        };
      },
    });

    const agent = new Agent({
      id: 'pf-802-aggregate-signal-agent',
      name: 'PF-802 Aggregate Signal Agent',
      instructions: 'Test',
      model,
    });

    const subscription = await agent.subscribeToThread({
      threadId: 'pf-802-aggregate-thread',
      resourceId: 'pf-802-aggregate-user',
    });
    try {
      const iterator = subscription.stream[Symbol.asyncIterator]();
      const runPromise = readNextRunWithParts(iterator);

      const stream = await agent.stream('Hello', {
        memory: { thread: 'pf-802-aggregate-thread', resource: 'pf-802-aggregate-user' },
      });
      await expect(waitForActiveRun(subscription)).resolves.toBe(stream.runId);

      const signalResult = await agent.sendSignal(
        { type: 'user-message', contents: 'Hello while running' },
        { resourceId: 'pf-802-aggregate-user', threadId: 'pf-802-aggregate-thread' },
      );
      expect(signalResult.runId).toBe(stream.runId);
      await expect(signalResult.accepted).resolves.toMatchObject({ action: 'deliver', runId: stream.runId });

      releaseFirst();
      const run = await runPromise;
      const textDeltas = run.value.parts.filter(part => part.type === 'text-delta');

      expect(run.value.runId).toBe(stream.runId);
      expect(run.value.text).toBe('first responsesignal response');
      expect(textDeltas.map(part => part.payload.text)).toEqual(['first response', 'signal response']);
      expect(new Set(textDeltas.map(part => part.runId))).toEqual(new Set([stream.runId]));
      const textDeltaMessageIds = textDeltas.map(part => part.messageId ?? part.payload?.messageId);
      expect(textDeltaMessageIds).toEqual([undefined, undefined]);
      expect(
        run.value.parts.map(part => ({
          segmentId: part.segmentId ?? part.payload?.segmentId,
          segmentIndex: part.segmentIndex ?? part.payload?.segmentIndex,
        })),
      ).toEqual(run.value.parts.map(() => ({ segmentId: undefined, segmentIndex: undefined })));

      await stream.consumeStream();
    } finally {
      subscription.unsubscribe();
    }
  });

  it('drops a not-yet-visible current-step tool call when draining a follow-up signal', async () => {
    const prompts: any[][] = [];
    let callCount = 0;
    let continueToToolCall!: () => void;
    const waitBeforeToolCall = new Promise<void>(resolve => {
      continueToToolCall = resolve;
    });

    const model = new MockLanguageModelV2({
      doStream: async ({ prompt }) => {
        callCount += 1;
        const callIndex = callCount;
        prompts.push(prompt);

        if (callIndex === 1) {
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: new ReadableStream({
              async start(controller) {
                controller.enqueue({ type: 'stream-start', warnings: [] });
                controller.enqueue({
                  type: 'response-metadata',
                  id: 'id-1',
                  modelId: 'mock-model-id',
                  timestamp: new Date(0),
                });
                controller.enqueue({ type: 'text-start', id: 'text-1' });
                controller.enqueue({ type: 'text-delta', id: 'text-1', delta: 'I will check' });
                await waitBeforeToolCall;
                controller.enqueue({
                  type: 'tool-call',
                  toolCallId: 'stale-tool-call',
                  toolName: 'staleTool',
                  input: '{}',
                });
                controller.enqueue({ type: 'text-end', id: 'text-1' });
                controller.enqueue({
                  type: 'finish',
                  finishReason: 'stop',
                  usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
                });
                controller.close();
              },
            }),
          };
        }

        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: convertArrayToReadableStream([
            { type: 'stream-start', warnings: [] },
            { type: 'response-metadata', id: 'id-2', modelId: 'mock-model-id', timestamp: new Date(0) },
            { type: 'text-start', id: 'text-2' },
            { type: 'text-delta', id: 'text-2', delta: 'signal response' },
            { type: 'text-end', id: 'text-2' },
            {
              type: 'finish',
              finishReason: 'stop',
              usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
            },
          ]),
        };
      },
    });

    const agent = new Agent({
      id: 'tool-interjection-signal-agent',
      name: 'Tool Interjection Signal Agent',
      instructions: 'Test',
      model,
    });

    const subscription = await agent.subscribeToThread({
      threadId: 'tool-interjection-thread',
      resourceId: 'tool-interjection-user',
    });
    const iterator = subscription.stream[Symbol.asyncIterator]();
    const chunks: any[] = [];
    const runPromise = (async () => {
      while (true) {
        const next = await iterator.next();
        if (next.done) return;
        chunks.push(next.value);
        if (next.value.type === 'finish' || next.value.type === 'error' || next.value.type === 'abort') return;
      }
    })();

    const stream = await agent.stream('Hello', {
      memory: { thread: 'tool-interjection-thread', resource: 'tool-interjection-user' },
    });
    await expect(waitForActiveRun(subscription)).resolves.toBe(stream.runId);

    const signalResult = await agent.sendSignal(
      { type: 'user-message', contents: 'Actually stop and answer this instead' },
      { resourceId: 'tool-interjection-user', threadId: 'tool-interjection-thread' },
    );
    await expect(signalResult.accepted).resolves.toMatchObject({ action: 'deliver', runId: stream.runId });

    continueToToolCall();
    await waitForCondition(() => callCount === 2);
    await runPromise;
    await stream._waitUntilFinished();

    expect(chunks.map(chunk => chunk.type)).not.toContain('tool-call');
    expect(JSON.stringify(prompts[1])).toContain('Actually stop and answer this instead');
    expect(JSON.stringify(prompts[1])).not.toContain('stale-tool-call');

    subscription.unsubscribe();
  });

  it('interrupts an active reasoning stream to drain thread-targeted follow-up signals', async () => {
    const prompts: any[][] = [];
    let callCount = 0;
    let releaseReasoningChunk: (() => void) | undefined;
    let finishFirstCall: (() => void) | undefined;

    const model = new MockLanguageModelV2({
      doStream: async ({ prompt }) => {
        callCount += 1;
        const callIndex = callCount;
        prompts.push(prompt);

        if (callIndex === 1) {
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: new ReadableStream({
              async start(controller) {
                controller.enqueue({ type: 'stream-start', warnings: [] });
                controller.enqueue({
                  type: 'response-metadata',
                  id: 'id-1',
                  modelId: 'mock-model-id',
                  timestamp: new Date(0),
                });
                controller.enqueue({ type: 'reasoning-start', id: 'reasoning-1' });
                controller.enqueue({ type: 'reasoning-delta', id: 'reasoning-1', delta: 'thinking' });
                await new Promise<void>(resolve => (releaseReasoningChunk = resolve));
                controller.enqueue({ type: 'reasoning-delta', id: 'reasoning-1', delta: ' still thinking' });
                await new Promise<void>(resolve => (finishFirstCall = resolve));
                controller.enqueue({ type: 'reasoning-end', id: 'reasoning-1' });
                controller.enqueue({
                  type: 'finish',
                  finishReason: 'stop',
                  usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
                });
                controller.close();
              },
            }),
          };
        }

        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: convertArrayToReadableStream([
            { type: 'stream-start', warnings: [] },
            { type: 'response-metadata', id: 'id-2', modelId: 'mock-model-id', timestamp: new Date(0) },
            { type: 'text-start', id: 'text-1' },
            { type: 'text-delta', id: 'text-1', delta: 'signal response' },
            { type: 'text-end', id: 'text-1' },
            {
              type: 'finish',
              finishReason: 'stop',
              usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
            },
          ]),
        };
      },
    });

    const agent = new Agent({
      id: 'interleaved-reasoning-signal-agent',
      name: 'Interleaved Reasoning Signal Agent',
      instructions: 'Test',
      model,
    });

    const subscription = await agent.subscribeToThread({
      threadId: 'interleaved-reasoning-thread',
      resourceId: 'interleaved-reasoning-user',
    });
    const iterator = subscription.stream[Symbol.asyncIterator]();
    const runPromise = readNextRun(iterator);

    const stream = await agent.stream('Hello', {
      memory: { thread: 'interleaved-reasoning-thread', resource: 'interleaved-reasoning-user' },
    });
    await expect(waitForActiveRun(subscription)).resolves.toBe(stream.runId);
    await waitForCondition(() => !!releaseReasoningChunk);

    const signalResult = await agent.sendSignal(
      { type: 'user-message', contents: 'Stop reasoning and answer this' },
      { resourceId: 'interleaved-reasoning-user', threadId: 'interleaved-reasoning-thread' },
    );
    await expect(signalResult.accepted).resolves.toMatchObject({ action: 'deliver', runId: stream.runId });

    releaseReasoningChunk?.();
    await waitForCondition(() => !!finishFirstCall);
    finishFirstCall?.();
    await waitForCondition(() => callCount === 2);

    const run = await runPromise;
    expect(run.value.text).toContain('signal response');
    expect(JSON.stringify(prompts[1])).toContain('Stop reasoning and answer this');

    subscription.unsubscribe();
  });

  it('drains thread-targeted follow-up signals into an idle-started run before the run record exists', async () => {
    const prompts: any[][] = [];

    const model = new MockLanguageModelV2({
      doStream: async ({ prompt }) => {
        prompts.push(prompt);

        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: convertArrayToReadableStream([
            { type: 'stream-start', warnings: [] },
            { type: 'response-metadata', id: 'id-0', modelId: 'mock-model-id', timestamp: new Date(0) },
            { type: 'text-start', id: 'text-1' },
            { type: 'text-delta', id: 'text-1', delta: 'response' },
            { type: 'text-end', id: 'text-1' },
            {
              type: 'finish',
              finishReason: 'stop',
              usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
            },
          ]),
        };
      },
    });

    const agent = new Agent({
      id: 'idle-start-thread-target-agent',
      name: 'Idle Start Thread Target Agent',
      instructions: 'Test',
      model,
    });

    const subscription = await agent.subscribeToThread({
      threadId: 'idle-start-thread',
      resourceId: 'idle-start-user',
    });
    const iterator = subscription.stream[Symbol.asyncIterator]();
    const runPromise = readNextRun(iterator);

    const firstSignal = await agent.sendSignal(
      { type: 'user-message', contents: 'start idle stream' },
      {
        resourceId: 'idle-start-user',
        threadId: 'idle-start-thread',
        ifIdle: { streamOptions: { memory: { resource: 'idle-start-user', thread: 'idle-start-thread' } } },
      },
    );

    const followUp = await agent.sendSignal(
      { type: 'user-message', contents: 'thread targeted follow up' },
      {
        resourceId: 'idle-start-user',
        threadId: 'idle-start-thread',
        ifIdle: { streamOptions: { memory: { resource: 'idle-start-user', thread: 'idle-start-thread' } } },
      },
    );

    const firstAccepted = await firstSignal.accepted;
    const followUpAccepted = await followUp.accepted;
    const firstRunId = 'runId' in firstAccepted ? firstAccepted.runId : undefined;
    const followUpRunId = 'runId' in followUpAccepted ? followUpAccepted.runId : undefined;
    expect(firstAccepted.action).toBe('wake');
    expect(followUpRunId).toBe(firstRunId);

    const run = await runPromise;
    expect(run.value.runId).toBe(firstRunId);
    expect(run.value.text).toBe('response');
    expect(prompts).toHaveLength(1);
    expect(JSON.stringify(prompts[0])).toContain('thread targeted follow up');

    subscription.unsubscribe();
  });

  it('completes a signal-started run that no caller subscribes to or consumes', async () => {
    // Regression: a fire-and-forget wake (e.g. an agent schedule) starts a thread run
    // but never subscribes to or consumes the returned stream. The runtime must
    // still drive the stream to completion on its own so the run reaches a
    // terminal state and its active-run record releases. If it does not, the
    // thread stays wedged and every later signal coalesces into the stuck run.
    const agent = new Agent({
      id: 'unconsumed-wake-agent',
      name: 'Unconsumed Wake Agent',
      instructions: 'Test',
      model: createTextStreamModel('unconsumed response'),
    });

    const resourceId = 'unconsumed-wake-user';
    const threadId = 'unconsumed-wake-thread';

    // Wake the thread without subscribing or consuming the resulting stream.
    const accepted = await agent.sendSignal(
      { type: 'user-message', contents: 'wake without a consumer' },
      {
        resourceId,
        threadId,
        ifIdle: { streamOptions: { memory: { resource: resourceId, thread: threadId } } },
      },
    ).accepted;
    expect(accepted.action).toBe('wake');
    const runId = 'runId' in accepted ? accepted.runId : undefined;
    expect(runId).toBeTruthy();
    expect(agent.getActiveThreadRunId({ resourceId, threadId })).toBe(runId);

    // With no consumer, the run must still finish and release the active-run record.
    await waitForCondition(() => agent.getActiveThreadRunId({ resourceId, threadId }) === undefined, 2000);
    expect(agent.getActiveThreadRunId({ resourceId, threadId })).toBeUndefined();

    // A follow-up wake now starts a fresh run rather than coalescing into a stuck one.
    const followUp = await agent.sendSignal(
      { type: 'user-message', contents: 'second wake after first completed' },
      {
        resourceId,
        threadId,
        ifIdle: { streamOptions: { memory: { resource: resourceId, thread: threadId } } },
      },
    ).accepted;
    expect(followUp.action).toBe('wake');
    const followUpRunId = 'runId' in followUp ? followUp.runId : undefined;
    expect(followUpRunId).not.toBe(runId);
  });

  it('preserves active interjections sent immediately after repeated idle signal-started runs', async () => {
    const releaseInitialCalls: Array<() => void> = [];
    const prompts: any[][] = [];
    let callCount = 0;

    const model = new MockLanguageModelV2({
      doStream: async ({ prompt }) => {
        callCount += 1;
        const callIndex = callCount;
        prompts.push(prompt);

        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: new ReadableStream({
            async start(controller) {
              controller.enqueue({ type: 'stream-start', warnings: [] });
              controller.enqueue({
                type: 'response-metadata',
                id: `id-${callIndex}`,
                modelId: 'mock-model-id',
                timestamp: new Date(0),
              });
              controller.enqueue({ type: 'text-start', id: 'text-1' });
              controller.enqueue({ type: 'text-delta', id: 'text-1', delta: `response ${callIndex}` });
              controller.enqueue({ type: 'text-end', id: 'text-1' });
              if (callIndex === 1 || callIndex === 2) {
                await new Promise<void>(resolve => releaseInitialCalls.push(resolve));
              }
              controller.enqueue({
                type: 'finish',
                finishReason: 'stop',
                usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
              });
              controller.close();
            },
          }),
        };
      },
    });

    const agent = new Agent({
      id: 'repeated-idle-signal-agent',
      name: 'Repeated Idle Signal Agent',
      instructions: 'Test',
      model,
    });

    const subscription = await agent.subscribeToThread({
      threadId: 'repeated-idle-thread',
      resourceId: 'repeated-idle-user',
    });
    const iterator = subscription.stream[Symbol.asyncIterator]();

    const firstRunPromise = readNextRun(iterator);
    const firstIdle = await agent.sendSignal(
      { type: 'user-message', contents: 'start first idle stream' },
      {
        resourceId: 'repeated-idle-user',
        threadId: 'repeated-idle-thread',
        ifIdle: { streamOptions: { memory: { resource: 'repeated-idle-user', thread: 'repeated-idle-thread' } } },
      },
    );
    await agent.sendSignal(
      { type: 'user-message', contents: 'first active interjection' },
      { runId: firstIdle.runId, resourceId: 'repeated-idle-user', threadId: 'repeated-idle-thread' },
    );
    while (releaseInitialCalls.length < 1) await nextTick();
    releaseInitialCalls.shift()?.();
    const firstRun = await firstRunPromise;
    expect(firstRun.value.text).toBe('response 1');
    expect(JSON.stringify(prompts[0])).toContain('first active interjection');

    const secondRunPromise = readNextRun(iterator);
    const secondIdle = await agent.sendSignal(
      { type: 'user-message', contents: 'start second idle stream' },
      {
        resourceId: 'repeated-idle-user',
        threadId: 'repeated-idle-thread',
        ifIdle: { streamOptions: { memory: { resource: 'repeated-idle-user', thread: 'repeated-idle-thread' } } },
      },
    );
    await agent.sendSignal(
      { type: 'user-message', contents: 'second active interjection' },
      { runId: secondIdle.runId, resourceId: 'repeated-idle-user', threadId: 'repeated-idle-thread' },
    );
    while (releaseInitialCalls.length < 1) await nextTick();
    releaseInitialCalls.shift()?.();
    const secondRun = await secondRunPromise;
    expect(secondRun.value.text).toBe('response 2');
    expect(JSON.stringify(prompts[1])).toContain('second active interjection');

    subscription.unsubscribe();
  });

  it('queues a signal from another agent until the active thread run finishes', async () => {
    let releaseFirst!: () => void;
    const firstFinished = new Promise<void>(resolve => {
      releaseFirst = resolve;
    });
    let firstStarted = false;
    let secondStarted = false;

    const firstAgent = new Agent({
      id: 'cross-agent-a',
      name: 'Cross Agent A',
      instructions: 'Test',
      model: new MockLanguageModelV2({
        doStream: async () => {
          firstStarted = true;
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: new ReadableStream({
              async start(controller) {
                controller.enqueue({ type: 'stream-start', warnings: [] });
                controller.enqueue({
                  type: 'response-metadata',
                  id: 'cross-a',
                  modelId: 'mock-model-id',
                  timestamp: new Date(0),
                });
                controller.enqueue({ type: 'text-start', id: 'text-1' });
                controller.enqueue({ type: 'text-delta', id: 'text-1', delta: 'first response' });
                controller.enqueue({ type: 'text-end', id: 'text-1' });
                await firstFinished;
                controller.enqueue({
                  type: 'finish',
                  finishReason: 'stop',
                  usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
                });
                controller.close();
              },
            }),
          };
        },
      }),
    });
    const secondAgent = new Agent({
      id: 'cross-agent-b',
      name: 'Cross Agent B',
      instructions: 'Test',
      model: new MockLanguageModelV2({
        doStream: async () => {
          secondStarted = true;
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: convertArrayToReadableStream([
              { type: 'stream-start', warnings: [] },
              { type: 'response-metadata', id: 'cross-b', modelId: 'mock-model-id', timestamp: new Date(0) },
              { type: 'text-start', id: 'text-1' },
              { type: 'text-delta', id: 'text-1', delta: 'second response' },
              { type: 'text-end', id: 'text-1' },
              {
                type: 'finish',
                finishReason: 'stop',
                usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
              },
            ]),
          };
        },
      }),
    });
    new Mastra({ agents: { firstAgent, secondAgent }, logger: false });

    const subscription = await firstAgent.subscribeToThread({
      threadId: 'cross-agent-thread',
      resourceId: 'cross-agent-user',
    });
    const iterator = subscription.stream[Symbol.asyncIterator]();
    const firstRunPromise = readNextRun(iterator);

    const firstStream = await firstAgent.stream('Hello', {
      memory: { thread: 'cross-agent-thread', resource: 'cross-agent-user' },
    });
    const firstText = firstStream.text;
    await waitForCondition(() => firstStarted);
    expect(firstStarted).toBe(true);

    const signalResult = await secondAgent.sendSignal(
      { type: 'user-message', contents: 'Hello from another agent' },
      {
        resourceId: 'cross-agent-user',
        threadId: 'cross-agent-thread',
        ifIdle: { streamOptions: { memory: { resource: 'cross-agent-user', thread: 'cross-agent-thread' } } },
      },
    );
    await nextTick();
    expect(secondStarted).toBe(false);

    releaseFirst();
    await expect(firstText).resolves.toBe('first response');
    await expect(firstRunPromise).resolves.toMatchObject({ value: { runId: firstStream.runId }, done: false });

    const signalAccepted = await signalResult.accepted;
    const signalRunId = 'runId' in signalAccepted ? signalAccepted.runId : undefined;
    const secondRun = await readNextRun(iterator);
    expect(secondRun.value.runId).toBe(signalRunId);
    expect(secondRun.value.text).toBe('second response');
    expect(secondStarted).toBe(true);

    subscription.unsubscribe();
  });

  it('preserves caller-provided runId for idle wake signals', async () => {
    const agent = new Agent({
      id: 'caller-run-id-agent',
      name: 'Caller Run Id Agent',
      instructions: 'Test',
      model: createTextStreamModel('caller run response'),
    });
    const subscription = await agent.subscribeToThread({
      resourceId: 'caller-run-user',
      threadId: 'caller-run-thread',
    });

    const signalResult = await agent.sendSignal(
      { type: 'user-message', contents: 'wake with caller id' },
      {
        runId: 'caller-provided-run',
        resourceId: 'caller-run-user',
        threadId: 'caller-run-thread',
        ifIdle: { streamOptions: { memory: { resource: 'caller-run-user', thread: 'caller-run-thread' } } },
      },
    );

    expect(signalResult.runId).toBe('caller-provided-run');
    await expect(signalResult.accepted).resolves.toMatchObject({ action: 'wake', runId: 'caller-provided-run' });
    await expect(readNextRun(subscription.stream[Symbol.asyncIterator]())).resolves.toMatchObject({
      value: { runId: 'caller-provided-run', text: 'caller run response' },
      done: false,
    });

    subscription.unsubscribe();
  });

  it('runs idle wake rejection cleanup when a queued idle stream fails', async () => {
    const runtime = new AgentThreadStreamRuntime();
    let finishActive!: () => void;
    const activeFinished = new Promise<void>(resolve => {
      finishActive = resolve;
    });

    const completion = runtime.registerRun(
      { id: 'active-agent' } as any,
      {
        runId: 'active-run',
        status: 'running',
        _waitUntilFinished: () => activeFinished,
      } as any,
      {
        runId: 'active-run',
        memory: { resource: 'queued-failure-user', thread: 'queued-failure-thread' },
      } as any,
    );
    const cleanup = vi.fn();
    const stream = vi.fn(async () => {
      throw new Error('queued idle stream failed');
    });

    const result = runtime.sendSignal(
      { id: 'queued-idle-agent', stream } as any,
      { type: 'user-message', contents: 'queued wake' },
      {
        resourceId: 'queued-failure-user',
        threadId: 'queued-failure-thread',
        ifIdle: {
          streamOptions: { memory: { resource: 'queued-failure-user', thread: 'queued-failure-thread' } },
          _onThreadStreamRunRejected: cleanup,
        } as any,
      },
    );

    expect(result.output).toBeUndefined();
    expect(cleanup).not.toHaveBeenCalled();

    finishActive();
    await completion;

    expect(cleanup).toHaveBeenCalledTimes(1);
    expect(stream).toHaveBeenCalledWith(
      expect.objectContaining({ contents: 'queued wake' }),
      expect.objectContaining({
        runId: result.runId,
        memory: { resource: 'queued-failure-user', thread: 'queued-failure-thread' },
      }),
    );
  });

  it('wakes reservation waiters when a queued idle stream fails', async () => {
    const runtime = new AgentThreadStreamRuntime();
    let finishActive!: () => void;
    const activeFinished = new Promise<void>(resolve => {
      finishActive = resolve;
    });
    let rejectStream!: (error: Error) => void;

    const completion = runtime.registerRun(
      { id: 'active-agent' } as any,
      {
        runId: 'active-run',
        status: 'running',
        _waitUntilFinished: () => activeFinished,
      } as any,
      {
        runId: 'active-run',
        memory: { resource: 'queued-waiter-user', thread: 'queued-waiter-thread' },
      } as any,
    );
    const stream = vi.fn(
      () =>
        new Promise((_resolve, reject) => {
          rejectStream = reject;
        }),
    );

    runtime.sendSignal(
      { id: 'queued-idle-agent', stream } as any,
      { type: 'user-message', contents: 'queued wake' },
      {
        resourceId: 'queued-waiter-user',
        threadId: 'queued-waiter-thread',
        ifIdle: {
          streamOptions: { memory: { resource: 'queued-waiter-user', thread: 'queued-waiter-thread' } },
        } as any,
      },
    );

    finishActive();
    await nextTick();
    expect(stream).toHaveBeenCalledTimes(1);

    let waiterResolved = false;
    const waiter = runtime
      .waitForCrossAgentThreadRun(
        { id: 'other-agent' } as any,
        {
          runId: 'other-run',
          memory: { resource: 'queued-waiter-user', thread: 'queued-waiter-thread' },
        } as any,
      )
      .then(() => {
        waiterResolved = true;
      });
    await nextTick();
    expect(waiterResolved).toBe(false);

    rejectStream(new Error('queued idle stream failed'));
    await completion;
    await waiter;
    expect(waiterResolved).toBe(true);
  });

  it('does not reserve queued idle streams before preflight when reservation is deferred', async () => {
    const runtime = new AgentThreadStreamRuntime();
    let finishActive!: () => void;
    const activeFinished = new Promise<void>(resolve => {
      finishActive = resolve;
    });
    let rejectFirstStream!: (error: Error) => void;

    const completion = runtime.registerRun(
      { id: 'active-agent' } as any,
      {
        runId: 'active-run',
        status: 'running',
        _waitUntilFinished: () => activeFinished,
      } as any,
      {
        runId: 'active-run',
        memory: { resource: 'queued-deferred-user', thread: 'queued-deferred-thread' },
      } as any,
    );
    const stream = vi
      .fn()
      .mockImplementationOnce(
        () =>
          new Promise((_resolve, reject) => {
            rejectFirstStream = reject;
          }),
      )
      .mockResolvedValueOnce({ runId: 'queued-deferred-second-run' })
      .mockResolvedValueOnce({ runId: 'queued-deferred-retry-run' });

    const firstResult = runtime.sendSignal(
      { id: 'queued-deferred-agent', stream } as any,
      { id: 'queued-deferred-signal', type: 'user-message', contents: 'queued deferred wake' },
      {
        resourceId: 'queued-deferred-user',
        threadId: 'queued-deferred-thread',
        ifIdle: {
          _skipThreadRunReservationBeforePreflight: true,
          streamOptions: { memory: { resource: 'queued-deferred-user', thread: 'queued-deferred-thread' } },
        } as any,
      },
    );
    runtime.sendSignal(
      { id: 'queued-deferred-agent', stream } as any,
      { type: 'user-message', contents: 'queued second deferred wake' },
      {
        resourceId: 'queued-deferred-user',
        threadId: 'queued-deferred-thread',
        ifIdle: {
          _skipThreadRunReservationBeforePreflight: true,
          streamOptions: { memory: { resource: 'queued-deferred-user', thread: 'queued-deferred-thread' } },
        } as any,
      },
    );

    finishActive();
    await nextTick();
    expect(stream).toHaveBeenCalledWith(
      expect.objectContaining({ contents: 'queued deferred wake' }),
      expect.not.objectContaining({ _threadRunReservationOwner: true }),
    );

    let waiterResolved = false;
    await runtime
      .waitForCrossAgentThreadRun(
        { id: 'other-agent' } as any,
        {
          runId: 'other-run',
          memory: { resource: 'queued-deferred-user', thread: 'queued-deferred-thread' },
        } as any,
      )
      .then(() => {
        waiterResolved = true;
      });
    expect(waiterResolved).toBe(true);

    rejectFirstStream(new Error('queued deferred idle stream failed'));
    await completion;
    expect(stream).toHaveBeenCalledWith(
      expect.objectContaining({ contents: 'queued second deferred wake' }),
      expect.not.objectContaining({ _threadRunReservationOwner: true }),
    );
    expect(stream).toHaveBeenCalledTimes(2);

    const retryResult = runtime.sendSignal(
      { id: 'queued-deferred-agent', stream } as any,
      { id: 'queued-deferred-signal', type: 'user-message', contents: 'queued deferred wake' },
      {
        resourceId: 'queued-deferred-user',
        threadId: 'queued-deferred-thread',
        ifIdle: {
          _skipThreadRunReservationBeforePreflight: true,
          streamOptions: { memory: { resource: 'queued-deferred-user', thread: 'queued-deferred-thread' } },
        } as any,
      },
    );
    expect(retryResult.runId).not.toBe(firstResult.runId);
    await expect(retryResult.accepted).resolves.toMatchObject({ action: 'wake', runId: retryResult.runId });
    expect(stream).toHaveBeenCalledTimes(3);
  });

  it('aborts queued deferred idle streams after they start preflight without reservation', async () => {
    const runtime = new AgentThreadStreamRuntime();
    let finishActive!: () => void;
    const activeFinished = new Promise<void>(resolve => {
      finishActive = resolve;
    });
    let rejectStream!: (error: Error) => void;

    const completion = runtime.registerRun(
      { id: 'active-agent' } as any,
      {
        runId: 'active-run',
        status: 'running',
        _waitUntilFinished: () => activeFinished,
      } as any,
      {
        runId: 'active-run',
        memory: { resource: 'queued-deferred-abort-user', thread: 'queued-deferred-abort-thread' },
      } as any,
    );
    const stream = vi.fn(
      () =>
        new Promise((_resolve, reject) => {
          rejectStream = reject;
        }),
    );

    const result = runtime.sendSignal(
      { id: 'queued-deferred-abort-agent', stream } as any,
      { type: 'user-message', contents: 'queued deferred abort wake' },
      {
        resourceId: 'queued-deferred-abort-user',
        threadId: 'queued-deferred-abort-thread',
        ifIdle: {
          _skipThreadRunReservationBeforePreflight: true,
          streamOptions: { memory: { resource: 'queued-deferred-abort-user', thread: 'queued-deferred-abort-thread' } },
        } as any,
      },
    );

    finishActive();
    await nextTick();
    expect(stream).toHaveBeenCalledTimes(1);

    const waiter = runtime.waitForRunOutput(result.runId);
    expect(runtime.abortRun(result.runId)).toBe(true);
    await expect(waiter).rejects.toThrow('has been aborted');

    rejectStream(new Error('queued deferred abort stream stopped'));
    await completion;
  });

  it('aborts immediate deferred idle streams while preflight is pending', async () => {
    const runtime = new AgentThreadStreamRuntime();
    let rejectStream!: (error: Error) => void;
    const stream = vi.fn(
      () =>
        new Promise((_resolve, reject) => {
          rejectStream = reject;
        }),
    );

    const result = runtime.sendSignal(
      { id: 'immediate-deferred-abort-agent', stream } as any,
      { type: 'user-message', contents: 'immediate deferred abort wake' },
      {
        resourceId: 'immediate-deferred-abort-user',
        threadId: 'immediate-deferred-abort-thread',
        ifIdle: {
          _skipThreadRunReservationBeforePreflight: true,
          streamOptions: {
            memory: { resource: 'immediate-deferred-abort-user', thread: 'immediate-deferred-abort-thread' },
          },
        } as any,
      },
    );
    void result.output?.catch(() => {});
    await waitForCondition(() => stream.mock.calls.length === 1);
    expect(stream).toHaveBeenCalledTimes(1);

    const waiter = runtime.waitForRunOutput(result.runId);
    expect(runtime.abortRun(result.runId)).toBe(true);
    await expect(waiter).rejects.toThrow('has been aborted');

    rejectStream(new Error('immediate deferred abort stream stopped'));
    await expect(result.output).rejects.toThrow('immediate deferred abort stream stopped');
  });

  it('blocks direct reservations while a deferred idle run id is inflight', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new EventEmitterPubSub();
    let rejectStream!: (error: Error) => void;
    const stream = vi.fn(
      () =>
        new Promise((_resolve, reject) => {
          rejectStream = reject;
        }),
    );

    const result = runtime.sendSignal(
      { id: 'inflight-deferred-owner-agent', stream } as any,
      { type: 'user-message', contents: 'inflight deferred wake' },
      {
        runId: 'inflight-deferred-run',
        resourceId: 'inflight-deferred-user',
        threadId: 'inflight-deferred-thread',
        ifIdle: {
          _skipThreadRunReservationBeforePreflight: true,
          streamOptions: { memory: { resource: 'inflight-deferred-user', thread: 'inflight-deferred-thread' } },
        } as any,
      },
      pubsub,
    );
    await waitForCondition(() => stream.mock.calls.length === 1);
    expect(stream).toHaveBeenCalledTimes(1);

    expect(() =>
      runtime.reserveRun(
        {
          runId: 'inflight-deferred-run',
          memory: { resource: 'inflight-deferred-user', thread: 'inflight-deferred-thread' },
        } as any,
        pubsub,
      ),
    ).toThrow('already reserved');

    const duplicateOutput = buildFakeOutput({
      runId: 'inflight-deferred-run',
      fullOutput: { text: 'duplicate response', finishReason: 'stop', usage: {} },
      chunks: [{ runId: 'inflight-deferred-run', type: 'finish', payload: {} }],
    });
    expect(() =>
      runtime.registerRun(
        { id: 'duplicate-inflight-agent' } as any,
        duplicateOutput,
        {
          runId: 'inflight-deferred-run',
          memory: { resource: 'inflight-deferred-user', thread: 'inflight-deferred-thread' },
        } as any,
        pubsub,
      ),
    ).toThrow('already reserved');

    rejectStream(new Error('inflight deferred stream stopped'));
    await expect(result.output).rejects.toThrow('inflight deferred stream stopped');
  });

  it('keeps deferred idle run ids inflight when owner reservation waits for an active thread', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new EventEmitterPubSub();
    let rejectStream!: (error: Error) => void;
    const stream = vi.fn((_signal, options) => {
      runtime.registerRun(
        { id: 'inflight-owner-blocking-agent' } as any,
        {
          runId: 'inflight-owner-blocking-active-run',
          status: 'running',
          fullStream: (async function* () {})(),
          _waitUntilFinished: () => new Promise<void>(() => {}),
        } as any,
        {
          runId: 'inflight-owner-blocking-active-run',
          memory: { resource: 'inflight-owner-blocked-user', thread: 'inflight-owner-blocked-thread' },
        } as any,
        pubsub,
      );
      expect(runtime.reserveRun(options as any, pubsub, 'inflight-owner-blocked-agent')).toBeUndefined();
      return new Promise((_resolve, reject) => {
        rejectStream = reject;
      });
    });

    const result = runtime.sendSignal(
      { id: 'inflight-owner-blocked-agent', stream } as any,
      { type: 'user-message', contents: 'inflight owner blocked wake' },
      {
        runId: 'inflight-owner-blocked-run',
        resourceId: 'inflight-owner-blocked-user',
        threadId: 'inflight-owner-blocked-thread',
        ifIdle: {
          _skipThreadRunReservationBeforePreflight: true,
          streamOptions: {
            memory: { resource: 'inflight-owner-blocked-user', thread: 'inflight-owner-blocked-thread' },
          },
        } as any,
      },
      pubsub,
    );
    await waitForCondition(() => stream.mock.calls.length === 1);
    expect(stream).toHaveBeenCalledTimes(1);

    expect(() =>
      runtime.reserveRun(
        {
          runId: 'inflight-owner-blocked-run',
          memory: { resource: 'inflight-owner-blocked-user', thread: 'inflight-owner-blocked-thread' },
        } as any,
        pubsub,
      ),
    ).toThrow('already reserved');
    expect(runtime.abortRun(result.runId, pubsub)).toBe(true);

    rejectStream(new Error('inflight owner blocked stream stopped'));
    await expect(result.output).rejects.toThrow('inflight owner blocked stream stopped');
  });

  it('wakes reservation waiters when an immediate idle stream fails', async () => {
    const runtime = new AgentThreadStreamRuntime();
    let rejectStream!: (error: Error) => void;
    const stream = vi
      .fn()
      .mockImplementationOnce(
        () =>
          new Promise((_resolve, reject) => {
            rejectStream = reject;
          }),
      )
      .mockResolvedValueOnce({ runId: 'immediate-retry-run' });

    const result = runtime.sendSignal(
      { id: 'immediate-idle-agent', stream } as any,
      { id: 'immediate-wake-signal', type: 'user-message', contents: 'immediate wake' },
      {
        resourceId: 'immediate-waiter-user',
        threadId: 'immediate-waiter-thread',
        ifIdle: {
          streamOptions: { memory: { resource: 'immediate-waiter-user', thread: 'immediate-waiter-thread' } },
        } as any,
      },
    );
    await waitForCondition(() => stream.mock.calls.length === 1);
    expect(stream).toHaveBeenCalledTimes(1);

    let waiterResolved = false;
    const waiter = runtime
      .waitForCrossAgentThreadRun(
        { id: 'other-agent' } as any,
        {
          runId: 'other-run',
          memory: { resource: 'immediate-waiter-user', thread: 'immediate-waiter-thread' },
        } as any,
      )
      .then(() => {
        waiterResolved = true;
      });
    await nextTick();
    expect(waiterResolved).toBe(false);

    rejectStream(new Error('immediate idle stream failed'));
    await expect(result.output).rejects.toThrow('immediate idle stream failed');
    await expect(runtime.waitForRunOutput(result.runId)).rejects.toThrow('was rejected');
    await waiter;
    expect(waiterResolved).toBe(true);

    const retryResult = runtime.sendSignal(
      { id: 'immediate-idle-agent', stream } as any,
      { id: 'immediate-wake-signal', type: 'user-message', contents: 'immediate wake' },
      {
        resourceId: 'immediate-waiter-user',
        threadId: 'immediate-waiter-thread',
        ifIdle: {
          streamOptions: { memory: { resource: 'immediate-waiter-user', thread: 'immediate-waiter-thread' } },
        } as any,
      },
    );
    expect(retryResult.runId).not.toBe(result.runId);
    await expect(retryResult.accepted).resolves.toMatchObject({ action: 'wake', runId: retryResult.runId });
    expect(stream).toHaveBeenCalledTimes(2);
  });

  it('wakes waiters and drops queued signals when a reserved setup run is released', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new EventEmitterPubSub();
    const streamOptions = {
      runId: 'reserved-setup-run',
      memory: { resource: 'reserved-setup-user', thread: 'reserved-setup-thread' },
    } as any;
    const release = runtime.reserveRun(streamOptions, pubsub);
    expect(release).toBeDefined();

    let waiterResolved = false;
    const waiter = runtime
      .waitForCrossAgentThreadRun(
        { id: 'other-agent' } as any,
        {
          runId: 'reserved-setup-run',
          memory: { resource: 'reserved-setup-user', thread: 'reserved-setup-thread' },
        } as any,
        pubsub,
      )
      .then(() => {
        waiterResolved = true;
      });
    await nextTick();
    expect(waiterResolved).toBe(false);

    const signalResult = runtime.sendSignal(
      { id: 'reserved-agent' } as any,
      { type: 'user-message', contents: 'stale setup signal' },
      {
        resourceId: 'reserved-setup-user',
        threadId: 'reserved-setup-thread',
      },
      pubsub,
    );
    expect(signalResult).toEqual(expect.objectContaining({ runId: 'reserved-setup-run' }));

    release!();
    await waiter;
    expect(waiterResolved).toBe(true);

    const laterRelease = runtime.reserveRun(
      {
        runId: 'later-run',
        memory: { resource: 'reserved-setup-user', thread: 'reserved-setup-thread' },
      } as any,
      pubsub,
    );
    expect(laterRelease).toBeDefined();
    expect(runtime.drainPendingSignals('later-run', pubsub)).toEqual([]);
    const laterSignal = runtime.sendSignal(
      { id: 'reserved-agent' } as any,
      { type: 'user-message', contents: 'successor signal' },
      {
        resourceId: 'reserved-setup-user',
        threadId: 'reserved-setup-thread',
      },
      pubsub,
    );
    expect(laterSignal).toEqual(expect.objectContaining({ runId: 'later-run' }));
    expect(runtime.drainPendingSignals('later-run', pubsub, 'pre-run')).toHaveLength(1);
    laterRelease!();
  });

  it('does not overwrite an existing run reservation with a reused run id', () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new EventEmitterPubSub();
    const release = runtime.reserveRun(
      {
        runId: 'reused-reserved-run',
        memory: { resource: 'first-reservation-user', thread: 'first-reservation-thread' },
      } as any,
      pubsub,
    );

    expect(release).toBeDefined();
    expect(() =>
      runtime.reserveRun(
        {
          runId: 'reused-reserved-run',
          memory: { resource: 'first-reservation-user', thread: 'first-reservation-thread' },
        } as any,
        pubsub,
      ),
    ).toThrow('already reserved');
    expect(() =>
      runtime.reserveRun(
        {
          runId: 'reused-reserved-run',
          memory: { resource: 'second-reservation-user', thread: 'second-reservation-thread' },
        } as any,
        pubsub,
      ),
    ).toThrow('already reserved for another thread');
    expect(
      runtime.abortThread({ resourceId: 'second-reservation-user', threadId: 'second-reservation-thread' }, pubsub),
    ).toBe(false);
    expect(
      runtime.abortThread({ resourceId: 'first-reservation-user', threadId: 'first-reservation-thread' }, pubsub),
    ).toBe(true);
  });

  it('rejects duplicate queued idle run ids before either idle wake starts', () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new EventEmitterPubSub();
    const stream = vi.fn(async () => ({
      runId: 'queued-duplicate-idle-run',
      status: 'running',
      fullStream: (async function* () {})(),
      _waitUntilFinished: async () => {},
    }));

    runtime.registerRun(
      { id: 'active-agent' } as any,
      {
        runId: 'active-before-queued-duplicate',
        status: 'running',
        _waitUntilFinished: () => new Promise<any>(() => {}),
      } as any,
      {
        runId: 'active-before-queued-duplicate',
        memory: { resource: 'queued-duplicate-user', thread: 'queued-duplicate-thread' },
      } as any,
      pubsub,
    );

    const target = {
      runId: 'queued-duplicate-idle-run',
      resourceId: 'queued-duplicate-user',
      threadId: 'queued-duplicate-thread',
      ifIdle: {
        streamOptions: { memory: { resource: 'queued-duplicate-user', thread: 'queued-duplicate-thread' } },
      },
    } as any;

    const firstQueued = runtime.sendSignal(
      { id: 'queued-duplicate-agent', stream } as any,
      { type: 'user-message', contents: 'first queued idle' },
      target,
      pubsub,
    );
    expect(firstQueued.runId).toBe('queued-duplicate-idle-run');
    expect(firstQueued.accepted).toBeInstanceOf(Promise);
    expect(() =>
      runtime.sendSignal(
        { id: 'queued-duplicate-agent', stream } as any,
        { type: 'user-message', contents: 'second queued idle' },
        target,
        pubsub,
      ),
    ).toThrow('already reserved');
    expect(() =>
      runtime.reserveRun(
        {
          runId: 'queued-duplicate-idle-run',
          memory: { resource: 'queued-duplicate-user', thread: 'queued-duplicate-thread' },
        } as any,
        pubsub,
      ),
    ).toThrow('already reserved');
    expect(stream).not.toHaveBeenCalled();
  });

  it('clears caller-signal idempotency when a queued idle run is aborted', () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new EventEmitterPubSub();
    const stream = vi.fn(async () => ({
      runId: 'unused-queued-idempotency-run',
      status: 'running',
      fullStream: (async function* () {})(),
      _waitUntilFinished: async () => {},
    }));

    runtime.registerRun(
      { id: 'active-agent' } as any,
      {
        runId: 'active-before-queued-idempotency',
        status: 'running',
        _waitUntilFinished: () => new Promise<any>(() => {}),
      } as any,
      {
        runId: 'active-before-queued-idempotency',
        memory: { resource: 'queued-idempotency-user', thread: 'queued-idempotency-thread' },
      } as any,
      pubsub,
    );

    const target = {
      resourceId: 'queued-idempotency-user',
      threadId: 'queued-idempotency-thread',
      ifIdle: {
        streamOptions: { memory: { resource: 'queued-idempotency-user', thread: 'queued-idempotency-thread' } },
      },
    } as any;
    const signal = { id: 'caller-signal-id', type: 'user-message', contents: 'retry queued idle' } as any;

    const first = runtime.sendSignal({ id: 'queued-idempotency-agent', stream } as any, signal, target, pubsub);
    expect(runtime.abortRun(first.runId, pubsub)).toBe(true);
    const second = runtime.sendSignal({ id: 'queued-idempotency-agent', stream } as any, signal, target, pubsub);

    expect(second.runId).not.toBe(first.runId);
    expect(stream).not.toHaveBeenCalled();
  });

  it('rejects run-output waiters when a queued idle run is aborted before it starts', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new EventEmitterPubSub();

    runtime.registerRun(
      { id: 'active-agent' } as any,
      {
        runId: 'active-before-waiter-abort',
        status: 'running',
        fullStream: (async function* () {})(),
        _waitUntilFinished: () => new Promise<any>(() => {}),
      } as any,
      {
        runId: 'active-before-waiter-abort',
        memory: { resource: 'queued-waiter-abort-user', thread: 'queued-waiter-abort-thread' },
      } as any,
      pubsub,
    );

    const result = runtime.sendSignal(
      { id: 'queued-waiter-abort-agent', stream: vi.fn() } as any,
      { type: 'user-message', contents: 'queued waiter abort' },
      {
        resourceId: 'queued-waiter-abort-user',
        threadId: 'queued-waiter-abort-thread',
        ifIdle: {
          streamOptions: { memory: { resource: 'queued-waiter-abort-user', thread: 'queued-waiter-abort-thread' } },
        } as any,
      },
      pubsub,
    );
    const waiter = runtime.waitForRunOutput(result.runId, pubsub);

    expect(runtime.abortRun(result.runId, pubsub)).toBe(true);
    await expect(waiter).rejects.toThrow('has been aborted');
  });

  it('does not tombstone unknown run ids when abort returns false', () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new EventEmitterPubSub();

    expect(runtime.abortRun('unknown-abort-run', pubsub)).toBe(false);
    const output = buildFakeOutput({
      runId: 'unknown-abort-run',
      fullOutput: { text: 'not aborted', finishReason: 'stop', usage: {} },
      chunks: [{ runId: 'unknown-abort-run', type: 'finish', payload: {} }],
    });
    expect(() =>
      runtime.registerRun(
        { id: 'unknown-abort-agent' } as any,
        output,
        {
          runId: 'unknown-abort-run',
          memory: { resource: 'unknown-abort-user', thread: 'unknown-abort-thread' },
        } as any,
        pubsub,
      ),
    ).not.toThrow();
  });

  it('aborts a reserved setup run before stream preparation', () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new EventEmitterPubSub();
    runtime.reserveRun(
      {
        runId: 'reserved-abort-run',
        memory: { resource: 'reserved-abort-user', thread: 'reserved-abort-thread' },
      } as any,
      pubsub,
    );

    expect(runtime.abortRun('reserved-abort-run', pubsub)).toBe(true);
    const prepared = runtime.prepareRunOptions(
      {
        runId: 'reserved-abort-run',
        memory: { resource: 'reserved-abort-user', thread: 'reserved-abort-thread' },
      } as any,
      pubsub,
    );
    expect(prepared.abortSignal?.aborted).toBe(true);
  });

  it('rejects run-output waiters and releases a prepared setup run on abort before registration', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new EventEmitterPubSub();
    runtime.reserveRun(
      {
        runId: 'prepared-setup-abort-run',
        memory: { resource: 'prepared-setup-abort-user', thread: 'prepared-setup-abort-thread' },
      } as any,
      pubsub,
    );
    runtime.prepareRunOptions(
      {
        runId: 'prepared-setup-abort-run',
        memory: { resource: 'prepared-setup-abort-user', thread: 'prepared-setup-abort-thread' },
      } as any,
      pubsub,
    );
    const waiter = runtime.waitForRunOutput('prepared-setup-abort-run', pubsub);

    expect(runtime.abortRun('prepared-setup-abort-run', pubsub)).toBe(true);
    await expect(waiter).rejects.toThrow('has been aborted');
    const successorRelease = runtime.reserveRun(
      {
        runId: 'prepared-setup-successor-run',
        memory: { resource: 'prepared-setup-abort-user', thread: 'prepared-setup-abort-thread' },
      } as any,
      pubsub,
    );
    expect(successorRelease).toBeDefined();
    const lateOutput = buildFakeOutput({
      runId: 'prepared-setup-abort-run',
      fullOutput: { text: 'late aborted response', finishReason: 'stop', usage: {} },
      chunks: [{ runId: 'prepared-setup-abort-run', type: 'finish', payload: {} }],
    });
    expect(() =>
      runtime.registerRun(
        { id: 'prepared-setup-abort-agent' } as any,
        lateOutput,
        {
          runId: 'prepared-setup-abort-run',
          memory: { resource: 'prepared-setup-abort-user', thread: 'prepared-setup-abort-thread' },
        } as any,
        pubsub,
      ),
    ).toThrow('has been aborted');
  });

  it('keeps run-output waiters when a reservation is released for non-terminal retargeting', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new EventEmitterPubSub();
    runtime.reserveRun(
      {
        runId: 'retarget-waiter-run',
        memory: { resource: 'retarget-waiter-old-user', thread: 'retarget-waiter-old-thread' },
      } as any,
      pubsub,
    );
    const waiter = runtime.waitForRunOutput('retarget-waiter-run', pubsub);
    let waiterRejected = false;
    void waiter.catch(() => {
      waiterRejected = true;
    });

    expect(
      runtime.releaseRunReservation('retarget-waiter-run', pubsub, { cleanupPrepared: true, clearAbort: true }),
    ).toBe(true);
    await nextTick();
    expect(waiterRejected).toBe(false);

    runtime.reserveRun(
      {
        runId: 'retarget-waiter-run',
        memory: { resource: 'retarget-waiter-new-user', thread: 'retarget-waiter-new-thread' },
      } as any,
      pubsub,
    );
    const output = buildFakeOutput({
      runId: 'retarget-waiter-run',
      fullOutput: { text: 'retarget waiter response', finishReason: 'stop', usage: {} },
      chunks: [{ runId: 'retarget-waiter-run', type: 'finish', payload: {} }],
    });
    runtime.registerRun(
      { id: 'retarget-waiter-agent' } as any,
      output,
      {
        runId: 'retarget-waiter-run',
        memory: { resource: 'retarget-waiter-new-user', thread: 'retarget-waiter-new-thread' },
      } as any,
      pubsub,
    );

    await expect(waiter).resolves.toBe(output);
  });

  it('cancels run-output waiters without poisoning a later registration', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new EventEmitterPubSub();
    runtime.reserveRun(
      {
        runId: 'abortable-waiter-run',
        memory: { resource: 'abortable-waiter-user', thread: 'abortable-waiter-thread' },
      } as any,
      pubsub,
    );
    const waitAbortController = new AbortController();
    const waiter = runtime.waitForRunOutput('abortable-waiter-run', pubsub, waitAbortController.signal);

    waitAbortController.abort(new Error('stop waiting'));
    await expect(waiter).rejects.toThrow('stop waiting');

    const output = buildFakeOutput({
      runId: 'abortable-waiter-run',
      fullOutput: { text: 'abortable waiter response', finishReason: 'stop', usage: {} },
      chunks: [{ runId: 'abortable-waiter-run', type: 'finish', payload: {} }],
    });
    runtime.registerRun(
      { id: 'abortable-waiter-agent' } as any,
      output,
      {
        runId: 'abortable-waiter-run',
        memory: { resource: 'abortable-waiter-user', thread: 'abortable-waiter-thread' },
      } as any,
      pubsub,
    );

    await expect(runtime.waitForRunOutput('abortable-waiter-run', pubsub)).resolves.toBe(output);
  });

  it('keeps rejected run ids tombstoned when retry cannot reserve an active thread', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new EventEmitterPubSub();
    const releaseRejected = runtime.reserveRun(
      {
        runId: 'rejected-retry-run',
        memory: { resource: 'rejected-retry-user', thread: 'rejected-retry-thread' },
      } as any,
      pubsub,
    );
    const rejectedWaiter = runtime.waitForRunOutput('rejected-retry-run', pubsub);
    releaseRejected!();
    await expect(rejectedWaiter).rejects.toThrow('was rejected');
    runtime.registerRun(
      { id: 'active-retry-agent' } as any,
      {
        runId: 'active-retry-run',
        status: 'running',
        fullStream: (async function* () {})(),
        _waitUntilFinished: () => new Promise<void>(() => {}),
      } as any,
      {
        runId: 'active-retry-run',
        memory: { resource: 'rejected-retry-user', thread: 'rejected-retry-thread' },
      } as any,
      pubsub,
    );

    expect(
      runtime.reserveRun(
        {
          runId: 'rejected-retry-run',
          memory: { resource: 'rejected-retry-user', thread: 'rejected-retry-thread' },
        } as any,
        pubsub,
      ),
    ).toBeUndefined();

    const staleOutput = buildFakeOutput({
      runId: 'rejected-retry-run',
      fullOutput: { text: 'stale rejected response', finishReason: 'stop', usage: {} },
      chunks: [{ runId: 'rejected-retry-run', type: 'finish', payload: {} }],
    });
    expect(() =>
      runtime.registerRun(
        { id: 'stale-retry-agent' } as any,
        staleOutput,
        {
          runId: 'rejected-retry-run',
          memory: { resource: 'rejected-retry-user', thread: 'rejected-retry-thread' },
        } as any,
        pubsub,
      ),
    ).toThrow('was rejected');
  });

  it('starts queued idle wakes left behind when a reservation is retargeted', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new EventEmitterPubSub();
    runtime.reserveRun(
      {
        runId: 'retarget-with-idle-run',
        memory: { resource: 'retarget-idle-old-user', thread: 'retarget-idle-old-thread' },
      } as any,
      pubsub,
      'retarget-owner-agent',
    );
    const stream = vi.fn(async () => ({
      runId: 'retarget-queued-idle-run',
      status: 'running',
      fullStream: (async function* () {})(),
      _waitUntilFinished: async () => {},
    }));

    const result = runtime.sendSignal(
      { id: 'retarget-queued-idle-agent', stream } as any,
      { type: 'user-message', contents: 'wake after retarget' },
      {
        resourceId: 'retarget-idle-old-user',
        threadId: 'retarget-idle-old-thread',
        ifIdle: {
          streamOptions: { memory: { resource: 'retarget-idle-old-user', thread: 'retarget-idle-old-thread' } },
        } as any,
      },
      pubsub,
    );
    expect(stream).not.toHaveBeenCalled();

    expect(
      runtime.retargetReservedRun(
        'retarget-with-idle-run',
        { resourceId: 'retarget-idle-old-user', threadId: 'retarget-idle-old-thread' },
        { resourceId: 'retarget-idle-new-user', threadId: 'retarget-idle-new-thread' },
        pubsub,
        'retarget-owner-agent',
      ),
    ).toBe(true);
    await waitForCondition(() => stream.mock.calls.length > 0);
    expect(stream).toHaveBeenCalledWith(
      expect.objectContaining({ contents: 'wake after retarget' }),
      expect.objectContaining({
        runId: result.runId,
        memory: { resource: 'retarget-idle-old-user', thread: 'retarget-idle-old-thread' },
      }),
    );
  });

  it('wakes waiters parked on the old thread when a reservation is retargeted', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new EventEmitterPubSub();
    runtime.reserveRun(
      {
        runId: 'retarget-wakes-old-thread-run',
        memory: { resource: 'retarget-wakes-old-user', thread: 'retarget-wakes-old-thread' },
      } as any,
      pubsub,
      'retarget-wakes-owner',
    );

    let waiterResolved = false;
    const waiter = runtime
      .waitForCrossAgentThreadRun(
        { id: 'retarget-wakes-waiter' } as any,
        {
          runId: 'retarget-wakes-waiter-run',
          memory: { resource: 'retarget-wakes-old-user', thread: 'retarget-wakes-old-thread' },
        } as any,
        pubsub,
      )
      .then(() => {
        waiterResolved = true;
      });
    await nextTick();
    expect(waiterResolved).toBe(false);

    expect(
      runtime.retargetReservedRun(
        'retarget-wakes-old-thread-run',
        { resourceId: 'retarget-wakes-old-user', threadId: 'retarget-wakes-old-thread' },
        { resourceId: 'retarget-wakes-new-user', threadId: 'retarget-wakes-new-thread' },
        pubsub,
        'retarget-wakes-owner',
      ),
    ).toBe(true);

    await waiter;
    expect(waiterResolved).toBe(true);
  });

  it('starts a queued idle wake when a reserved setup run is aborted', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new EventEmitterPubSub();
    runtime.reserveRun(
      {
        runId: 'reserved-abort-with-idle-run',
        memory: { resource: 'reserved-abort-idle-user', thread: 'reserved-abort-idle-thread' },
      } as any,
      pubsub,
      'reserved-agent',
    );
    const stream = vi.fn(async () => ({
      runId: 'queued-idle-after-abort-run',
      status: 'running',
      fullStream: (async function* () {})(),
      _waitUntilFinished: async () => {},
    }));

    const result = runtime.sendSignal(
      { id: 'queued-idle-after-abort-agent', stream } as any,
      { type: 'user-message', contents: 'wake after abort' },
      {
        resourceId: 'reserved-abort-idle-user',
        threadId: 'reserved-abort-idle-thread',
        ifIdle: {
          streamOptions: { memory: { resource: 'reserved-abort-idle-user', thread: 'reserved-abort-idle-thread' } },
        } as any,
      },
      pubsub,
    );
    expect(result).toEqual(expect.objectContaining({ runId: expect.any(String) }));
    expect(stream).not.toHaveBeenCalled();

    expect(runtime.abortRun('reserved-abort-with-idle-run', pubsub)).toBe(true);
    await nextTick();
    expect(stream).toHaveBeenCalledWith(
      expect.objectContaining({ contents: 'wake after abort' }),
      expect.objectContaining({
        runId: result.runId,
        memory: { resource: 'reserved-abort-idle-user', thread: 'reserved-abort-idle-thread' },
      }),
    );
  });

  it('releases waiters when draining a queued active signal fails', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new EventEmitterPubSub();
    let finishActive!: () => void;
    const activeFinished = new Promise<void>(resolve => {
      finishActive = resolve;
    });
    const stream = vi.fn(async () => {
      throw new Error('queued active setup failed');
    });
    const owner = { id: 'queued-active-failure-agent', stream };
    const completion = runtime.registerRun(
      owner as any,
      {
        runId: 'queued-active-failure-run',
        status: 'running',
        fullStream: (async function* () {})(),
        _waitUntilFinished: () => activeFinished,
      } as any,
      {
        runId: 'queued-active-failure-run',
        memory: { resource: 'queued-active-failure-user', thread: 'queued-active-failure-thread' },
      } as any,
      pubsub,
    );
    const signalResult = runtime.sendSignal(
      owner as any,
      { type: 'user-message', contents: 'queued active failure' },
      { resourceId: 'queued-active-failure-user', threadId: 'queued-active-failure-thread' },
      pubsub,
    );
    await expect(signalResult.accepted).resolves.toMatchObject({
      action: 'deliver',
      runId: 'queued-active-failure-run',
    });

    let waiterResolved = false;
    const waiter = runtime
      .waitForCrossAgentThreadRun(
        { id: 'queued-active-failure-waiter' } as any,
        {
          runId: 'queued-active-failure-next-run',
          memory: { resource: 'queued-active-failure-user', thread: 'queued-active-failure-thread' },
        } as any,
        pubsub,
      )
      .then(() => {
        waiterResolved = true;
      });
    await nextTick();
    expect(waiterResolved).toBe(false);

    finishActive();
    await expect(completion).rejects.toThrow('queued active setup failed');
    await waiter;
    expect(waiterResolved).toBe(true);
    expect(
      runtime.reserveRun(
        {
          runId: 'queued-active-failure-next-run',
          memory: { resource: 'queued-active-failure-user', thread: 'queued-active-failure-thread' },
        } as any,
        pubsub,
      ),
    ).toEqual(expect.any(Function));
  });

  it('drops queued signals when a prepared run is aborted', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new EventEmitterPubSub();
    let finishActive!: () => void;
    const activeFinished = new Promise<void>(resolve => {
      finishActive = resolve;
    });

    const completion = runtime.registerRun(
      { id: 'active-agent' } as any,
      {
        runId: 'prepared-abort-run',
        status: 'running',
        fullStream: (async function* () {})(),
        _waitUntilFinished: () => activeFinished,
      } as any,
      {
        runId: 'prepared-abort-run',
        memory: { resource: 'prepared-abort-user', thread: 'prepared-abort-thread' },
      } as any,
      pubsub,
    );

    const signalResult = runtime.sendSignal(
      { id: 'active-agent' } as any,
      { type: 'user-message', contents: 'stale prepared signal' },
      {
        runId: 'prepared-abort-run',
      },
      pubsub,
    );
    expect(signalResult).toEqual(expect.objectContaining({ runId: 'prepared-abort-run' }));

    expect(runtime.abortRun('prepared-abort-run', pubsub)).toBe(true);
    expect(() =>
      runtime.sendSignal(
        { id: 'active-agent' } as any,
        { type: 'user-message', contents: 'post-abort stale signal' },
        {
          runId: 'prepared-abort-run',
        },
        pubsub,
      ),
    ).toThrow('has been aborted');
    finishActive();
    await completion;

    runtime.reserveRun(
      {
        runId: 'prepared-abort-successor-run',
        memory: { resource: 'prepared-abort-user', thread: 'prepared-abort-thread' },
      } as any,
      pubsub,
    );
    expect(runtime.drainPendingSignals('prepared-abort-successor-run', pubsub)).toEqual([]);
  });

  it('rejects duplicate registered run ids on the same thread', () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new EventEmitterPubSub();
    const streamOptions = {
      runId: 'duplicate-registered-run',
      memory: { resource: 'duplicate-registered-user', thread: 'duplicate-registered-thread' },
    } as any;

    runtime.registerRun(
      { id: 'active-agent' } as any,
      {
        runId: 'duplicate-registered-run',
        status: 'running',
        fullStream: (async function* () {})(),
        _waitUntilFinished: () => new Promise<void>(() => {}),
      } as any,
      streamOptions,
      pubsub,
    );

    expect(() =>
      runtime.registerRun(
        { id: 'active-agent' } as any,
        {
          runId: 'duplicate-registered-run',
          status: 'running',
          fullStream: (async function* () {})(),
          _waitUntilFinished: () => new Promise<void>(() => {}),
        } as any,
        streamOptions,
        pubsub,
      ),
    ).toThrow('already registered');
  });

  it('rejects same-agent registration when another run is active on the thread', () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new EventEmitterPubSub();
    const streamOptions = {
      memory: { resource: 'same-agent-active-user', thread: 'same-agent-active-thread' },
    } as any;

    runtime.registerRun(
      { id: 'same-active-agent' } as any,
      {
        runId: 'same-active-first-run',
        status: 'running',
        fullStream: (async function* () {})(),
        _waitUntilFinished: () => new Promise<void>(() => {}),
      } as any,
      { ...streamOptions, runId: 'same-active-first-run' },
      pubsub,
    );

    expect(() =>
      runtime.registerRun(
        { id: 'same-active-agent' } as any,
        {
          runId: 'same-active-second-run',
          status: 'running',
          fullStream: (async function* () {})(),
          _waitUntilFinished: () => new Promise<void>(() => {}),
        } as any,
        { ...streamOptions, runId: 'same-active-second-run' },
        pubsub,
      ),
    ).toThrow('already active for this thread');
  });

  it('does not treat a same-agent active run as a cross-agent blocker', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new EventEmitterPubSub();
    let finishActive!: () => void;
    const activeFinished = new Promise<void>(resolve => {
      finishActive = resolve;
    });
    const completion = runtime.registerRun(
      { id: 'same-wait-agent' } as any,
      {
        runId: 'same-wait-active-run',
        status: 'running',
        fullStream: (async function* () {})(),
        _waitUntilFinished: () => activeFinished,
      } as any,
      {
        runId: 'same-wait-active-run',
        memory: { resource: 'same-wait-user', thread: 'same-wait-thread' },
      } as any,
      pubsub,
    );

    let waiterResolved = false;
    const waiter = runtime
      .waitForCrossAgentThreadRun(
        { id: 'same-wait-agent' } as any,
        {
          runId: 'same-wait-next-run',
          memory: { resource: 'same-wait-user', thread: 'same-wait-thread' },
        } as any,
        pubsub,
      )
      .then(() => {
        waiterResolved = true;
      });
    await nextTick();
    expect(waiterResolved).toBe(true);

    finishActive();
    await completion;
    await waiter;
    expect(waiterResolved).toBe(true);
  });

  it('does not block on a completed active record awaiting cleanup', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new EventEmitterPubSub();
    let finishActive!: () => void;
    const activeFinished = new Promise<void>(resolve => {
      finishActive = resolve;
    });
    const completion = runtime.registerRun(
      { id: 'completed-window-agent' } as any,
      {
        runId: 'completed-window-active-run',
        status: 'success',
        fullStream: (async function* () {})(),
        _waitUntilFinished: () => activeFinished,
      } as any,
      {
        runId: 'completed-window-active-run',
        memory: { resource: 'completed-window-user', thread: 'completed-window-thread' },
      } as any,
      pubsub,
    );

    let waiterResolved = false;
    const waiter = runtime
      .waitForCrossAgentThreadRun(
        { id: 'completed-window-next-agent' } as any,
        {
          runId: 'completed-window-next-run',
          memory: { resource: 'completed-window-user', thread: 'completed-window-thread' },
        } as any,
        pubsub,
      )
      .then(() => {
        waiterResolved = true;
      });
    await nextTick();
    expect(waiterResolved).toBe(true);

    finishActive();
    await completion;
    await waiter;
    expect(waiterResolved).toBe(true);
  });

  it('reserves the thread after waiting so concurrent stream callers do not overlap execution', async () => {
    let finishActive!: () => void;
    const activeFinished = new Promise<void>(resolve => {
      finishActive = resolve;
    });
    let finishFirstWaiter!: () => void;
    const firstWaiterFinished = new Promise<void>(resolve => {
      finishFirstWaiter = resolve;
    });
    let streamCalls = 0;
    const runner = new Agent({
      id: 'post-wait-reservation-agent',
      name: 'Post Wait Reservation Agent',
      instructions: 'Test',
      model: new MockLanguageModelV2({
        doStream: async () => {
          streamCalls += 1;
          const call = streamCalls;
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: new ReadableStream({
              async start(controller) {
                controller.enqueue({ type: 'stream-start', warnings: [] });
                controller.enqueue({
                  type: 'response-metadata',
                  id: `id-${call}`,
                  modelId: 'mock-model-id',
                  timestamp: new Date(0),
                });
                controller.enqueue({ type: 'text-start', id: `text-${call}` });
                controller.enqueue({ type: 'text-delta', id: `text-${call}`, delta: `response ${call}` });
                if (call === 1) await firstWaiterFinished;
                controller.enqueue({ type: 'text-end', id: `text-${call}` });
                controller.enqueue({
                  type: 'finish',
                  finishReason: 'stop',
                  usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
                });
                controller.close();
              },
            }),
          };
        },
      }),
    });
    const activeCompletion = agentThreadStreamRuntime.registerRun(
      runner as any,
      {
        runId: 'post-wait-active-run',
        status: 'running',
        fullStream: (async function* () {})(),
        _waitUntilFinished: () => activeFinished,
      } as any,
      {
        runId: 'post-wait-active-run',
        memory: { resource: 'post-wait-user', thread: 'post-wait-thread' },
      } as any,
    );

    const first = runner.stream('first', {
      memory: { resource: 'post-wait-user', thread: 'post-wait-thread' },
    });
    const second = runner.stream('second', {
      memory: { resource: 'post-wait-user', thread: 'post-wait-thread' },
    });
    await nextTick();
    expect(streamCalls).toBe(0);

    finishActive();
    await activeCompletion;
    await waitForCondition(() => streamCalls === 1);
    await nextTick();
    expect(streamCalls).toBe(1);

    const firstOutput = await first;
    const firstText = firstOutput.text;
    finishFirstWaiter();
    await expect(firstText).resolves.toBe('response 1');
    await waitForCondition(() => streamCalls === 2);

    const secondOutput = await second;
    await expect(secondOutput.text).resolves.toBe('response 2');
  });

  it('drains accepted queued signals before releasing waiters after async completion publish', async () => {
    const runtime = agentThreadStreamRuntime;
    const pubsub = new BlockingRunCompletedPubSub();
    let finishActive!: () => void;
    const activeFinished = new Promise<void>(resolve => {
      finishActive = resolve;
    });
    let finishQueued!: () => void;
    const queuedFinished = new Promise<void>(resolve => {
      finishQueued = resolve;
    });
    const ownerCalls: string[] = [];
    const competitorCalls: string[] = [];
    const callOrder: string[] = [];
    const owner = new Agent({
      id: 'completion-drain-agent',
      name: 'Completion Drain Owner',
      instructions: 'Test',
      model: new MockLanguageModelV2({
        doStream: async ({ prompt }) => {
          callOrder.push('queued');
          ownerCalls.push(JSON.stringify(prompt));
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: new ReadableStream({
              async start(controller) {
                controller.enqueue({ type: 'stream-start', warnings: [] });
                controller.enqueue({
                  type: 'response-metadata',
                  id: 'queued-id',
                  modelId: 'mock-model-id',
                  timestamp: new Date(0),
                });
                controller.enqueue({ type: 'text-start', id: 'queued-text' });
                controller.enqueue({ type: 'text-delta', id: 'queued-text', delta: 'queued response' });
                await queuedFinished;
                controller.enqueue({ type: 'text-end', id: 'queued-text' });
                controller.enqueue({
                  type: 'finish',
                  finishReason: 'stop',
                  usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
                });
                controller.close();
              },
            }),
          };
        },
      }),
    });
    const competitor = new Agent({
      id: 'completion-drain-agent',
      name: 'Completion Drain Competitor',
      instructions: 'Test',
      model: new MockLanguageModelV2({
        doStream: async ({ prompt }) => {
          callOrder.push('competitor');
          competitorCalls.push(JSON.stringify(prompt));
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: convertArrayToReadableStream([
              { type: 'stream-start', warnings: [] },
              { type: 'response-metadata', id: 'competitor-id', modelId: 'mock-model-id', timestamp: new Date(0) },
              { type: 'text-start', id: 'competitor-text' },
              { type: 'text-delta', id: 'competitor-text', delta: 'competitor response' },
              { type: 'text-end', id: 'competitor-text' },
              {
                type: 'finish',
                finishReason: 'stop',
                usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
              },
            ]),
          };
        },
      }),
    });

    const completion = runtime.registerRun(
      owner as any,
      {
        runId: 'completion-drain-active-run',
        status: 'running',
        fullStream: (async function* () {})(),
        _waitUntilFinished: () => activeFinished,
      } as any,
      {
        runId: 'completion-drain-active-run',
        memory: { resource: 'completion-drain-user', thread: 'completion-drain-thread' },
      } as any,
      pubsub,
    );
    const queuedSignal = runtime.sendSignal(
      owner as any,
      { type: 'user-message', contents: 'queued signal' },
      { resourceId: 'completion-drain-user', threadId: 'completion-drain-thread' },
      pubsub,
    );
    await expect(queuedSignal.accepted).resolves.toMatchObject({
      action: 'deliver',
      runId: 'completion-drain-active-run',
    });

    finishActive();
    await waitForCondition(() => pubsub.sawRunCompleted);
    const competitorStream = competitor.stream('competing stream', {
      memory: { resource: 'completion-drain-user', thread: 'completion-drain-thread' },
      _pubsub: pubsub,
    } as any);
    await nextTick();
    expect(ownerCalls).toHaveLength(0);
    expect(competitorCalls).toHaveLength(0);

    pubsub.unblockRunCompleted();
    await waitForCondition(() => ownerCalls.length === 1);
    expect(JSON.stringify(ownerCalls)).toContain('queued signal');
    expect(callOrder[0]).toBe('queued');

    finishQueued();
    await completion;
    await expect(competitorStream.then(stream => stream.text)).resolves.toBe('competitor response');
    expect(competitorCalls).toHaveLength(1);
    expect(callOrder).toEqual(['queued', 'competitor']);
  });

  it('moves reservation waiters onto the registered run on setup success', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new EventEmitterPubSub();
    runtime.reserveRun(
      {
        runId: 'reserved-success-run',
        memory: { resource: 'reserved-success-user', thread: 'reserved-success-thread' },
      } as any,
      pubsub,
      'owner-agent',
    );

    let waiterResolved = false;
    const waiter = runtime
      .waitForCrossAgentThreadRun(
        { id: 'other-agent' } as any,
        {
          runId: 'other-run',
          memory: { resource: 'reserved-success-user', thread: 'reserved-success-thread' },
        } as any,
        pubsub,
      )
      .then(() => {
        waiterResolved = true;
      });
    await nextTick();
    expect(waiterResolved).toBe(false);

    let finishActive!: () => void;
    const activeFinished = new Promise<void>(resolve => {
      finishActive = resolve;
    });
    const completion = runtime.registerRun(
      { id: 'owner-agent' } as any,
      {
        runId: 'reserved-success-run',
        status: 'running',
        fullStream: (async function* () {})(),
        _waitUntilFinished: () => activeFinished,
      } as any,
      {
        runId: 'reserved-success-run',
        memory: { resource: 'reserved-success-user', thread: 'reserved-success-thread' },
      } as any,
      pubsub,
    );
    await nextTick();
    expect(waiterResolved).toBe(false);

    finishActive();
    await completion;
    await waiter;
    expect(waiterResolved).toBe(true);
  });

  it('does not treat matching run ids from another agent as its own reservation', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new EventEmitterPubSub();
    let finishActive!: () => void;
    const activeFinished = new Promise<void>(resolve => {
      finishActive = resolve;
    });

    const completion = runtime.registerRun(
      { id: 'owner-agent' } as any,
      {
        runId: 'shared-run-id',
        status: 'running',
        fullStream: (async function* () {})(),
        _waitUntilFinished: () => activeFinished,
      } as any,
      {
        runId: 'shared-run-id',
        memory: { resource: 'run-id-collision-user', thread: 'run-id-collision-thread' },
      } as any,
      pubsub,
    );

    let waiterResolved = false;
    const waiter = runtime
      .waitForCrossAgentThreadRun(
        { id: 'different-agent' } as any,
        {
          runId: 'shared-run-id',
          memory: { resource: 'run-id-collision-user', thread: 'run-id-collision-thread' },
        } as any,
        pubsub,
      )
      .then(() => {
        waiterResolved = true;
      });
    await nextTick();
    expect(waiterResolved).toBe(false);

    finishActive();
    await completion;
    await waiter;
    expect(waiterResolved).toBe(true);
  });

  it('does not treat matching run ids from the same agent as its own reservation without ownership', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new EventEmitterPubSub();
    const release = runtime.reserveRun(
      {
        runId: 'same-agent-shared-run-id',
        memory: { resource: 'same-agent-collision-user', thread: 'same-agent-collision-thread' },
      } as any,
      pubsub,
      'owner-agent',
    );

    let waiterResolved = false;
    const waiter = runtime
      .waitForCrossAgentThreadRun(
        { id: 'owner-agent' } as any,
        {
          runId: 'same-agent-shared-run-id',
          memory: { resource: 'same-agent-collision-user', thread: 'same-agent-collision-thread' },
        } as any,
        pubsub,
      )
      .then(() => {
        waiterResolved = true;
      });
    await nextTick();
    expect(waiterResolved).toBe(false);

    release?.();
    await waiter;
    expect(waiterResolved).toBe(true);
  });

  it('rejects registration when another agent owns the reservation', () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new EventEmitterPubSub();
    runtime.reserveRun(
      {
        runId: 'reserved-owner-register-run',
        memory: { resource: 'reserved-owner-register-user', thread: 'reserved-owner-register-thread' },
      } as any,
      pubsub,
      'owner-agent',
    );

    expect(() =>
      runtime.registerRun(
        { id: 'different-agent' } as any,
        {
          runId: 'reserved-owner-register-run',
          status: 'running',
          fullStream: (async function* () {})(),
          _waitUntilFinished: () => new Promise<void>(() => {}),
        } as any,
        {
          runId: 'reserved-owner-register-run',
          memory: { resource: 'reserved-owner-register-user', thread: 'reserved-owner-register-thread' },
        } as any,
        pubsub,
      ),
    ).toThrow('reserved by another agent');
  });

  it('cleans up a thread subscription and completes the iterator', async () => {
    const agent = new Agent({
      id: 'cleanup-signal-agent',
      name: 'Cleanup Signal Agent',
      instructions: 'Test',
      model: createTextStreamModel('cleanup response'),
    });

    const subscription = await agent.subscribeToThread({
      threadId: 'cleanup-thread',
      resourceId: 'cleanup-user',
    });
    const iterator = subscription.stream[Symbol.asyncIterator]();

    subscription.unsubscribe();
    await expect(iterator.next()).resolves.toEqual({ value: undefined, done: true });
  });

  it('allows a thread follower to abort the active run controller', () => {
    const runtime = new AgentThreadStreamRuntime();
    const options = runtime.prepareRunOptions({
      runId: 'abort-run',
      memory: { thread: 'abort-thread', resource: 'abort-user' },
    } as any);
    const neverFinishes = new Promise<any>(() => {});

    runtime.registerRun(
      { id: 'abortable-agent' } as any,
      {
        runId: 'abort-run',
        status: 'running',
        _waitUntilFinished: () => neverFinishes,
      } as any,
      options,
    );

    expect(runtime.abortThread({ threadId: 'abort-thread', resourceId: 'abort-user' })).toBe(true);
    expect(options.abortSignal?.aborted).toBe(true);
  });

  it('does not consume active run output while watching for completion', () => {
    const runtime = new AgentThreadStreamRuntime();
    const getFullOutput = vi.fn();

    runtime.registerRun(
      { id: 'watch-agent' } as any,
      {
        runId: 'watch-run',
        status: 'running',
        getFullOutput,
        _waitUntilFinished: () => new Promise<any>(() => {}),
      } as any,
      {
        runId: 'watch-run',
        memory: { thread: 'watch-thread', resource: 'watch-user' },
      } as any,
    );

    expect(getFullOutput).not.toHaveBeenCalled();
  });

  it('delivers a future thread run to multiple subscribers', async () => {
    const agent = new Agent({
      id: 'multiple-subscriber-agent',
      name: 'Multiple Subscriber Agent',
      instructions: 'Test',
      model: createTextStreamModel('multi response'),
    });

    const firstSubscription = await agent.subscribeToThread({
      threadId: 'multi-thread',
      resourceId: 'multi-user',
    });
    const secondSubscription = await agent.subscribeToThread({
      threadId: 'multi-thread',
      resourceId: 'multi-user',
    });
    const firstRunPromise = readNextRun(firstSubscription.stream[Symbol.asyncIterator]());
    const secondRunPromise = readNextRun(secondSubscription.stream[Symbol.asyncIterator]());

    const stream = await agent.stream('Hello', {
      memory: { thread: 'multi-thread', resource: 'multi-user' },
    });

    await expect(firstRunPromise).resolves.toMatchObject({ value: { runId: stream.runId }, done: false });
    await expect(secondRunPromise).resolves.toMatchObject({ value: { runId: stream.runId }, done: false });

    firstSubscription.unsubscribe();
    secondSubscription.unsubscribe();
  });

  it('isolates subscriptions by resource and thread id', async () => {
    const agent = new Agent({
      id: 'isolated-signal-agent',
      name: 'Isolated Signal Agent',
      instructions: 'Test',
      model: createTextStreamModel('isolated response'),
    });

    const targetSubscription = await agent.subscribeToThread({
      threadId: 'isolated-thread',
      resourceId: 'isolated-user',
    });
    const otherResourceSubscription = await agent.subscribeToThread({
      threadId: 'isolated-thread',
      resourceId: 'other-user',
    });
    const otherThreadSubscription = await agent.subscribeToThread({
      threadId: 'other-thread',
      resourceId: 'isolated-user',
    });

    const targetNext = readNextRun(targetSubscription.stream[Symbol.asyncIterator]());
    const otherResourceNext = readNextRun(otherResourceSubscription.stream[Symbol.asyncIterator]());
    const otherThreadNext = readNextRun(otherThreadSubscription.stream[Symbol.asyncIterator]());

    const stream = await agent.stream('Hello', {
      memory: { thread: 'isolated-thread', resource: 'isolated-user' },
    });

    await expect(targetNext).resolves.toMatchObject({ value: { runId: stream.runId }, done: false });
    await nextTick();

    otherResourceSubscription.unsubscribe();
    otherThreadSubscription.unsubscribe();
    await expect(otherResourceNext).resolves.toEqual({ value: undefined, done: true });
    await expect(otherThreadNext).resolves.toEqual({ value: undefined, done: true });

    targetSubscription.unsubscribe();
  });

  it('does not replay completed thread runs to late subscribers', async () => {
    const agent = new Agent({
      id: 'late-subscription-agent',
      name: 'Late Subscription Agent',
      instructions: 'Test',
      model: createTextStreamModel('late response'),
    });

    const stream = await agent.stream('Hello', {
      memory: { thread: 'late-thread', resource: 'late-user' },
    });
    await stream.text;
    const subscription = await agent.subscribeToThread({
      threadId: 'late-thread',
      resourceId: 'late-user',
    });
    const iterator = subscription.stream[Symbol.asyncIterator]();

    const nextRun = readNextRun(iterator);
    await nextTick();
    subscription.unsubscribe();
    await expect(nextRun).resolves.toEqual({ value: undefined, done: true });
  });

  it('drains a signal by active run id into the active run', async () => {
    let releaseFirst!: () => void;
    const firstFinished = new Promise<void>(resolve => {
      releaseFirst = resolve;
    });
    let streamCount = 0;
    const prompts: any[][] = [];

    const model = new MockLanguageModelV2({
      doStream: async ({ prompt }) => {
        streamCount += 1;
        prompts.push(prompt);
        const responseText = streamCount === 1 ? 'run id first response' : 'run id signal response';

        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: new ReadableStream({
            async start(controller) {
              controller.enqueue({ type: 'stream-start', warnings: [] });
              controller.enqueue({
                type: 'response-metadata',
                id: `run-id-${streamCount}`,
                modelId: 'mock-model-id',
                timestamp: new Date(0),
              });
              controller.enqueue({ type: 'text-start', id: 'text-1' });
              controller.enqueue({ type: 'text-delta', id: 'text-1', delta: responseText });
              controller.enqueue({ type: 'text-end', id: 'text-1' });
              if (streamCount === 1) {
                await firstFinished;
              }
              controller.enqueue({
                type: 'finish',
                finishReason: 'stop',
                usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
              });
              controller.close();
            },
          }),
        };
      },
    });

    const agent = new Agent({
      id: 'run-id-signal-agent',
      name: 'Run Id Signal Agent',
      instructions: 'Test',
      model,
    });
    const subscription = await agent.subscribeToThread({
      threadId: 'run-id-thread',
      resourceId: 'run-id-user',
    });
    const iterator = subscription.stream[Symbol.asyncIterator]();
    const firstRunPromise = readNextRun(iterator);

    const stream = await agent.stream('Hello', {
      memory: { thread: 'run-id-thread', resource: 'run-id-user' },
    });
    await expect(waitForActiveRun(subscription)).resolves.toBe(stream.runId);

    const runIdSignalResult = agent.sendSignal(
      { type: 'user-message', contents: 'Hello by run id' },
      { runId: stream.runId },
    );
    await expect(runIdSignalResult.accepted).resolves.toMatchObject({ action: 'deliver', runId: stream.runId });

    releaseFirst();
    await firstRunPromise;
    await expect(stream.text).resolves.toBe('run id first responserun id signal response');
    expect(streamCount).toBe(2);
    expect(JSON.stringify(prompts[1])).toContain('Hello by run id');

    subscription.unsubscribe();
  });

  it('throws when sending a signal to an unknown run id without a thread target', () => {
    const agent = new Agent({
      id: 'missing-run-signal-agent',
      name: 'Missing Run Signal Agent',
      instructions: 'Test',
      model: createTextStreamModel('missing run response'),
    });

    expect(() => agent.sendSignal({ type: 'user-message', contents: 'Hello' }, { runId: 'missing-run-id' })).toThrow(
      'No active agent run found for signal target',
    );
  });

  it('starts an idle thread run with a system-reminder signal as user-role XML context', async () => {
    let capturedPrompt: any[] | undefined;
    const model = new MockLanguageModelV2({
      doStream: async ({ prompt }) => {
        capturedPrompt = prompt;
        return {
          rawCall: { rawPrompt: prompt, rawSettings: {} },
          warnings: [],
          stream: convertArrayToReadableStream([
            { type: 'stream-start', warnings: [] },
            { type: 'response-metadata', id: 'system-signal-id', modelId: 'mock-model-id', timestamp: new Date(0) },
            { type: 'text-start', id: 'text-1' },
            { type: 'text-delta', id: 'text-1', delta: 'system signal response' },
            { type: 'text-end', id: 'text-1' },
            {
              type: 'finish',
              finishReason: 'stop',
              usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
            },
          ]),
        };
      },
    });

    const agent = new Agent({
      id: 'system-signal-agent',
      name: 'System Signal Agent',
      instructions: 'Test',
      model,
    });

    const stream = await agent.sendSignal(
      { type: 'system-reminder', contents: 'continue', attributes: { reminderType: 'test-reminder' } },
      {
        resourceId: 'system-signal-user',
        threadId: 'system-signal-thread',
        ifIdle: { streamOptions: { memory: { resource: 'system-signal-user', thread: 'system-signal-thread' } } },
      },
    );

    await expect(stream.accepted).resolves.toMatchObject({ action: 'wake' });
    for (let i = 0; i < 10 && !capturedPrompt; i++) {
      await nextTick();
    }
    expect(
      capturedPrompt?.some(
        message =>
          message.role === 'user' &&
          Array.isArray(message.content) &&
          message.content.some(
            (part: any) => part.text === '<system-reminder reminderType="test-reminder">continue</system-reminder>',
          ),
      ),
    ).toBe(true);
  });

  describe('delivery option attributes', () => {
    it('resolveDeliveryAttributes merges option attributes into signal attributes', () => {
      const signal = createSignal({
        type: 'user-message',
        contents: 'hello',
        attributes: { existing: 'yes' },
      });

      const resolved = resolveDeliveryAttributes(signal, { delivery: 'while-active' });
      expect(resolved.attributes).toEqual({ existing: 'yes', delivery: 'while-active' });
    });

    it('resolveDeliveryAttributes returns same signal when no option attributes are selected', () => {
      const signal = createSignal({
        type: 'user-message',
        contents: 'hello',
      });

      const resolved = resolveDeliveryAttributes(signal, undefined);
      expect(resolved).toBe(signal);
    });

    it('resolved delivery attributes appear in toLLMMessage XML', () => {
      const signal = createSignal({
        type: 'user-message',
        contents: 'fix the bug',
      });

      const resolved = resolveDeliveryAttributes(signal, { delivery: 'while-active' });
      expect(resolved.toLLMMessage()).toEqual({
        role: 'user',
        content: '<user delivery="while-active">fix the bug</user>',
      });
    });

    it('resolved delivery attributes appear in toDBMessage and toDataPart', () => {
      const signal = createSignal({
        type: 'user-message',
        contents: 'fix the bug',
      });

      const resolved = resolveDeliveryAttributes(signal, { delivery: 'while-active' });
      const db = resolved.toDBMessage({ threadId: 't', resourceId: 'r' });
      expect((db.content.metadata!.signal as Record<string, unknown>).attributes).toEqual({
        delivery: 'while-active',
      });

      const dataPart = resolved.toDataPart();
      expect(dataPart.data.attributes).toEqual({ delivery: 'while-active' });
    });

    it('thread-stream-runtime resolves ifActive.attributes as while-active on active signal delivery', () => {
      const runtime = new AgentThreadStreamRuntime();
      const pubsub = new EventEmitterPubSub();
      const agent = { id: 'delivery-active-agent' } as any;

      // Prepare and register a run that is still "running" so the thread is active.
      const options = runtime.prepareRunOptions(
        {
          runId: 'active-run',
          memory: { thread: 'delivery-thread', resource: 'delivery-resource' },
        } as any,
        pubsub,
      );
      runtime.registerRun(
        agent,
        {
          runId: 'active-run',
          status: 'running',
          _waitUntilFinished: () => new Promise<any>(() => {}),
        } as any,
        options,
        pubsub,
      );

      // Send a signal while the run is still active.
      const result = runtime.sendSignal(
        agent,
        {
          type: 'user-message',
          contents: 'while-active test',
        },
        {
          resourceId: 'delivery-resource',
          threadId: 'delivery-thread',
          ifActive: { attributes: { delivery: 'while-active' } },
          ifIdle: {
            attributes: { delivery: 'message' },
            streamOptions: {
              memory: { thread: 'delivery-thread', resource: 'delivery-resource' },
            },
          },
        },
        pubsub,
      );

      // Active run → ifActive.attributes → delivery: 'while-active'
      expect(result.signal.attributes).toEqual({ delivery: 'while-active' });
    });

    it('thread-stream-runtime resolves ifIdle.attributes as message on idle signal delivery', () => {
      const runtime = new AgentThreadStreamRuntime();
      const pubsub = new EventEmitterPubSub();
      const agent = { id: 'delivery-idle-agent', stream: () => new Promise(() => {}) } as any;

      // No run registered → thread is idle.
      const result = runtime.sendSignal(
        agent,
        {
          type: 'user-message',
          contents: 'idle test',
        },
        {
          resourceId: 'idle-resource',
          threadId: 'idle-thread',
          ifActive: { attributes: { delivery: 'while-active' } },
          ifIdle: {
            attributes: { delivery: 'message' },
            streamOptions: {
              memory: { thread: 'idle-thread', resource: 'idle-resource' },
            },
          },
        },
        pubsub,
      );

      // No active run → ifIdle.attributes → delivery: 'message'
      expect(result.signal.attributes).toEqual({ delivery: 'message' });
    });
  });
});
