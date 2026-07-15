import { createHash } from 'node:crypto';

import { describe, expect, it } from 'vitest';

import { MessageList } from '../../agent/message-list';
import { isCreatedAgentSignal } from '../../agent/signals';
import { extractSignalContents, setupHarness } from './__test-utils__';
import {
  HarnessAttachmentInUseError,
  HarnessAttachmentUnavailableError,
  HarnessQueueFullError,
  HarnessValidationError,
} from './errors';

describe('Harness.attachments', () => {
  it('uploads attachments with digest metadata and deletes them by attachment id', async () => {
    const { harness, storage } = setupHarness();
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });

    const source = Buffer.from('hello attachment');
    const expectedSha256 = createHash('sha256').update(source).digest('hex');
    const result = await harness.attachments.upload({
      sessionId: session.id,
      data: source,
      filename: 'note.txt',
      contentType: 'text/plain',
    });

    expect(result.resourceId).toBe('u');
    expect(result.ownerSessionId).toBe(session.id);
    expect(result.bytes).toBe(source.length);
    expect(result.sha256).toBe(expectedSha256);
    expect(result.source).toBe('preupload');

    const record = await storage.getAttachmentRecord({
      sessionId: session.id,
      attachmentId: result.attachmentId,
    });
    expect(record).not.toBeNull();
    expect(record).toMatchObject({
      ownerSessionId: session.id,
      name: 'note.txt',
      mimeType: 'text/plain',
      bytes: source.length,
      sha256: expectedSha256,
      source: 'preupload',
    });

    source[0] = 'j'.charCodeAt(0);

    const loaded = await storage.loadAttachment({
      sessionId: session.id,
      attachmentId: result.attachmentId,
    });
    expect(loaded).not.toBeNull();
    expect(loaded).toMatchObject({
      name: 'note.txt',
      mimeType: 'text/plain',
      bytes: source.length,
      sha256: expectedSha256,
    });
    expect(Buffer.from(loaded!.data).toString()).toBe('hello attachment');

    await harness.attachments.delete({ attachmentId: result.attachmentId, sessionId: session.id });

    await expect(
      storage.getAttachmentRecord({
        sessionId: session.id,
        attachmentId: result.attachmentId,
      }),
    ).resolves.toBeNull();
    await expect(
      storage.loadAttachment({
        sessionId: session.id,
        attachmentId: result.attachmentId,
      }),
    ).resolves.toBeNull();
  });

  it('emits session-scoped attachment_uploaded and attachment_deleted (§10.2)', async () => {
    const { harness } = setupHarness();
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    const events: Array<{ type: string; attachmentId?: string; name?: string; mimeType?: string; bytes?: number }> = [];
    session.subscribe(e => events.push(e as never));

    const result = await harness.attachments.upload({
      sessionId: session.id,
      data: Buffer.from('hello attachment'),
      filename: 'note.txt',
      contentType: 'text/plain',
    });

    expect(events.find(e => e.type === 'attachment_uploaded')).toMatchObject({
      attachmentId: result.attachmentId,
      name: 'note.txt',
      mimeType: 'text/plain',
      bytes: result.bytes,
    });

    await harness.attachments.delete({ attachmentId: result.attachmentId, sessionId: session.id });
    expect(events.find(e => e.type === 'attachment_deleted')).toMatchObject({
      attachmentId: result.attachmentId,
    });
  });

  it('reuses matching internal URL attachment ids without overwriting existing bytes', async () => {
    const { harness, storage } = setupHarness();
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    const uploadWithInternalUrlId = harness.attachments.upload as typeof harness.attachments.upload &
      ((
        opts: Parameters<typeof harness.attachments.upload>[0] & { attachmentId: string; source: 'url' },
      ) => ReturnType<typeof harness.attachments.upload>);

    const first = await uploadWithInternalUrlId({
      sessionId: session.id,
      attachmentId: 'attachment-url-stable',
      source: 'url',
      data: new Uint8Array([1, 2, 3]),
      filename: 'remote.bin',
      contentType: 'application/octet-stream',
    });
    const retry = await uploadWithInternalUrlId({
      sessionId: session.id,
      attachmentId: 'attachment-url-stable',
      source: 'url',
      data: new Uint8Array([1, 2, 3]),
      filename: 'remote.bin',
      contentType: 'application/octet-stream',
    });

    expect(retry).toEqual(first);
    await expect(
      uploadWithInternalUrlId({
        sessionId: session.id,
        attachmentId: 'attachment-url-stable',
        source: 'url',
        data: new Uint8Array([9]),
        filename: 'remote.bin',
        contentType: 'application/octet-stream',
      }),
    ).rejects.toMatchObject({ reason: 'digest_mismatch', attachmentId: 'attachment-url-stable' });

    const loaded = await storage.loadAttachment({ sessionId: session.id, attachmentId: 'attachment-url-stable' });
    expect(Array.from(loaded!.data)).toEqual([1, 2, 3]);
  });

  it('keeps file config immutable from caller-owned MIME arrays', async () => {
    const allowedUrlMimeTypes = ['text/plain'];
    const { harness } = setupHarness({ files: { allowedUrlMimeTypes } });

    allowedUrlMimeTypes.push('image/png');
    const exposed = harness.getFileConfig().allowedUrlMimeTypes as string[];

    expect(() => exposed.push('application/json')).toThrow(TypeError);
    expect(harness.getFileConfig().allowedUrlMimeTypes).toEqual(['text/plain']);
  });

  it('uploads primitive attachments as canonical bytes with semantic metadata', async () => {
    const { harness, storage } = setupHarness();
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });

    const result = await harness.attachments.upload({
      sessionId: session.id,
      kind: 'primitive',
      name: 'selection.json',
      primitiveType: 'selection',
      value: { z: 1, a: ['paper-1', 'paper-2'] },
      metadata: { label: 'Selected papers' },
    });

    const expectedBytes = Buffer.from('{"a":["paper-1","paper-2"],"z":1}');
    expect(result).toMatchObject({
      resourceId: 'u',
      ownerSessionId: session.id,
      kind: 'primitive',
      name: 'selection.json',
      mimeType: 'application/json',
      primitiveType: 'selection',
      metadata: { label: 'Selected papers' },
      bytes: expectedBytes.length,
      sha256: createHash('sha256').update(expectedBytes).digest('hex'),
    });

    await expect(
      storage.getAttachmentRecord({ sessionId: session.id, attachmentId: result.attachmentId }),
    ).resolves.toMatchObject({
      kind: 'primitive',
      primitiveType: 'selection',
      metadata: { label: 'Selected papers' },
    });

    const loaded = await storage.loadAttachment({ sessionId: session.id, attachmentId: result.attachmentId });
    expect(Buffer.from(loaded!.data).toString()).toBe('{"a":["paper-1","paper-2"],"z":1}');
    expect(loaded?.semantic).toMatchObject({ kind: 'primitive', primitiveType: 'selection' });
  });

  it('rejects invalid primitive and element attachment JSON payloads', async () => {
    const { harness } = setupHarness();
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    const cyclicPrimitive: Record<string, unknown> = { title: 'loop' };
    cyclicPrimitive.self = cyclicPrimitive;
    const cyclicElement: unknown[] = [];
    cyclicElement.push(cyclicElement);
    const sparsePrimitive: unknown[] = [];
    sparsePrimitive[1] = 'missing zero';

    await expect(
      harness.attachments.upload({
        sessionId: session.id,
        kind: 'primitive',
        name: 'loop.json',
        primitiveType: 'json',
        value: cyclicPrimitive,
      }),
    ).rejects.toMatchObject({ field: 'attachments.upload().value.self' });
    await expect(
      harness.attachments.upload({
        sessionId: session.id,
        kind: 'element',
        name: 'loop-element.json',
        elementType: 'loop',
        payload: cyclicElement,
      }),
    ).rejects.toMatchObject({ field: 'attachments.upload().payload[0]' });
    await expect(
      harness.attachments.upload({
        sessionId: session.id,
        kind: 'primitive',
        name: 'sparse.json',
        primitiveType: 'json',
        value: sparsePrimitive as never,
      }),
    ).rejects.toMatchObject({ field: 'attachments.upload().value[0]' });
    await expect(
      harness.attachments.upload({
        sessionId: session.id,
        data: new Uint8Array([1]),
        filename: 'metadata.bin',
        contentType: 'application/octet-stream',
        metadata: ['not-a-record'] as never,
      }),
    ).rejects.toMatchObject({ field: 'attachments.upload().metadata' });
  });

  it('uses the explicit owning session instead of the most-recent resource session', async () => {
    const { harness, storage } = setupHarness();
    const older = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    const newer = await harness.session({ resourceId: 'u', threadId: { fresh: true } });

    const result = await harness.attachments.upload({
      sessionId: older.id,
      resourceId: 'u',
      data: new Uint8Array([1, 2, 3]),
      filename: 'older.bin',
      contentType: 'application/octet-stream',
    });

    await expect(
      storage.getAttachmentRecord({
        sessionId: older.id,
        attachmentId: result.attachmentId,
      }),
    ).resolves.toMatchObject({ ownerSessionId: older.id });
    await expect(
      storage.getAttachmentRecord({
        sessionId: newer.id,
        attachmentId: result.attachmentId,
      }),
    ).resolves.toBeNull();

    await harness.attachments.delete({ sessionId: older.id, attachmentId: result.attachmentId });

    await expect(
      storage.getAttachmentRecord({
        sessionId: older.id,
        attachmentId: result.attachmentId,
      }),
    ).resolves.toBeNull();
  });

  it('guards uploaded attachments once a queued item references them', async () => {
    const { harness, storage, agent } = setupHarness();
    agent.enqueueRun({ finishReason: 'stop', text: 'queued reply' });
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    const result = await harness.attachments.upload({
      sessionId: session.id,
      data: new Uint8Array([1, 2, 3]),
      filename: 'note.bin',
      contentType: 'application/octet-stream',
    });

    await session.queue({ content: 'use this', attachments: [result] });

    await expect(
      storage.listAttachmentReferences({ sessionId: session.id, attachmentId: result.attachmentId }),
    ).resolves.toEqual([{ source: 'queued_item', sourceId: expect.any(String) }]);
    await expect(
      harness.attachments.delete({ sessionId: session.id, attachmentId: result.attachmentId }),
    ).rejects.toBeInstanceOf(HarnessAttachmentInUseError);
  });

  it('dispatches a queued turn with attachments as a native user signal carrying the file part', async () => {
    // §13.7/§14.2: a direct `queue({ content, attachments })` turn forwards the
    // attachment bytes to the model as file/image parts (not just the dedup
    // identity). The non-channel analogue of the channel signal-delivery dispatch
    // shape — characterizes the queue-drain dispatch shape.
    const { harness, agent } = setupHarness();
    agent.enqueueRun({ finishReason: 'stop', text: 'queued reply' });
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    const result = await harness.attachments.upload({
      sessionId: session.id,
      data: new TextEncoder().encode('attachment-bytes'),
      filename: 'note.txt',
      contentType: 'text/plain',
    });

    await session.queue({ content: 'see attached', attachments: [result] });

    expect(agent.streamCalls).toHaveLength(1);
    const dispatched = agent.streamCalls[0]?.messages;
    expect(isCreatedAgentSignal(dispatched)).toBe(true);
    if (!isCreatedAgentSignal(dispatched)) throw new Error('expected queued attachment dispatch to be a signal');
    const promptMessage = dispatched.toLLMMessage();
    expect(promptMessage.role).toBe('user');
    expect(Array.isArray(promptMessage.content)).toBe(true);
    const parts = promptMessage.content as Array<{ type: string; mediaType?: string; filename?: string }>;
    expect(parts.some(part => part.type === 'text')).toBe(true);
    expect(parts.some(part => part.type === 'file' && part.filename === 'note.txt')).toBe(true);
  });

  it('dispatches an image/png attachment whose bytes survive a real MessageList conversion', async () => {
    // §13.7/§14.2 + regression guard: an inline image attachment must reach the
    // model with its decoded bytes intact. The earlier image special-case built
    // `{ type: 'image', data: base64, mimeType }`, but the AI SDK v5 image part
    // keys on `image`/`mediaType` — so `convertImageFilePart` read `part.image`
    // (undefined) and produced `data:image/png;base64,undefined`, silently
    // dropping the bytes. The text-only dispatch tests above never caught it
    // because the mock agent captures raw `streamCalls` without routing through
    // MessageList. This test feeds the actual dispatched contents into a real
    // MessageList and asserts the prompt carries the original bytes.
    const { harness, agent } = setupHarness();
    agent.enqueueRun({ finishReason: 'stop', text: 'queued reply' });
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    const pngBytes = new TextEncoder().encode('PNGDATA');
    const result = await harness.attachments.upload({
      sessionId: session.id,
      data: pngBytes,
      filename: 'pic.png',
      contentType: 'image/png',
    });

    await session.queue({ content: 'look at this image', attachments: [result] });

    expect(agent.streamCalls).toHaveLength(1);
    const dispatched = agent.streamCalls[0]?.messages;
    expect(isCreatedAgentSignal(dispatched)).toBe(true);
    if (!isCreatedAgentSignal(dispatched)) throw new Error('expected queued image dispatch to be a signal');

    // Route the persisted signal through the real MessageList -> LLM prompt
    // conversion used when a signal row is reconstructed for the model.
    const list = new MessageList();
    list.add(dispatched.toDBMessage(), 'input');
    const prompt = await list.get.all.aiV5.llmPrompt();

    const filePart = prompt
      .flatMap(message => (Array.isArray(message.content) ? message.content : []))
      .find(part => part.type === 'file');
    expect(filePart).toBeDefined();
    expect(filePart?.mediaType).toBe('image/png');
    // The decoded bytes must be the original PNG bytes, NOT `base64,undefined`.
    const decoded =
      filePart?.data instanceof Uint8Array
        ? Buffer.from(filePart.data).toString()
        : Buffer.from(String(filePart?.data), 'base64').toString();
    expect(decoded).toBe('PNGDATA');
  });

  it('dispatches a queued turn with NO attachments as a bare string (unchanged)', async () => {
    const { harness, agent } = setupHarness();
    agent.enqueueRun({ finishReason: 'stop', text: 'queued reply' });
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });

    await session.queue({ content: 'plain text turn' });

    expect(agent.streamCalls).toHaveLength(1);
    expect(extractSignalContents(agent.streamCalls[0]?.messages)).toBe('plain text turn');
  });

  it('freezes element attachment descriptors into queued refs and guarded delete', async () => {
    const { harness, storage, agent } = setupHarness();
    let releaseManual!: () => void;
    const manualGate = new Promise<void>(resolve => {
      releaseManual = resolve;
    });
    agent.enqueueRun({ finishReason: 'stop', text: 'manual', holdUntil: manualGate });
    agent.enqueueRun({ finishReason: 'stop', text: 'queued reply' });
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    const manual = session.message({ content: 'hold drain' });
    await new Promise(resolve => setImmediate(resolve));

    const element = await harness.attachments.upload({
      sessionId: session.id,
      kind: 'element',
      name: 'chart-card.json',
      elementType: 'chart-card',
      payload: { title: 'Trend', values: [1, 3, 2] },
      renderer: { id: 'chart-card', version: '1' },
      schemaId: 'chart-card.v1',
      metadata: { purpose: 'preview' },
    });
    const queued = session.queue({ content: 'render this', attachments: [element] });
    await new Promise(resolve => setImmediate(resolve));

    expect(session.getRecord().pendingQueue[0]?.attachments[0]).toMatchObject({
      kind: 'ref',
      attachmentKind: 'element',
      elementType: 'chart-card',
      renderer: { id: 'chart-card', version: '1' },
      schemaId: 'chart-card.v1',
      metadata: { purpose: 'preview' },
    });
    await expect(
      storage.listAttachmentReferences({ sessionId: session.id, attachmentId: element.attachmentId }),
    ).resolves.toEqual([{ source: 'queued_item', sourceId: expect.any(String) }]);
    await expect(
      harness.attachments.delete({ sessionId: session.id, attachmentId: element.attachmentId }),
    ).rejects.toBeInstanceOf(HarnessAttachmentInUseError);

    releaseManual();
    await expect(manual).resolves.toMatchObject({ text: 'manual' });
    await expect(queued).resolves.toMatchObject({ text: 'queued reply' });
  });

  it('rejects queue admission for cross-session or mismatched attachment refs', async () => {
    const { harness } = setupHarness();
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    const other = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    const result = await harness.attachments.upload({
      sessionId: session.id,
      data: new Uint8Array([1, 2, 3]),
      filename: 'note.bin',
      contentType: 'application/octet-stream',
    });

    await expect(other.queue({ content: 'bad', attachments: [result] })).rejects.toBeInstanceOf(HarnessValidationError);
    await expect(
      session.queue({ content: 'bad', attachments: [{ ...result, sha256: '0'.repeat(64) }] }),
    ).rejects.toBeInstanceOf(HarnessValidationError);
    await expect(
      session.queue({ content: 'bad', attachments: [{ attachmentId: 'missing', resourceId: 'u' }] }),
    ).rejects.toBeInstanceOf(HarnessAttachmentUnavailableError);
  });

  it('rejects a queued turn when an admitted attachment digest changes before drain', async () => {
    const { harness, storage, agent } = setupHarness();
    let releaseManual!: () => void;
    const manualGate = new Promise<void>(resolve => {
      releaseManual = resolve;
    });
    agent.enqueueRun({ finishReason: 'stop', text: 'manual', holdUntil: manualGate });
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    const result = await harness.attachments.upload({
      sessionId: session.id,
      data: new Uint8Array([1, 2, 3]),
      filename: 'note.bin',
      contentType: 'application/octet-stream',
    });

    const manual = session.message({ content: 'hold drain' });
    await new Promise(resolve => setImmediate(resolve));
    const queued = session.queue({ content: 'use this', attachments: [result] });
    await new Promise(resolve => setImmediate(resolve));

    const record = await storage.getAttachmentRecord({ sessionId: session.id, attachmentId: result.attachmentId });
    expect(record).not.toBeNull();
    record!.sha256 = '0'.repeat(64);

    releaseManual();

    await expect(queued).rejects.toMatchObject({ reason: 'digest_mismatch', attachmentId: result.attachmentId });
    await expect(manual).resolves.toMatchObject({ text: 'manual' });
    expect(agent.streamCalls).toHaveLength(1);
  });

  it('does not record attachment references for queue items rejected by capacity', async () => {
    const { harness, storage, agent } = setupHarness({ sessions: { maxQueueDepth: 1 } });
    agent.enqueueRun({
      finishReason: 'suspended',
      runId: 'r1',
      suspendPayload: { toolCallId: 'tc-1', toolName: 'shell', args: { cmd: 'x' } },
    });
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    const first = await harness.attachments.upload({
      sessionId: session.id,
      data: new Uint8Array([1]),
      filename: 'first.bin',
      contentType: 'application/octet-stream',
    });
    const second = await harness.attachments.upload({
      sessionId: session.id,
      data: new Uint8Array([2]),
      filename: 'second.bin',
      contentType: 'application/octet-stream',
    });

    const queued = session.queue({ content: 'first', attachments: [first] });
    queued.catch(() => {});
    await new Promise(resolve => setImmediate(resolve));

    await expect(session.queue({ content: 'second', attachments: [second] })).rejects.toBeInstanceOf(
      HarnessQueueFullError,
    );
    await expect(
      storage.listAttachmentReferences({ sessionId: session.id, attachmentId: second.attachmentId }),
    ).resolves.toEqual([]);
  });
});

describe('Session flat attachment accessors (§4.2e)', () => {
  it('uploadAttachment maps the raw-bytes form onto a file upload, reports progress, returns the id', async () => {
    const { harness, storage } = setupHarness();
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });

    const data = Buffer.from('flat upload bytes');
    let progress: [number, number] | undefined;
    const { attachmentId } = await session.uploadAttachment({
      name: 'flat.txt',
      mimeType: 'text/plain',
      data,
      onProgress: (loaded, total) => {
        progress = [loaded, total];
      },
    });

    expect(attachmentId).toBeTruthy();
    expect(progress).toEqual([data.byteLength, data.byteLength]);

    const record = await storage.getAttachmentRecord({ sessionId: session.id, attachmentId });
    expect(record).toMatchObject({
      ownerSessionId: session.id,
      name: 'flat.txt',
      mimeType: 'text/plain',
      bytes: data.length,
    });
  });

  it('deleteAttachment removes a session-scoped attachment by id', async () => {
    const { harness, storage } = setupHarness();
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });

    const { attachmentId } = await session.uploadAttachment({
      name: 'gone.txt',
      mimeType: 'text/plain',
      data: Buffer.from('temporary'),
    });
    await session.deleteAttachment({ attachmentId });

    await expect(storage.getAttachmentRecord({ sessionId: session.id, attachmentId })).resolves.toBeNull();
  });
});
