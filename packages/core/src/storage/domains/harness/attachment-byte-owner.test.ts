import { createHash } from 'node:crypto';

import { describe, expect, it } from 'vitest';

import {
  HarnessAttachmentByteOwnerConflictError,
  HarnessAttachmentByteOwnerIntegrityError,
  HarnessAttachmentByteOwnerInvalidInputError,
  HarnessAttachmentByteOwnerLimitError,
  InMemoryHarnessAttachmentByteOwner,
} from './attachment-byte-owner';
import type {
  HarnessAttachmentByteOwner,
  HarnessAttachmentByteOwnerLoadInput,
  HarnessAttachmentByteOwnerSaveInput,
  InMemoryHarnessAttachmentObject,
} from './attachment-byte-owner';

const baseOwner = {
  harnessName: 'doxa-chat',
  sessionId: 'session-1',
  attachmentId: 'attachment-1',
  incarnation: 'incarnation-1',
} as const;

const payload = new TextEncoder().encode('native attachment bytes');
const payloadSha256 = createHash('sha256').update(payload).digest('hex');

function makeSaveInput(
  overrides: Partial<HarnessAttachmentByteOwnerSaveInput> = {},
): HarnessAttachmentByteOwnerSaveInput {
  return {
    owner: { ...baseOwner },
    operationId: 'upload-1',
    storageNamespace: 'memory://memory',
    data: payload,
    expectedBytes: payload.byteLength,
    expectedSha256: payloadSha256,
    mimeType: 'text/plain',
    ...overrides,
  };
}

function makeLoadInput(
  input: Pick<HarnessAttachmentByteOwnerLoadInput, 'blobRef'> &
    Partial<Omit<HarnessAttachmentByteOwnerLoadInput, 'blobRef'>>,
): HarnessAttachmentByteOwnerLoadInput {
  return {
    owner: { ...baseOwner },
    expectedBytes: payload.byteLength,
    expectedSha256: payloadSha256,
    maxBytes: payload.byteLength,
    ...input,
  };
}

describe('InMemoryHarnessAttachmentByteOwner', () => {
  it('stores, replays, and returns an isolated copy for one exact incarnation', async () => {
    const owner = new InMemoryHarnessAttachmentByteOwner();
    const input = makeSaveInput();

    const stored = await owner.save(input);
    expect(stored.outcome).toBe('stored');
    if (stored.outcome === 'unknown') {
      throw new Error('in-memory owner must resolve a save outcome');
    }

    const replay = await owner.save(input);
    expect(replay).toEqual({
      outcome: 'already_stored',
      blobRef: stored.blobRef,
    });

    const loaded = await owner.load(makeLoadInput({ blobRef: stored.blobRef }));
    expect(loaded).not.toBeNull();
    expect(new TextDecoder().decode(loaded!)).toBe('native attachment bytes');
    loaded!.fill(0);

    const loadedAgain = await owner.load(makeLoadInput({ blobRef: stored.blobRef }));
    expect(new TextDecoder().decode(loadedAgain!)).toBe('native attachment bytes');
  });

  it('rejects invalid or conflicting identity and enforces bounded verified reads', async () => {
    const owner = new InMemoryHarnessAttachmentByteOwner();
    const input = makeSaveInput();
    const stored = await owner.save(input);
    if (stored.outcome === 'unknown') {
      throw new Error('in-memory owner must resolve a save outcome');
    }

    await expect(owner.save({ ...input, expectedBytes: input.expectedBytes + 1 })).rejects.toBeInstanceOf(
      HarnessAttachmentByteOwnerInvalidInputError,
    );
    await expect(owner.save({ ...input, expectedSha256: '0'.repeat(64) })).rejects.toBeInstanceOf(
      HarnessAttachmentByteOwnerIntegrityError,
    );
    await expect(owner.save({ ...input, operationId: 'different-operation' })).rejects.toBeInstanceOf(
      HarnessAttachmentByteOwnerConflictError,
    );
    await expect(
      owner.load(
        makeLoadInput({
          blobRef: stored.blobRef,
          maxBytes: input.expectedBytes - 1,
        }),
      ),
    ).rejects.toBeInstanceOf(HarnessAttachmentByteOwnerLimitError);
    await expect(
      owner.load(
        makeLoadInput({
          blobRef: stored.blobRef,
          expectedSha256: '0'.repeat(64),
        }),
      ),
    ).rejects.toBeInstanceOf(HarnessAttachmentByteOwnerIntegrityError);
    await expect(owner.load(makeLoadInput({ blobRef: 'memory://other/object' }))).rejects.toBeInstanceOf(
      HarnessAttachmentByteOwnerConflictError,
    );

    await expect(
      owner.load(
        makeLoadInput({
          blobRef: 'memory://memory/harness-attachments/missing',
        }),
      ),
    ).rejects.toBeInstanceOf(HarnessAttachmentByteOwnerConflictError);
  });

  it('keeps a recreated incarnation alive when a stale incarnation is deleted', async () => {
    const owner = new InMemoryHarnessAttachmentByteOwner();
    const oldOwner = { ...baseOwner, incarnation: 'incarnation-old' };
    const newOwner = { ...baseOwner, incarnation: 'incarnation-new' };
    const oldInput = makeSaveInput({ owner: oldOwner, operationId: 'old-upload' });
    const newInput = makeSaveInput({ owner: newOwner, operationId: 'new-upload' });
    const oldStored = await owner.save(oldInput);
    const newStored = await owner.save(newInput);
    if (oldStored.outcome === 'unknown' || newStored.outcome === 'unknown') {
      throw new Error('in-memory owner must resolve a save outcome');
    }
    expect(oldStored.blobRef).not.toBe(newStored.blobRef);

    await expect(
      owner.delete({
        blobRef: oldStored.blobRef,
        owner: oldOwner,
        expectedBytes: oldInput.expectedBytes,
        expectedSha256: oldInput.expectedSha256,
      }),
    ).resolves.toEqual({ outcome: 'deleted' });

    const loadedNew = await owner.load(makeLoadInput({ blobRef: newStored.blobRef, owner: newOwner }));
    expect(new TextDecoder().decode(loadedNew!)).toBe('native attachment bytes');
    await expect(owner.load(makeLoadInput({ blobRef: oldStored.blobRef, owner: oldOwner }))).resolves.toBeNull();
    await expect(owner.save(oldInput)).rejects.toBeInstanceOf(HarnessAttachmentByteOwnerConflictError);
  });

  it('surfaces missing and corrupt objects without making ambiguity a cleanup winner', async () => {
    const objects = new Map<string, InMemoryHarnessAttachmentObject>();
    const owner = new InMemoryHarnessAttachmentByteOwner({ objects });
    const input = makeSaveInput();
    await expect(
      owner.load(
        makeLoadInput({
          blobRef: 'memory://memory/harness-attachments/doxa-chat/session-1/attachment-1/incarnation-1',
        }),
      ),
    ).resolves.toBeNull();

    const stored = await owner.save(input);
    if (stored.outcome === 'unknown') {
      throw new Error('in-memory owner must resolve a save outcome');
    }
    const storedObject = objects.get(stored.blobRef);
    expect(storedObject).toBeDefined();
    storedObject!.data[0] = storedObject!.data[0]! ^ 0xff;
    await expect(owner.load(makeLoadInput({ blobRef: stored.blobRef }))).rejects.toBeInstanceOf(
      HarnessAttachmentByteOwnerIntegrityError,
    );
    await expect(
      owner.delete({
        blobRef: stored.blobRef,
        owner: input.owner,
        expectedBytes: input.expectedBytes,
        expectedSha256: input.expectedSha256,
      }),
    ).rejects.toBeInstanceOf(HarnessAttachmentByteOwnerIntegrityError);

    const cleanOwner = new InMemoryHarnessAttachmentByteOwner();
    const ambiguousOwner: HarnessAttachmentByteOwner = {
      storageNamespace: cleanOwner.storageNamespace,
      save: async saveInput => {
        const result = await cleanOwner.save(saveInput);
        if (result.outcome === 'unknown') {
          return result;
        }
        return { outcome: 'unknown', blobRef: result.blobRef };
      },
      load: inputToLoad => cleanOwner.load(inputToLoad),
      delete: inputToDelete => cleanOwner.delete(inputToDelete),
      cancel: inputToCancel => cleanOwner.cancel(inputToCancel),
    };
    const ambiguousResult = await ambiguousOwner.save(input);
    expect(ambiguousResult.outcome).toBe('unknown');
    if (ambiguousResult.outcome !== 'unknown' || ambiguousResult.blobRef === undefined) {
      throw new Error('ambiguous save should retain a bounded blob reference');
    }
    await expect(cleanOwner.load(makeLoadInput({ blobRef: ambiguousResult.blobRef }))).resolves.not.toBeNull();
    await expect(
      cleanOwner.cancel({
        owner: input.owner,
        operationId: input.operationId,
        expectedBytes: input.expectedBytes,
        expectedSha256: input.expectedSha256,
        storageNamespace: input.storageNamespace,
      }),
    ).resolves.toMatchObject({ outcome: 'deleted' });
    await expect(cleanOwner.save(input)).rejects.toThrow();
  });

  it('refuses a cancellation bound to another namespace or reference without fencing its own', async () => {
    const original = new InMemoryHarnessAttachmentByteOwner({ providerId: 'bucket-a' });
    const replacement = new InMemoryHarnessAttachmentByteOwner({ providerId: 'bucket-b' });
    expect(original.storageNamespace).not.toBe(replacement.storageNamespace);
    const input = makeSaveInput({ storageNamespace: original.storageNamespace });
    const stored = await original.save(input);
    if (stored.outcome === 'unknown') throw new Error('in-memory owner must resolve a save outcome');
    await expect(replacement.save(input)).rejects.toBeInstanceOf(HarnessAttachmentByteOwnerConflictError);
    const cancelInput = {
      owner: input.owner,
      operationId: input.operationId,
      expectedBytes: input.expectedBytes,
      expectedSha256: input.expectedSha256,
      storageNamespace: original.storageNamespace,
    };

    // The replacement namespace never reports cleanup for the original one.
    await expect(replacement.cancel(cancelInput)).rejects.toBeInstanceOf(HarnessAttachmentByteOwnerConflictError);
    await expect(
      replacement.cancel({ ...cancelInput, storageNamespace: replacement.storageNamespace, blobRef: stored.blobRef }),
    ).rejects.toBeInstanceOf(HarnessAttachmentByteOwnerConflictError);
    await expect(original.cancel({ ...cancelInput, blobRef: stored.blobRef + '-other' })).rejects.toBeInstanceOf(
      HarnessAttachmentByteOwnerConflictError,
    );
    await expect(
      original.load(makeLoadInput({ blobRef: stored.blobRef, maxBytes: payload.byteLength })),
    ).resolves.not.toBeNull();

    await expect(original.cancel({ ...cancelInput, blobRef: stored.blobRef })).resolves.toMatchObject({
      outcome: 'deleted',
    });
    await expect(original.save(input)).rejects.toBeInstanceOf(HarnessAttachmentByteOwnerConflictError);
  });

  it('rejects a reference from another namespace on load and delete instead of reading it as absent', async () => {
    const original = new InMemoryHarnessAttachmentByteOwner({ storageNamespace: 'r2://endpoint-a/bucket' });
    // Same provider id, different namespace: references must still differ.
    const replacement = new InMemoryHarnessAttachmentByteOwner({ storageNamespace: 'r2://endpoint-b/bucket' });
    const input = makeSaveInput({ storageNamespace: original.storageNamespace });
    const stored = await original.save(input);
    if (stored.outcome === 'unknown') throw new Error('in-memory owner must resolve a save outcome');
    expect(stored.blobRef.startsWith(original.storageNamespace + '/')).toBe(true);

    await expect(replacement.load(makeLoadInput({ blobRef: stored.blobRef }))).rejects.toBeInstanceOf(
      HarnessAttachmentByteOwnerConflictError,
    );
    await expect(
      replacement.delete({
        blobRef: stored.blobRef,
        owner: input.owner,
        expectedBytes: input.expectedBytes,
        expectedSha256: input.expectedSha256,
      }),
    ).rejects.toBeInstanceOf(HarnessAttachmentByteOwnerConflictError);
    await expect(original.load(makeLoadInput({ blobRef: stored.blobRef }))).resolves.not.toBeNull();
  });

  it('rejects an unusable declared storage namespace', () => {
    expect(() => new InMemoryHarnessAttachmentByteOwner({ storageNamespace: '' })).toThrow(
      HarnessAttachmentByteOwnerInvalidInputError,
    );
    expect(() => new InMemoryHarnessAttachmentByteOwner({ storageNamespace: 'bucket\u0000a' })).toThrow(
      HarnessAttachmentByteOwnerInvalidInputError,
    );
  });

  it('rejects a providerId whose encoded form exceeds the scope-component bound', () => {
    // Each CJK codepoint URI-encodes to 9 characters ('%E6%BC%A2'), so 512 raw
    // characters satisfy the raw-length check while producing a blob-reference
    // segment past the bound load and delete enforce — an object saved under it
    // would be unaddressable.
    expect(() => new InMemoryHarnessAttachmentByteOwner({ providerId: '漢'.repeat(512) })).toThrow(
      HarnessAttachmentByteOwnerInvalidInputError,
    );
    expect(() => new InMemoryHarnessAttachmentByteOwner({ providerId: 'native-attachment-test' })).not.toThrow();
  });
});
