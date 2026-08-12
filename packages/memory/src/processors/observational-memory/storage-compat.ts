import type { StorageThreadType } from '@mastra/core/memory';
import type { MemoryStorage, ObservationalMemoryWriteGuard, StorageResourceType } from '@mastra/core/storage';

type GuardedResourceUpdate = {
  resourceId: string;
  workingMemory: string;
  guard: ObservationalMemoryWriteGuard;
};

type GuardedThreadUpdate = {
  id: string;
  title?: string;
  metadata: Record<string, unknown>;
  guard: ObservationalMemoryWriteGuard;
};

/**
 * Use generation-fenced writes when the installed Core exposes them, while
 * retaining @mastra/memory's declared compatibility with older Core peers.
 */
export async function updateResourceFromObservationalMemory(
  storage: MemoryStorage,
  args: GuardedResourceUpdate,
): Promise<StorageResourceType> {
  const guardedUpdate = storage.updateResourceFromObservationalMemory;
  if (typeof guardedUpdate === 'function') {
    return guardedUpdate.call(storage, args);
  }
  return storage.updateResource({
    resourceId: args.resourceId,
    workingMemory: args.workingMemory,
  });
}

/**
 * See updateResourceFromObservationalMemory. Older Core peers cannot provide
 * the atomic generation fence, but must continue to run their established
 * update path rather than failing at method dispatch.
 */
export async function updateThreadFromObservationalMemory(
  storage: MemoryStorage,
  args: GuardedThreadUpdate,
): Promise<StorageThreadType> {
  const guardedUpdate = storage.updateThreadFromObservationalMemory;
  if (typeof guardedUpdate === 'function') {
    return guardedUpdate.call(storage, args);
  }
  return storage.updateThread({
    id: args.id,
    title: args.title,
    metadata: args.metadata,
  });
}
