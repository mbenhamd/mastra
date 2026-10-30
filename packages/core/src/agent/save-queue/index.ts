import type { IMastraLogger } from '../../logger';
import type { MemoryConfigInternal } from '../../memory';
import type { MastraMemory } from '../../memory/memory';
import type { ObservationalMemorySourceWriteGuard } from '../../storage';
import { isRunFenceConflictError } from '../../storage/run-fencing';
import { filterMessagesForPersistence } from '../filter-messages-for-persistence';
import type { MastraDBMessage, MessageList } from '../message-list';
import { noteThreadMessagesSaved } from '../thread-saves';

type UnsavedMessageSnapshot = ReturnType<MessageList['snapshotUnsavedMessages']>;

export interface FlushMessagesOptions {
  /**
   * Runs inside the thread's save queue, immediately before unsaved messages
   * are drained. A rejection skips the save, leaves the messages unsaved, and
   * rejects the flush with that error. Unless throwOnError is enabled, storage
   * errors are logged, except a run fence conflict, which rejects like the guard.
   */
  beforePersist?: () => Promise<void>;
  /** Reject this flush on storage failure without rejecting the shared queue. */
  throwOnError?: boolean;
}

export class SaveQueueManager {
  private logger?: IMastraLogger;
  private debounceMs: number;
  private memory?: MastraMemory;
  private onSaved?: (messages: MastraDBMessage[]) => void;

  private static MAX_STALENESS_MS = 1000;

  constructor({
    logger,
    debounceMs,
    memory,
    onSaved,
  }: {
    logger?: IMastraLogger;
    debounceMs?: number;
    memory?: MastraMemory;
    /** Called with the rows memory stored after each successful save. */
    onSaved?: (messages: MastraDBMessage[]) => void;
  }) {
    this.logger = logger;
    this.debounceMs = debounceMs || 100;
    this.memory = memory;
    this.onSaved = onSaved;
  }
  private saveQueues = new Map<string, Promise<void>>();
  private saveDebounceTimers = new Map<string, NodeJS.Timeout>();

  /**
   * Debounces save operations for a thread, ensuring that consecutive save requests
   * are batched and only the latest is executed after a short delay.
   * @param threadId - The ID of the thread to debounce saves for.
   * @param messageList - The MessageList instance containing unsaved messages.
   * @param memoryConfig - Optional memory configuration to use for saving.
   * @returns A promise that resolves when the debounced save completes.
   */
  private debounceSave(threadId: string, messageList: MessageList, memoryConfig?: MemoryConfigInternal): Promise<void> {
    return new Promise((resolve, reject) => {
      if (this.saveDebounceTimers.has(threadId)) {
        clearTimeout(this.saveDebounceTimers.get(threadId)!);
      }
      this.saveDebounceTimers.set(
        threadId,
        setTimeout(() => {
          this.enqueueSave(threadId, messageList, memoryConfig)
            .then(resolve)
            .catch(err => {
              this.logger?.error?.('Error in debounceSave', { err, threadId });
              reject(err);
            })
            .finally(() => {
              this.saveDebounceTimers.delete(threadId);
            });
        }, this.debounceMs),
      );
    });
  }

  /**
   * Enqueues a save operation for a thread, ensuring that saves are executed in order and
   * only one save runs at a time per thread. If a save is already in progress for the thread,
   * the new save is queued to run after the previous completes.
   *
   * @param threadId - The ID of the thread whose messages should be saved.
   * @param messageList - The MessageList instance containing unsaved messages.
   * @param memoryConfig - Optional memory configuration to use for saving.
   * @param options.beforePersist - Optional guard; see {@link FlushMessagesOptions}.
   * @param options.strictSnapshot - Fork: a pre-captured snapshot persisted fail-closed (errors propagate).
   */
  private enqueueSave(
    threadId: string,
    messageList: MessageList,
    memoryConfig?: MemoryConfigInternal,
    options?: {
      strictSnapshot?: UnsavedMessageSnapshot;
      observationalMemorySourceWriteGuard?: ObservationalMemorySourceWriteGuard;
      beforePersist?: FlushMessagesOptions['beforePersist'];
      throwOnError?: FlushMessagesOptions['throwOnError'];
    },
  ) {
    const prev = this.saveQueues.get(threadId) || Promise.resolve();
    const beforePersist = options?.beforePersist;
    const throwOnError = options?.throwOnError ?? false;
    let saveFailed = false;
    let saveError: unknown;
    const operation = prev.then(async () => {
      if (beforePersist) {
        try {
          await beforePersist();
        } catch (err) {
          saveFailed = true;
          saveError = err;
          return;
        }
      }
      if (options?.strictSnapshot) {
        await this.persistUnsavedMessagesStrict(
          options.strictSnapshot,
          memoryConfig,
          options.observationalMemorySourceWriteGuard,
        );
      } else {
        await this.persistUnsavedMessages(threadId, messageList, memoryConfig);
      }
    });
    const next = operation
      .catch(err => {
        // Strict callers need storage failures too; the queue itself must still resolve.
        if (throwOnError || (beforePersist && isRunFenceConflictError(err))) {
          saveFailed = true;
          saveError = err;
          return;
        }
        this.logger?.error?.('Error in enqueueSave', { err, threadId });
      })
      .then(() => {
        if (this.saveQueues.get(threadId) === next) {
          this.saveQueues.delete(threadId);
        }
      });
    this.saveQueues.set(threadId, next);
    // Fork: strict saves are fail-closed; storage errors propagate to the caller.
    if (options?.strictSnapshot) {
      return operation.then(() => {
        if (saveFailed) throw saveError;
      });
    }
    // The queue chain itself never rejects, so later saves aren't poisoned;
    // only this caller sees the guard or strict save rejection.
    if (!beforePersist && !throwOnError) return next;
    return next.then(() => {
      if (saveFailed) throw saveError;
    });
  }

  /**
   * Clears any pending debounced save for a thread, preventing the scheduled save
   * from executing if it hasn't already fired.
   *
   * @param threadId - The ID of the thread whose debounced save should be cleared.
   */
  clearDebounce(threadId: string) {
    if (this.saveDebounceTimers.has(threadId)) {
      clearTimeout(this.saveDebounceTimers.get(threadId)!);
      this.saveDebounceTimers.delete(threadId);
    }
  }

  /** Bookkeeping only: a failure here must not turn a successful save into an error. */
  private notifySaved(saved: { messages?: MastraDBMessage[] } | undefined) {
    if (!saved?.messages?.length) return;
    try {
      this.onSaved?.(saved.messages);
    } catch (error) {
      this.logger?.warn?.('onSaved callback failed', { error });
    }
  }

  /**
   * Persists any unsaved messages from the MessageList to memory storage.
   * Acknowledges only after the memory backend confirms the write, so a storage
   * failure cannot consume state needed by a later strict terminal retry.
   * @param messageList - The MessageList instance for the current thread.
   * @param memoryConfig - The memory configuration for saving.
   */
  private async persistUnsavedMessages(
    threadId: string,
    messageList: MessageList,
    memoryConfig?: MemoryConfigInternal,
  ) {
    const savedAt = Date.now();
    const snapshot = messageList.snapshotUnsavedMessages({ detached: true });
    const newMessages = filterMessagesForPersistence(snapshot.messages);
    if (newMessages.length > 0 && this.memory) {
      // Fork: the snapshot is acknowledged only after storage confirms the write
      // (below), so a failed save leaves these messages unsaved for a later retry
      // (the same guarantee upstream #27549 gets from drainUnsavedMessagesForSave().restore).
      const saved = await this.memory.saveMessages({
        messages: newMessages,
        memoryConfig,
        observationalMemorySourceWriteGuard: messageList.serialize().memoryInfo?.observationalMemorySourceWriteGuard,
      });
      this.notifySaved(saved);
      noteThreadMessagesSaved({ threadId, resourceId: newMessages.find(m => m.resourceId)?.resourceId, savedAt });
    }
    snapshot.commit();
  }

  /**
   * Persist without acknowledging the MessageList until storage confirms the
   * write. Unlike the historical best-effort queue path, errors propagate and
   * the same snapshot remains unsaved for an idempotent retry.
   */
  private async persistUnsavedMessagesStrict(
    snapshot: UnsavedMessageSnapshot,
    memoryConfig?: MemoryConfigInternal,
    observationalMemorySourceWriteGuard?: ObservationalMemorySourceWriteGuard,
  ) {
    const newMessages = filterMessagesForPersistence(snapshot.messages);
    if (newMessages.length === 0) {
      // Nothing persistable (e.g. only transient signals): acknowledge the snapshot.
      snapshot.commit();
      return;
    }
    if (!this.memory) {
      throw new Error('Cannot strictly persist messages without a memory backend');
    }
    const saved = await this.memory.saveMessages({
      messages: newMessages,
      memoryConfig,
      observationalMemorySourceWriteGuard,
    });
    snapshot.commit();
    this.notifySaved(saved);
    // Strict snapshots are captured before waiting in the queue. Their write time
    // is not a safe saved-through watermark for newer parts; do not announce it.
  }

  /**
   * Batches a save of unsaved messages for a thread, using debouncing to batch rapid updates.
   * If the oldest unsaved message is stale (older than MAX_STALENESS_MS), the save is performed immediately.
   * Otherwise, the save is delayed to batch multiple updates and reduce redundant writes.
   *
   * @param messageList - The MessageList instance containing unsaved messages.
   * @param threadId - The ID of the thread whose messages are being saved.
   * @param memoryConfig - Optional memory configuration for saving.
   */
  async batchMessages(messageList: MessageList, threadId?: string, memoryConfig?: MemoryConfigInternal) {
    if (!threadId) return;
    const earliest = messageList.getEarliestUnsavedMessageTimestamp();
    const now = Date.now();

    if (earliest && now - earliest > SaveQueueManager.MAX_STALENESS_MS) {
      return this.flushMessages(messageList, threadId, memoryConfig);
    } else {
      return this.debounceSave(threadId, messageList, memoryConfig);
    }
  }

  /**
   * Forces an immediate save of unsaved messages for a thread, bypassing any debounce delay.
   * This is used when a flush to persistent storage is required (e.g., on shutdown or critical transitions).
   *
   * @param messageList - The MessageList instance containing unsaved messages.
   * @param threadId - The ID of the thread whose messages are being saved.
   * @param memoryConfig - Optional memory configuration for saving.
   * @param options - Optional {@link FlushMessagesOptions}.
   */
  async flushMessages(
    messageList: MessageList,
    threadId?: string,
    memoryConfig?: MemoryConfigInternal,
    options?: FlushMessagesOptions,
  ) {
    if (!threadId) return;
    this.clearDebounce(threadId);
    return this.enqueueSave(threadId, messageList, memoryConfig, {
      beforePersist: options?.beforePersist,
      throwOnError: options?.throwOnError,
    });
  }

  /**
   * Flush a recoverability-critical transition. Storage failures reject and
   * leave the snapshot unsaved so the caller can retry before publishing.
   */
  async flushMessagesStrict(
    messageList: MessageList,
    threadId?: string,
    memoryConfig?: MemoryConfigInternal,
    options?: FlushMessagesOptions,
  ) {
    if (!threadId) throw new Error('Strict message persistence requires a thread id');
    this.clearDebounce(threadId);
    // Capture before waiting on earlier queue entries. A previously enqueued
    // best-effort save takes its own snapshot only when it starts and may
    // otherwise drain this recoverability-critical transition first.
    const strictSnapshot = messageList.snapshotUnsavedMessages({ detached: true });
    return this.enqueueSave(threadId, messageList, memoryConfig, {
      strictSnapshot,
      observationalMemorySourceWriteGuard: messageList.serialize().memoryInfo?.observationalMemorySourceWriteGuard,
      beforePersist: options?.beforePersist,
    });
  }

  /**
   * Cancels any debounced save for a thread and waits for saves already in flight to finish,
   * so a caller can change stored messages without a queued save landing afterwards.
   *
   * @param threadId - The ID of the thread whose pending saves should settle.
   */
  async flushPending(threadId: string) {
    this.clearDebounce(threadId);
    await this.saveQueues.get(threadId);
  }
}
