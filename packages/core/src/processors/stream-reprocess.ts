import type { ChunkType } from '../stream';

/**
 * Well-known key a stream output processor can set on its `state` to ask the
 * ProcessorRunner to re-drive an additional part through the full output
 * processor chain after the current part has been emitted.
 *
 * `processOutputStream` can only return a single part, but some processors
 * (e.g. `BatchPartsProcessor`) need to emit two parts for one input: a flushed
 * batch of buffered text plus the non-text part that triggered the flush. The
 * processor returns the flushed batch (so it flows through downstream
 * processors normally) and stashes the non-text part under this key. The runner
 * then re-feeds the stashed part through the whole chain so it also receives
 * downstream processing and is emitted in order — instead of being deferred to
 * a "next" call that may never happen (which dropped the part when a `stopWhen`
 * condition stopped the agent on that part — issue #17094).
 */
export const REPROCESS_PART_KEY = '__mastraReprocessPart';

/**
 * Well-known key a stream output processor sets on its `state` when it holds
 * parts back until a later part arrives (a batch, a sentence buffer). The value
 * is a function that releases every held part, already processed by the
 * holding processor, and clears the hold.
 *
 * When the stream ends, the runner calls it and pushes the released parts
 * through the processors after the holder, so held text is not dropped when
 * nothing follows the last held part (issue #27523).
 */
export const FLUSH_HELD_PARTS_KEY = '__mastraFlushHeldParts';

/**
 * With `discard`, the hold is cleared without running any checks: held content
 * (see {@link withoutHeldContent}) is dropped and only held control parts are
 * returned, since the stream failed or was aborted and its content must not be emitted.
 */
export type FlushHeldParts = ((options?: { discard?: boolean }) => Promise<ChunkType[]> | ChunkType[]) & {
  /**
   * Id of the processor that registered the hook. A processor that runs another processor
   * inside its own `processOutputStream` and passes it its `state` (e.g. one that holds text
   * in a `BatchPartsProcessor` until its own check passes) gets the inner processor's hook on
   * its state; the runner only releases through a hook its holder registered, so the inner
   * batch is not released past the outer processor's check. Discards are not limited.
   */
  ownerId?: string;
  /** `false` when the registering processor publishes the held parts itself; only discards then run the hook. */
  releasable?: boolean;
};

/** Whether a part marks its model stream as errored or aborted (held parts are then discarded). */
export function isFailedStreamChunk<OUTPUT>(part: ChunkType<OUTPUT>): boolean {
  if (part.type === 'error' || part.type === 'abort') return true;
  if (part.type !== 'finish') return false;
  const reason = (part.payload as { stepResult?: { reason?: string } } | undefined)?.stepResult?.reason;
  return reason === 'error' || reason === 'abort';
}

/** Drops content parts from a discarded hold, keeping control parts (start, error, finish, ...). */
export function withoutHeldContent<T extends { type: string }>(parts: T[]): T[] {
  return parts.filter(part => part.type !== 'text-delta' && part.type !== 'reasoning-delta');
}

/** Releases the parts a processor is holding, or none if it holds nothing. */
export async function takeHeldParts(
  state: Record<string, unknown> | undefined,
  options?: { discard?: boolean },
  holderId?: string,
): Promise<ChunkType[]> {
  const flush = state?.[FLUSH_HELD_PARTS_KEY] as FlushHeldParts | undefined;
  if (!flush) return [];
  // Only a release is limited to the holder's own hook. A discard (failed or aborted stream)
  // always clears the hold, including a nested one, so nothing stale reaches the next reply.
  if (
    !options?.discard &&
    (flush.releasable === false ||
      (holderId !== undefined && flush.ownerId !== undefined && flush.ownerId !== holderId))
  ) {
    return [];
  }
  return await flush(options);
}
