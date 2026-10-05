/**
 * Tombstone authority a `complete` export stamps onto the rows it leaves
 * behind: exported sessions take it as `owner_id` and exported live outbox
 * rows take it as `claim_id`, each with `EXPORTED_FENCE_EXPIRES_AT` as the
 * expiry. The U+001F prefix cannot collide with a runtime-generated owner or
 * claim id (the same convention the channel-binding external-id sentinel
 * uses), and the far-future expiry makes the row read as permanently claimed
 * to every lease/claim predicate — `acquireSessionLease`, lease renewals, the
 * save paths, and `claimChannelOutbox` all refuse it with no special case.
 * Only the owner can release or renew a claim, and no worker ever holds this
 * id, so the fence is durable: the exported epoch can never resume on the
 * source.
 */
export const EXPORTED_FENCE_AUTHORITY = '\x1f__mastra_execution_closure_exported__';
/**
 * Far-future expiry for {@link EXPORTED_FENCE_AUTHORITY} rows — the largest
 * timestamp `Date` can represent (~275,760 years out), so no TTL ever reaches
 * it while conflict errors that format it via `new Date(...)` stay valid.
 */
export const EXPORTED_FENCE_EXPIRES_AT = 8_640_000_000_000_000;
