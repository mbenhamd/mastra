---
'@mastra/core': patch
'@mastra/pg': patch
'@mastra/libsql': patch
---

Deleting a Harness session now also erases the content of the rows it keeps as fences. Its terminal admissions lose their seed, its terminal intents and admissions keep only the projection's kind, id, size and hash and the terminal result's status, run, time, finish reason and error code, its settled projection intents lose the session post-image and error, and its committed message results lose their result and error. Their identity, status, revision and hash fields stay, so a late admission is still refused, a committed retry still resolves to the stored winner, and a late acknowledgement still resolves as fenced or duplicate. Admission replays now compare the seed by its hash. Grant tombstones store only the reason code.

A duplicate or waiting caller that asks for the result of a turn whose session was deleted now fails with `HarnessMessageResultErasedError` (`harness.message_result_erased`) instead of resolving with an empty result.

PostgreSQL and LibSQL also gain session indexes on the channel and wakeup tables, so erasing a deleted session's channel and wakeup rows, and listing channel diagnostics, no longer scans those tables.

**Database change.** `init()` creates the new indexes automatically. If you run with `disableInit: true` or `MASTRA_DISABLE_STORAGE_INIT=true`, the store only checks that they exist, and every Harness channel and wakeup call, and every session delete (`deleteSessions` validates all four channel and wakeup tables and their indexes), fails until they do. That mode also requires the four channel and wakeup tables themselves (`mastra_harness_channel_inbox`, `mastra_harness_channel_outbox`, `mastra_harness_channel_action_tokens` with `mastra_harness_channel_action_receipts`, and `mastra_harness_wakeups`). Create the indexes before deploying this version:

```sql
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_harness_channel_inbox_session
  ON mastra_harness_channel_inbox (harness_name, session_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_harness_channel_outbox_session
  ON mastra_harness_channel_outbox (harness_name, session_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_harness_channel_outbox_owning_session
  ON mastra_harness_channel_outbox (harness_name, owning_session_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_harness_channel_action_tokens_owning_session
  ON mastra_harness_channel_action_tokens (harness_name, owning_session_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_harness_channel_action_receipts_owning_session
  ON mastra_harness_channel_action_receipts (harness_name, owning_session_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_harness_wakeups_session
  ON mastra_harness_wakeups (harness_name, session_id);
```

With a custom `schemaName`, take the exact statements from `exportSchemas(schemaName)` from `@mastra/pg`: the store prefixes index names with the schema and shortens names longer than 63 bytes with a hash suffix, so the names differ from the ones above.
