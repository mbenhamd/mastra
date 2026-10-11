---
'@mastra/pg': patch
---

Deleting a Harness session now erases the content of the rows it keeps for replay protection, and the channel and wakeup tables gain session indexes, so erasing a deleted session's rows no longer scans them.

**Database change.** `init()` creates the new indexes. With `disableInit: true` or `MASTRA_DISABLE_STORAGE_INIT=true` the store only checks that they exist, and every session delete and every channel and wakeup call fails until they do. That mode also needs the four channel and wakeup tables. Create the indexes before deploying:

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

With a custom `schemaName`, take the exact statements from `exportSchemas(schemaName)`: index names are prefixed with the schema and shortened with a hash suffix past 63 bytes.
