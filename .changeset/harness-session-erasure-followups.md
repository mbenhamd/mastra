---
'@mastra/core': patch
'@mastra/pg': patch
'@mastra/libsql': patch
---

Deleting a Harness session now also erases the content of the rows it keeps as fences. Its terminal admissions and intents lose their seed, terminal result and projection payload; its projection intents lose the session post-image; and its committed message results lose their result and error. Their identity, status, revision and hash fields stay, so a late admission is still refused and a committed retry still resolves to the stored winner. Admission replays now compare the seed by its hash.

PostgreSQL and LibSQL also gain session indexes on the channel and wakeup tables, so erasing a deleted session's channel and wakeup rows, and listing channel diagnostics, no longer scans those tables.

**Database change.** `init()` creates the new indexes automatically. If you run with `disableInit: true` or `MASTRA_DISABLE_STORAGE_INIT=true`, the store only checks that they exist, and Harness channel and wakeup storage calls fail until they do. That mode also requires the four channel and wakeup tables (`mastra_harness_channel_inbox`, `mastra_harness_channel_outbox`, `mastra_harness_channel_action_tokens` with `mastra_harness_channel_action_receipts`, and `mastra_harness_wakeups`), because session deletes now erase their rows. Create the indexes before deploying this version:

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

With a custom `schemaName`, qualify the tables and use the schema-prefixed index names the store expects.
