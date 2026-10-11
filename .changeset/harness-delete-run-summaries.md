---
'@mastra/core': patch
'@mastra/pg': patch
'@mastra/libsql': patch
---

Deleting a Harness session now also deletes the session's run summaries. Before, `deleteSessions` left each run's summary row behind (its resource, thread and session ids, model, status and token usage), so a deleted session's identifiers outlived it. The terminal session fence is kept on purpose.
