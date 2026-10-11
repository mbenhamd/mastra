---
'@mastra/core': patch
'@mastra/pg': patch
'@mastra/libsql': patch
---

Deleting a Harness session now also deletes the session's run summaries. Before, `deleteSessions` left each run's summary row behind (its resource, thread and session ids, model, status and token usage), so a deleted session's identifiers outlived it. A run summary is now written only while its session exists, so a run that finishes after the delete cannot bring one back, and deleting a session tree first waits for its live sessions' pending summary writes. PostgreSQL gains the `(harness_name, session_id, completed_at, run_id)` run-summary index LibSQL already had. The terminal session fence is kept on purpose.
