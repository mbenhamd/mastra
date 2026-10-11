---
'@mastra/core': patch
'@mastra/pg': patch
'@mastra/libsql': patch
---

A Harness run summary is now written only while its session exists. A run that finishes after its session was deleted no longer brings the deleted session's identifiers back: `saveRunSummary` drops the write and returns `null`. PostgreSQL checks the session under a key-share row lock that serializes with `deleteSessions`; LibSQL and the in-memory store check inside the same write. PostgreSQL also gains the `(harness_name, session_id, completed_at, run_id)` run-summary index that LibSQL already had, so deleting a session's summaries no longer scans the table.
