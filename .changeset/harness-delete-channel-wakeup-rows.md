---
'@mastra/core': patch
'@mastra/pg': patch
'@mastra/libsql': patch
---

Deleting a Harness session now also erases its channel and wakeup rows: its channel inbox items (inbound content), the outbox items it sent or owns (outbound payloads), the action tokens and receipts it owns, and its wakeups. Before, `deleteSessions` left all of them behind, keyed by the deleted session id.

A channel or wakeup write that names a session now stores its row only while that session exists, and otherwise fails with `HarnessStorageSessionNotFoundError`, so a late write cannot bring the rows back. Rows that name no session are unaffected. The content-free terminal session fences and grant tombstones are kept on purpose.
