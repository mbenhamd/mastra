---
'@mastra/core': patch
---

Fixed scheduled workflows and their public event streams so they keep working across processes.

Scheduled and long-running workflows now keep their event streams connected while they are suspended,
cancelled, or completing, including when a run ID is reused promptly after completion.
