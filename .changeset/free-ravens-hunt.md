---
'@mastra/otel-exporter': patch
---

Fixed missing conversation IDs in exported traces. Spans now include the thread ID from metadata so related events are easier to follow.
