---
'@mastra/core': patch
---

Keep upstream AgentController as the canonical controller API while exposing the fork's durable Harness v1 runtime only from `@mastra/core/harness/v1`, without carrying a second legacy Harness implementation. Harness v1 now also flushes pending assistant drafts before committing a session's closed marker.
