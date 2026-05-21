---
'@mastra/core': patch
---

Harness v1 channel wakeups now record `channel_binding_closed` and stop instead of running with stale or mismatched channel binding context.
