---
'@mastra/core': patch
---

Improve Agent processor performance by using process-local workflow runs that skip unnecessary storage lookups and unsupported processor phases. Transient runs now reject suspension and durable replay operations.
