---
'@mastra/core': patch
'@mastra/inngest': patch
'@mastra/temporal': patch
---

Improve Agent processor performance by using process-local workflow runs that skip unnecessary storage lookups and unsupported processor phases. Transient runs now reject suspension, durable replay operations, and nesting of remote durable workflow engines.
