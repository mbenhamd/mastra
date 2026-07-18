---
'@mastra/core': patch
'@mastra/inngest': patch
'@mastra/temporal': patch
---

Improve Agent processor performance by using process-local workflow runs that skip unnecessary storage and external lifecycle-broker traffic for built-in run IDs and unsupported processor phases. Reconcile workflow-returned message histories in linear time and reuse stream signal callbacks. Transient runs now reject suspension, durable replay operations, and nesting of remote durable workflow engines.
