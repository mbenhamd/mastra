---
'@mastra/core': patch
'@mastra/inngest': patch
'@mastra/temporal': patch
---

Improve Agent processor performance by bypassing unsupported stream transforms, keeping heterogeneous processor phases direct, and using process-local workflow runs that skip unnecessary storage and lifecycle-event work for built-in run IDs. Reconcile processor-returned message histories in linear time and reuse stream signal callbacks. Transient runs now reject suspension, durable replay operations, and nesting of remote durable workflow engines.
