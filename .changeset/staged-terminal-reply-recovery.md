---
'@mastra/core': patch
---

Preserve completed native terminal replies across a process restart. The Harness stages output durably before delivering run completion or writing session usage. Recovery commits that completed output under the admission's recorded finalizer instead of interrupting the turn. A resumed turn uses the same durable output to finish settlement without calling the provider again.
