---
'@mastra/core': patch
---

Preserve completed native terminal replies across a process restart. The Harness stages output durably before delivering run completion or writing session usage. Recovery commits that completed output under the admission's recorded finalizer instead of interrupting the turn. Session usage is reconciled once across a crash before or after its save, and a failed completion publication remains recoverable without sealing a provisional failed summary. A resumed turn uses the same durable output to finish settlement without calling the provider again. A committed completed turn stays recoverable until its run summary exists. After upgrading, any earlier committed completed turn whose summary write failed is therefore published once, as a reconstructed completion, when its session is next recovered.
