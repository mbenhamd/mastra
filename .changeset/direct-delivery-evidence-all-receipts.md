---
'@mastra/core': patch
---

A subagent that ends by direct delivery now carries every framework-observed tool receipt as evidence, not only the terminal call. Callers that require each delegated call to be proven can verify a multi-call direct delivery. Run tool-result fallbacks and ambiguous call ids are still left out, and failed calls appear with an error status.
