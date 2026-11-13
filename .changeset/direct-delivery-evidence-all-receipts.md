---
'@mastra/core': patch
---

A subagent that ends by direct delivery can now prove its earlier tool calls, not only the last one, as long as they fit the result's limits. The inline `spawn_subagent` result lists the delivering call first, then the subagent's earlier framework-observed tool calls that fit.

- Earlier calls are added only while they fit the result's limits: 24 evidence items and 64 KiB. A call that doesn't fit is left out and stays unproven.
- Failed earlier calls are listed with an error status.
- Ambiguous call ids and provider-reported results are never added.
- Results saved to plan tasks keep only the delivering call, as before.
- A direct delivery that was valid before stays valid.
