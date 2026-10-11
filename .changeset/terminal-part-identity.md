---
'@mastra/core': patch
---

Terminal tool results are now delivered when a final output processor gives the assistant message a new id. Before, a memory processor that stamped its own message id made every terminal delivery fail with `Final output processors did not retain exactly one terminal tool result`, and the caller got no answer.

Earlier terminal answers that recalled history brings into the response no longer block delivery either. A processor that drops, duplicates, copies or rewrites a terminal result is still rejected.
