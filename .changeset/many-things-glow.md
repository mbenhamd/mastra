---
'@mastra/core': patch
---

Fixed agent evaluation to correctly include all tool calls when extracting evaluation steps. Previously, some tool calls were missing from trajectory analysis, which could skew evaluation results.
