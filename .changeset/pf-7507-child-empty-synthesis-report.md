---
'@mastra/core': patch
---

Fixed delegated Harness subagents that finish their tool work without writing a reply. The empty-reply recovery used to forbid every tool, so such a subagent could not call `report_subagent_outcome` and ended with `harness.subagent_outcome_missing`. It now asks the subagent for its outcome report and allows only that tool for one step. Top-level sessions keep the existing text-only recovery.
