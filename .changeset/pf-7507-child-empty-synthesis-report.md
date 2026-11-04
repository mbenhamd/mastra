---
'@mastra/core': patch
---

Fixed delegated Harness subagents that finish their tool work without writing a reply. The empty-reply recovery used to forbid every tool, so such a subagent's `report_subagent_outcome` call was refused and it ended with `harness.subagent_incomplete`. It now asks the subagent for its outcome report and allows only that tool for one step. Top-level sessions keep the existing text-only recovery.
