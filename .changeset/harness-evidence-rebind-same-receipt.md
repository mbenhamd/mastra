---
'@mastra/core': patch
---

Fixed Harness subagents losing a correct result when the subagent's `report_subagent_outcome` cites the same tool call twice. Previously, a second description of one recorded call (for example, one search call cited once for its query and once for its results) made the whole report fail with `harness.subagent_evidence_unverified`, and the parent got no result. Now a claim without a tool call id that finds no uncited matching call binds again to an already cited call with the same tool name and status. Evidence is still never accepted without a recorded call of that name and status behind it, and a call whose receipts disagree is still never bound by name.
