---
'@mastra/core': patch
---

Fixed Harness parent agents getting no result when a subagent's `report_subagent_outcome` cites the same tool call twice. Such a report now verifies instead of failing with `harness.subagent_evidence_unverified`.

Evidence still needs a recorded tool call with the same tool name and status behind it.
