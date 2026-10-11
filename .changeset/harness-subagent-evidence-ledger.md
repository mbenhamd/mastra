---
'@mastra/core': patch
---

Harness v1 now verifies a subagent's `report_subagent_outcome` tool evidence against the child's recorded tool calls, so a report no longer has to quote framework tool-call ids the model was never shown.

A `tool-result` evidence item names the tool and its observed status. `toolCallId` is now optional. An item without an id binds to one distinct recorded call of that tool with that status, in execution order. The verified report, the parent's tool result, and the projected receipts carry the bound framework id. A report is still rejected with `harness.subagent_evidence_unverified` when it claims a tool the child never ran, claims success for a call that failed, claims more runs than were recorded, or cites an id that is not in the record. Items that cite an exact `toolCallId` are verified exactly as before.

```json
{
  "outcome": "completed",
  "summary": "The mean is 4.2.",
  "evidence": [
    {
      "kind": "tool-result",
      "toolName": "run_python_analysis",
      "status": "success",
      "description": "The sandbox computed the mean."
    }
  ]
}
```
