---
'@mastra/core': minor
---

Added `subagent_answer_delta` Harness events so a delegated subagent's answer streams as it is written. A subagent answers through the `summary` of its `report_subagent_outcome` call, which the model streams as tool input. The `spawn_subagent` bridge now decodes that summary incrementally and emits its characters in order on the parent session, each event tagged with the report's `innerToolCallId`. The JSON escapes are decoded, and no event ends in half a surrogate pair. When the spawn ends with a direct answer, the deltas of the reporting call concatenate to exactly that answer's text. Interim subagent prose stays in `subagent_text_delta`, and `subagent_end` remains the authoritative result.
