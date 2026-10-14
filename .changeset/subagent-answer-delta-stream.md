---
'@mastra/core': minor
---

Added `subagent_answer_delta` Harness events so a delegated subagent's answer streams as it is written. A subagent answers through the `summary` of its `report_subagent_outcome` call, which the model streams as tool input. Both `spawn_subagent` and task delegation now decode that summary incrementally and emit its characters in order on the parent session, each event tagged with the report's `innerToolCallId`. JSON escapes are decoded, and no event ends in half a surrogate pair.

The deltas of one report call form a prefix of the final direct answer's text, and equal it when the report completes as a direct answer. Streaming stops early, leaving a strict prefix that may be empty, when the model streams no tool input, when a display policy transforms or suppresses the report input, or when the answer exceeds the direct-answer size bound.

It also stops at a second top-level `summary` key. The parsed report keeps that later value, so the streamed first value is then not a prefix of it.

Interim subagent prose stays in `subagent_text_delta`, and `subagent_end` remains the authoritative result.
