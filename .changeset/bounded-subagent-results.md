---
'@mastra/core': patch
---

The `subagent` tool's `status` listings and multi-subagent `wait` outputs now stay under 64 KiB, whatever the number of subagents or the size of their results and task texts. A subagent you name, or one that settles during a wait, gets its full result while it fits. Other subagents get a short preview, marked `resultPreview`, and a result that does not fit is left out, marked `resultOmitted`. If the subagents still do not fit, their task texts are shortened, and then the last subagents are left out: `omittedSubagentCount` counts them and `resultsHint` lists their ids for a `wait` with `timeoutMs: 0`. The output then sets `resultsShortened`, and `resultsHint` says how to read a full result. A call that names exactly one subagent always returns its full result, which the terminal result caps already bound.
