---
'@mastra/core': patch
---

The `subagent` tool's `status` listings and multi-subagent `wait` outputs now keep results under 64 KiB. A subagent you name, or one that settles during a wait, gets its full result while it fits. Other subagents get a short preview, marked `resultPreview`, and a result that does not fit is left out, marked `resultOmitted`. The output then sets `resultsShortened` and says how to read a full result. A call that names exactly one subagent always returns its full result, which the terminal result caps already bound.
