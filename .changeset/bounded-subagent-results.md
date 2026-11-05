---
'@mastra/core': patch
---

The `subagent` tool's `status` and `wait` outputs now stay under 64 KiB. A subagent you name, or one that settles during a wait, gets its full result. Other subagents get a short preview, marked `resultPreview`. A result that does not fit is left out and marked `resultOmitted`. When this happens, the output sets `resultsShortened` and says how to read a full result.
