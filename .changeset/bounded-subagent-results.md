---
'@mastra/core': patch
---

The `subagent` tool's `status` listings and multi-subagent `wait` outputs now always stay under 64 KiB, however many subagents there are and however large their results and task texts. When an output would be too large:

- A subagent you name, or one that settles during a wait, gets its full result while it fits.
- Other subagents get a short preview of their result, marked `resultPreview`.
- A result that does not fit at all is left out, marked `resultOmitted`.
- If the subagents still do not fit, their task texts are shortened.
- If that is not enough, the last subagents are left out. `omittedSubagentCount` counts them, and `resultsHint` gives the `offset` that reads on from them.
- `status` and `wait` without ids take an optional `offset` to page through a long list of subagents.
- Any shortened output sets `resultsShortened`, and `resultsHint` says how to read a full result.

A call that names exactly one subagent always returns its full result, which the terminal result caps already bound.
