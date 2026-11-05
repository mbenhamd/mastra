---
'@mastra/core': patch
---

The `subagent` tool's `status` listings and multi-subagent `wait` outputs now always stay under 64 KiB, however many subagents there are and however large their results and task texts. When an output would be too large:

- A subagent you name, or one that settles during a wait, gets its full result while it fits.
- Other subagents get a short preview of their result, marked `resultPreview`.
- A result that does not fit at all is left out, marked `resultOmitted`.
- If the subagents still do not fit, their task texts are shortened.
- If that is not enough, the last subagents are left out. `omittedSubagentCount` counts them, and `resultsHint` gives the `offset` that reads on from them.
- `status` and `wait` without ids take an optional `offset` to page through a long list of subagents. The offset is a place in the list of all your subagents, running and settled, ordered by start time and then id. A subagent keeps its place when it settles, so following the hints reads every subagent once. A `wait` with an offset also returns the settled subagents from that place on.
- `status` lists at most 25 subagents per call; `resultsHint` gives the offset of the next ones.
- Listings without ids are now ordered oldest first, by start time and then id, so a new subagent joins at the end.
- A `spawn_subagent` subagent stays listed, at its place, for the rest of the turn in which its call returned.
- Pages can still shift when a subagent starts between calls with the same or an earlier start time, or when a new turn begins: a `spawn_subagent` subagent whose call returned in an earlier turn is no longer listed.
- Any shortened output sets `resultsShortened`, and `resultsHint` says how to read a full result.

A call that names exactly one subagent always returns its full result, which the terminal result caps already bound.
