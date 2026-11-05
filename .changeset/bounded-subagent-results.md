---
'@mastra/core': patch
---

Improved the `subagent` tool for parents with many subagents. `status` listings and multi-subagent `wait` outputs now stay at or under 64 KiB, however many subagents there are and however large their results. `status` and `wait` without ids take an optional `offset` to read the next page.

- When an output leaves subagents out, `resultsHint` gives the `offset` that reads on from them, and following the hints reads every subagent once.
- `status` lists at most 25 subagents per call.
- Listings without ids are now ordered oldest first, by start time and then id.
- Positions can still shift when a subagent starts between calls with the same or an earlier start time, or when a new run begins (a resume continues the same run).

To read the next page, pass the `offset` that `resultsHint` gives:

```ts
{ action: 'status', offset: 25 }
{ action: 'wait', timeoutMs: 0, offset: 25 }
```
