---
'@mastra/core': patch
---

A parent can now act on its first finished subagent without waiting for the others. Pass `until: 'any'` to the `subagent` tool's `wait`, and it returns as soon as one of the subagents has settled. The default, `until: 'all'`, still waits for every one. The subagents that settled during an `until: 'any'` wait are always in its output, even when a long list is cut short to stay under 64 KiB. A `wait` with an `offset` also returns the subagents already settled on its page, but only a subagent that settles during the call ends an `until: 'any'` wait. A wait also returns as soon as a `task_delegate` subagent settles in the same process, instead of on the next poll.

The model calls the `subagent` tool with input like this:

```ts
{ action: 'wait', until: 'any', timeoutMs: 10_000 }
```
