---
'@mastra/core': patch
---

Added `until: 'any'` to the `subagent` tool's `wait` action, so a parent can act on its first finished subagent without waiting for the others. The wait returns as soon as one of the subagents has settled. The default, `until: 'all'`, still waits for every one.

- The subagents that settled during an `until: 'any'` wait are always in its output, even when a long list is cut short to stay under 64 KiB.
- In a `wait` without ids, only a subagent that settles during the call ends an `until: 'any'` wait. A `wait` with an `offset` also returns the subagents already settled on its page, but they do not end it.
- A `wait` that names ids returns at once when one of them has already settled.
- A wait now returns as soon as a `task_delegate` subagent in the same process settles, instead of on the next poll.

The model calls the `subagent` tool with input like this:

```ts
{ action: 'wait', until: 'any', timeoutMs: 10_000 }
```
