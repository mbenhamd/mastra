---
'@mastra/core': minor
---

Added `HarnessSession.isTurnActive()`, so a host can accept a new user message while a background `task_delegate` child is still working.

`isBusy()` stays true until every delegated child settles, which made a session look occupied long after its own turn ended. `isTurnActive()` is true only for the session's own turn work: a running turn, a queue drain, queued messages, or a pending approval, question or suspension. Background subagents do not count. An inline `spawn_subagent` still counts, because it runs inside the turn. `isBusy()`, `waitForIdle()` and shutdown are unchanged.

```ts
// Before: rejects every message until all background children finish
if (session.isBusy()) throw new Error('A turn is still active');

// After: admits the message, so the user can ask for status, or stop or redirect one child
if (session.isTurnActive()) throw new Error('A turn is still active');
await session.message({ content: 'Stop the first survey' });
```
