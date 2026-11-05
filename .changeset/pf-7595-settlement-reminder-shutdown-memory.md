---
'@mastra/core': patch
---

Fixed delegated-settlement reminders in Harness:

- `shutdown()` now waits for reminder writes to finish, or reports an error at its deadline.
- `subagents.onDelegatedSettlement: 'remind'` is rejected when a mode that can delegate has no memory.
- A parent that switched to a memory-less mode after delegating still gets the reminder, in the memory of the mode it delegated from.

Before, a `remind` Harness whose delegating agent had no memory started, then silently dropped every reminder. Now construction throws a `HarnessConfigError` naming the mode. To fix it, give that agent memory, set `harnessBuiltins: 'exclude'` on modes that must not delegate, or use `'wake'` or `'none'`:

```ts
const harness = new Harness({
  agents: { parent: new Agent({ /* ... */ memory: new Memory() }) },
  modes: [{ id: 'default', agentId: 'parent' }],
  subagents: { onDelegatedSettlement: 'remind', types: { worker: { /* ... */ } } },
  // ...
});
```
