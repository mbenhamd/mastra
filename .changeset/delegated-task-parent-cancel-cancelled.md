---
'@mastra/core': patch
---

A delegated plan task now ends as `cancelled` when the parent cancels its subagent with the `subagent` tool or `cancelSubagent`. The `subagent` tool's status view also shows `cancelled`.

Timeouts, child errors, stopping the parent, and cancels from anyone else still end as `failed`. If two cancels happen close together, the earlier one decides the result. This also holds after a restart.

**Before / after**

`DelegatedSubagentOutcome` and the plan task view's `delegation.status` now include `'cancelled'`. Before, a parent cancel showed as `'failed'`. Code that only checks `'failed'` must also handle `'cancelled'`:

```ts
const status = view.delegation?.status;
if (status === 'failed' || status === 'cancelled') {
  // the delegated task did not complete
}
```
