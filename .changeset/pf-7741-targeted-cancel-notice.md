---
'@mastra/core': patch
---

Fixed how delegated subagent tasks settle and notify the parent when a cancel races their result.

- Cancelling a delegated subagent with `cancelSubagent()` never sends the parent a settlement reminder or wake turn, whatever the cancel reason, including `'delegation_timeout'`. The task settles as `cancelled`. A delegated task that really runs past its deadline still notifies the parent.
- A delegated task that completed always notifies the parent, even when the parent cancels the subagent just after its result arrived.
- After a restart, a cancel that reaches the subagent while its saved result is being read now always wins: the parent's own `cancelSubagent()` settles the task as `cancelled`, and any other cancel, such as a Stop, settles it as `failed` instead of reporting the saved result.
