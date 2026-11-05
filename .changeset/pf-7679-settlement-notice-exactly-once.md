---
'@mastra/core': patch
---

Harness v1: a delegated settlement notice in `wake` mode (`subagents.onDelegatedSettlement: 'wake'`) now reaches the parent model exactly once.

- **Timing change:** a settlement wake now waits until the parent has no running turn, parked interaction or queued turn before it claims and delivers the notice. Delivery timing changes; the exactly-once contract does not.
- **No duplicate through history:** if a fresh parent run of the agent whose memory saved the reminder row started after the save, that run already read the notice from history, so no wake is sent. Before, such a run and the wake could both deliver it.
- **No loss to a caller turn:** if a caller turn takes the idle slot after the wake claimed the notice, the notice is owed again and the wake waits for that turn to end. Before, the claimed notice was sent into that turn unlabelled, and a parent without memory could lose it when the wake was refused after the claim. A parent turn that began after the save also no longer suppresses the wake when no fresh run read the row (always the case without memory).
- **Close, shutdown and crash:** a close, Harness shutdown or crash while the wake waits leaves the notice owed, and the next owner of the session delivers it.
- **Remaining cases:** a crash during the claim write itself can lose the wake of a parent without memory. A dynamic or inherited memory, or a row saved to another mode's memory, is never treated as already delivered, so a caller run may read the row and the wake may still follow.
