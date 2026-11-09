---
'@mastra/core': patch
---

Fixed `subagents.onDelegatedSettlement: 'remind'` when the parent's Observational Memory sets `sourceWriteFencing: 'required'`. The settlement reminder is now written with the source-write guard of the parent's next turn that has one, before that turn calls the model, instead of failing on every retry. Until such a turn starts, the notice stays owed without logging warnings.
