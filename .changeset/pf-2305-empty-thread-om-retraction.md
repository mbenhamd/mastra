---
'@mastra/memory': patch
---

Stop a rejected non-atomic `deleteThread` from retracting the resource-scoped observational memory that sibling threads share when the targeted thread never held messages. The reconciler now decides from the pre-attempt transcript state, so an empty thread's rejected deletion — whether its row survives the rejection or its deletion had already committed — no longer wipes observational memory it never contributed to.
