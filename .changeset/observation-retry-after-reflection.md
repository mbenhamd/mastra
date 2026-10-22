---
'@mastra/core': patch
'@mastra/pg': patch
'@mastra/libsql': patch
---

Fixed observational memory failing an observation when another process reflected the same memory at the same time. Saving observations to the older memory record now reports that it was replaced, so the observation is retried against the new record instead of failing. Writes to memory that was cleared still fail as before. A guarded write naming another resource or thread is now refused in every store, including libSQL.
