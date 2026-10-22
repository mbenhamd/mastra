---
'@mastra/core': patch
'@mastra/pg': patch
'@mastra/libsql': patch
---

Fixed observational memory failing an observation when another process reflected the same memory at the same time. Saving observations to the older memory record now reports that it was replaced, so the observation is retried against the new record instead of failing. Writes to memory that was cleared still fail as before. Before reporting a record as replaced, the PostgreSQL and libSQL stores now check that the write names that record's resource and thread, so a write for another resource or thread is refused instead.
