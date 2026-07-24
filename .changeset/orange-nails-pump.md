---
'@mastra/libsql': patch
---

Fixed plan-task lease fencing and durable replay metadata persistence. New keyed creates require a non-empty immutable input hash, while migrated legacy rows whose hash is `NULL` retain key-only replay compatibility.
