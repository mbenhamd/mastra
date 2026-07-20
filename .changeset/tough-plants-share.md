---
'@mastra/core': patch
---

Fixed durable plan-task replay, lease fencing, and delegated child cleanup. New keyed creates require a non-empty immutable input hash so replay conflicts cannot silently alias different work; migrated legacy rows whose hash is `NULL` remain key-only compatible.
