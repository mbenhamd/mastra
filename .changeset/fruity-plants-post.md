---
'@mastra/pg': patch
---

Fixed PostgreSQL parent-context reads so rejected provisional revision locks, including stale generation-zero rows from earlier reads, are removed before authoritative recovery.
