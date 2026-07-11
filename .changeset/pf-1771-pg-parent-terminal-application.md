---
'@mastra/pg': minor
---

PostgreSQL workflow storage now implements version 1 graph-bound parent terminal application in one transaction. The locked parent snapshot, storage-owned revision generation, canonical receipt transition, immutable PF-1781 contract, and child journal evidence converge across racing adapter instances, while stale revisions roll back without orphan rows.

Committed continuations use a managed, schema-qualified JSONB contract table with strict continuous-mode and framework-action-key checks, canonical receipt/effect foreign keys, deterministic identity validation, cleanup/export support, and fenced recovery after process restart. A separate managed revision/tombstone table replaces `xmin`, seeds pre-table snapshots idempotently, uses the database clock, and changes generation on every workflow snapshot mutation.
