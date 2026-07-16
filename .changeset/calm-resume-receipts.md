---
'@mastra/core': minor
'@mastra/libsql': patch
'@mastra/pg': patch
---

Add public atomic workflow resume admission, checkpoint rollback, finalization receipt, result consumption, and fenced step-update primitives, with transactional implementations for LibSQL and PostgreSQL. Custom adapters fail closed for resumed execution until they advertise and implement both resume capability versions; their ordinary non-resume snapshot writes remain compatible.
