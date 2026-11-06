---
'@mastra/core': patch
---

A harness session whose run asks the user again after a resume, asks twice or asks and submits a plan in one step now parks each question or plan approval one after the other instead of failing the second call with "pending resume is already ...". A question asked by an agent-as-tool child is no longer announced on the parent session. After a restart, a question or plan approval whose run never reached its suspension is released instead of staying parked with no way to answer it.
