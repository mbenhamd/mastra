---
'@mastra/server': patch
'@mastra/libsql': patch
'@mastra/pg': patch
---

Hardened durable Harness human-in-the-loop interactions with exact pending-generation coordinates and expiry metadata in server request/response schemas. LibSQL and PostgreSQL Harness stores now index due approval, question, plan, and sandbox interactions so expired or interrupted waits can be recovered and terminalized after a process restart.
