---
'@mastra/pg': patch
---

Fixed PostgreSQL observational-memory retraction so every stored generation's working-memory ownership is honored and malformed nested observer metadata no longer aborts cleanup.
