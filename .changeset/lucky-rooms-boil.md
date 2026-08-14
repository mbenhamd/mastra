---
'@mastra/pg': patch
---

Fixed PostgreSQL observational-memory retraction so every stored generation's working-memory ownership is honored, duplicate message updates use one canonical destination, and malformed nested observer metadata no longer aborts cleanup.
