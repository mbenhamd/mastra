---
'@mastra/pg': patch
---

Fixed PostgreSQL observational-memory retraction so every stored generation's working-memory ownership is honored, owner-only rows are not rewritten when nothing derived can be removed, duplicate message updates use one canonical destination, and malformed nested observer metadata no longer aborts cleanup.
