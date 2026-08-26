---
'@mastra/pg': patch
---

PostgreSQL workflow storage now preserves parent revision generations across `jsonb`, `json`, and legacy `text` snapshot upgrades. Partial migration-provenance loss fails closed, malformed legacy JSON rolls the migration back, and recovery guidance now requires one atomic backup and restore of provenance plus workflow evidence.
