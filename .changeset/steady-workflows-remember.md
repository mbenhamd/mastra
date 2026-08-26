---
'@mastra/pg': patch
---

Improved PostgreSQL workflow upgrades for `jsonb`, `json`, and legacy `text` snapshots. Upgrades now preserve parent revision generations. Missing a required migration record now stops initialization. Malformed legacy JSON now rolls back the migration. Recovery now requires one atomic backup and restore of migration records and workflow data.
