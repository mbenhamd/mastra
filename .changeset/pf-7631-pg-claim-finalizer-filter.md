---
'@mastra/pg': minor
---

`claimTerminalIntents` accepts a `finalizerIds` filter. Per-session claim ordering now holds within one finalizer id, so an unsettled intent of one kind never blocks another kind. This is a query change only; the schema is unchanged.
