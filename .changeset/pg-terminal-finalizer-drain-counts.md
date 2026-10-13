---
'@mastra/pg': patch
---

Added `countPendingTerminalAdmissions` and `countUndeliveredTerminalIntents` to the Postgres harness store. They count the turns still pending, and the delivery intents not yet delivered, for one terminal finalizer version. Both reads use existing tables and indexes, so no schema change is needed.
