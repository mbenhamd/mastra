---
'@mastra/core': patch
'@mastra/pg': patch
'@mastra/libsql': patch
---

Fixed a workflow resume being admitted, rolled back or finalized by an execution that had already lost its claim on the run. After another execution takes the run over, those resume writes from the old one are now refused, like the run's other writes, so they can no longer overwrite the new owner's state. Writes made outside a durable run behave as before.
