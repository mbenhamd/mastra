---
'@mastra/memory': patch
---

Fixed `ObservationTurn.end()` and turn rollover so observer model errors reach the caller under the default `abort` failure policy. With `continue`, observer model errors still resolve. Chat messages are saved before observation.
