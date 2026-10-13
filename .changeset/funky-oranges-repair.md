---
'@mastra/memory': patch
---

Fixed `ObservationTurn.end()` and turn rollover so observer model errors reach the caller under the default `abort` failure policy. With `continue`, observer model errors still resolve, but non-model failures at turn end or rollover (such as storage or cancellation errors) now reject instead of being swallowed. Chat messages are saved before observation.
