---
'@mastra/core': patch
---

Fixed a storage driver failure being reported as a user cancellation when a native terminal turn's early answer stage failed. Only the turn's own cancellation now ends such a run as cancelled; any other stage failure fails the run, so no output processor (such as memory persistence) runs.
