---
'@mastra/memory': patch
---

Fixed Observational Memory ending a turn when the Reflector model is slow to answer. With `reflection.failurePolicy: 'continue'`, a reflection that hits Observational Memory's own idle timeout or deadline is now deferred like any other Reflector model failure, so the turn continues. Cancelling the run still stops it.
