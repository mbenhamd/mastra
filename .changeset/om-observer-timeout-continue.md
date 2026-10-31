---
'@mastra/memory': patch
---

Fixed Observational Memory ending a turn without an answer when the Observer model hangs.

- With `observation.failurePolicy: 'continue'`, an Observer call that Observational Memory itself times out is now treated like any other Observer model failure: observation is deferred and the step goes on to call the model. Cancelling the run is still fatal.
- A model call that streams nothing for 20 seconds is now aborted and retried, so one hung attempt no longer uses the whole 60-second budget for the operation.
- When a step's observation fails, the turn now saves its messages without running end-of-turn observation again, so the failure is reported without further Observer calls.
