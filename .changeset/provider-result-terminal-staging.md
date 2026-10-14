---
'@mastra/core': patch
---

Fixed lost replies when a process dies after the model has answered but before the run completes. A native terminal turn now durably stages the model's answer, with its usage, as soon as the model finishes. The staging runs alongside output processors (such as memory persistence) and finishes before `onFinish` or completion delivery. Recovery then settles the turn as completed with that reply under its recorded finalizer, instead of marking it interrupted. If this early staging fails, the run continues, and the existing completed-output staging still applies.
