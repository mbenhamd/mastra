---
'@mastra/core': patch
---

Fixed lost replies when a process dies after the model has answered but before the run completes. A native terminal turn now durably stages the model's answer, with its usage, as soon as the model finishes. This happens before output processors (such as memory persistence), `onFinish`, and completion delivery run. Recovery then settles the turn as completed with that reply under its recorded finalizer, instead of marking it interrupted. If the stage fails, it is retried once. If it still cannot be confirmed durable, the run fails before any output processor runs, so memory never keeps a reply that recovery cannot find. Staging reuses the admission this process already knows about, so each stage is a single storage transaction.
