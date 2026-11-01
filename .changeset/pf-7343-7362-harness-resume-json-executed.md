---
'@mastra/core': patch
---

Fixed two Harness inbox issues with paused tool calls:

- Pending question, plan, approval and suspension payloads now leave out arguments the model did not provide, instead of storing them as `undefined`. They stay valid JSON, so strict consumers can read them, for example a question asked without `selectionMode`.
- When an approved or answered tool already ran and only the follow-up model call failed, the interaction is no longer offered again as answerable. It is closed with a final, non-retryable failure, so a reload or a second client cannot answer it again.
