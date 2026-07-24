---
'@mastra/core': patch
---

Harness per-session Observational Memory now rejects enabled configuration at construction and rejects model-switch calls before persistence while native per-turn OM engine selection is unavailable. Sessions carrying unsupported overrides written by older builds park queued turns without mutating admission state; `session.om.clearOverride()` clears the legacy value and automatically re-kicks the queue without waiting for model execution. This prevents silent no-ops and MessageHistory suppression; configure OM directly on the Agent Memory instance in the meantime.
