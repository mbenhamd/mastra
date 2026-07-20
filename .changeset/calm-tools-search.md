---
'@mastra/core': patch
---

Hardened request-scoped agent execution and dynamic tool discovery. Dynamic models, memory, processors, and workspace tools now resolve once per execution boundary; parallel sub-agent and workflow invocations receive isolated request contexts; and context-backed ToolSearch state is reconstructed from persisted messages during durable preparation and cold approval resume.

ToolSearch now applies load policy to auto-loaded matches, rejects ambiguous or reserved catalog identities, supports explicit state cleanup and disposal, and records single-tool loads in the durable context result shape. Context storage accepts only canonical assistant activation results, marks auto-load results explicitly, uses request-local state rather than thread-global bridge state, and re-authorizes replayed activation receipts before exposing a tool.
