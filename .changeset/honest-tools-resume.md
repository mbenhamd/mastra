---
'@mastra/core': patch
'@mastra/ai-sdk': patch
---

Authenticated suspended tool resumes against the exact run, loop iteration, step, call, tool, and argument identity. Corrupt or replayed resume evidence now fails before policy evaluation or tool execution, falsy suspension payloads resume background work without redispatch, and approval provenance survives durable storage plus AI SDK v4/v6 conversion.
