---
'@mastra/core': patch
---

Asynchronous validation now waits for the JSON Schema validator to load, so a call right after startup is not refused as `JSON Schema validator unavailable`. This covers tool execution (input, output, suspend, resume and request context), the AI SDK tool-input hook, tool approval preflight and edited-argument checks, workflow input, resume, suspend, state and request context, structured output, terminal tool results, agent and durable-agent request context, agent tool resume data and adopted tool output, and agent-controller session state. `jsonSchemaValidatorReady` is re-exported from `@mastra/core/schema`. Synchronous callers can still see the startup window: the `validateToolInput` and `validateToolOutput` helpers from `@mastra/core/tools` and direct synchronous `validate` calls. Await `jsonSchemaValidatorReady()` first if your code calls them right after startup.
