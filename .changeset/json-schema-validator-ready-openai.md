---
'@mastra/openai': patch
---

Structured output validation now waits for the JSON Schema validator to load where code generation from strings is disallowed, so a result right after startup is not refused as `JSON Schema validator unavailable`. Adds `@mastra/schema-compat` as a dependency.
