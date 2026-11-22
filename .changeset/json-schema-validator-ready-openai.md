---
'@mastra/openai': patch
---

Structured output validation now waits for the JSON Schema validator to load, so a result right after startup is not refused as `JSON Schema validator unavailable`. Adds `@mastra/schema-compat` as a dependency.
