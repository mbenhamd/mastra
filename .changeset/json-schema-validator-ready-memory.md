---
'@mastra/memory': patch
---

Working-memory extraction now waits for the JSON Schema validator to load where code generation from strings is disallowed before validating, so an extraction right after startup is not refused as `JSON Schema validator unavailable`.
