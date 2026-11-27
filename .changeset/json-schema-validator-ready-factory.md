---
'@mastra/factory': patch
---

Sandbox settings validation now waits for the JSON Schema validator to load where code generation from strings is disallowed, so a settings update right after startup is not refused as `JSON Schema validator unavailable`.
