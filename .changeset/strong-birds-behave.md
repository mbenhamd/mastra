---
'@mastra/core': patch
---

Fixed agent stream responses losing the response id or timestamp when a provider, such as OpenRouter, sends response metadata in several chunks. The step and final `response` objects also include `headers` again (as `undefined` when the provider sent none).
