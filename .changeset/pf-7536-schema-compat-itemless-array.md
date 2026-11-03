---
'@mastra/schema-compat': patch
---

Fixed a crash when validating structured output or tool input against a JSON schema array with no `items` (for example `{ "type": "array" }`). Validation threw `Cannot read properties of undefined (reading 'anyOf')` for any non-empty array. An array schema without `items` now accepts any items. Arrays that declare `items` validate as before. The fix applies to the Anthropic, DeepSeek, Google, Meta and OpenAI compat layers.
