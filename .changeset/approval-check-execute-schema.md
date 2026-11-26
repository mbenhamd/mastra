---
'@mastra/core': patch
---

Fixed approval-gated tool calls that failed input validation after the user approved them. On models with a schema compat layer (for example Anthropic and Gemini), a call whose input would fail is now refused before the user is asked to approve it.
