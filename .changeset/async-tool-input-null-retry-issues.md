---
'@mastra/core': patch
---

Fixed agent tool-input refusals naming optional fields that were sent as `null`. When a model without the OpenAI compat layer (Anthropic, Gemini, Grok) called a tool with one real input fault plus explicit nulls on optional fields, the error listed the nulls as faults too. Agent tool calls now report the same errors as `validateToolInput`: only the faults left after the null retry. The stringified-JSON coercion and the null retry now also build on each other, as in `validateToolInput`.
