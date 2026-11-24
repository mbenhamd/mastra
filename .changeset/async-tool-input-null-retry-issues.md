---
'@mastra/core': patch
---

Fixed agent tool-input refusals naming optional fields that were sent as `null`. When a model without the OpenAI compat layer (Anthropic, Gemini, Grok) called a tool with one real input fault plus explicit nulls on optional fields, the error listed the nulls as faults too. Agent tool calls now report the same errors as `validateToolInput`: only the faults left after the null retry.

Agent tool calls now also handle fallbacks like `validateToolInput`:

- A `prompt` alias (such as `query`) that comes with a `null` optional field is now accepted. When the alias retry fails, the error lists that retry's faults.
- The stringified-JSON coercion and the null retry build on each other. This applies only where the tool's own Zod schema is validated: models with no compat layer (such as Grok via OpenRouter), and the approval check. On Anthropic and Gemini, agent tool calls validate a compat schema, which is never coerced.
