---
'@mastra/core': patch
---

Fixed agent tool-input refusals naming optional fields that were sent as `null`. When a model without the OpenAI compat layer (Anthropic, Gemini, Grok) called a tool with one real input fault plus explicit nulls on optional fields, the error listed the nulls as faults too. Agent tool calls now report the same errors as `validateToolInput`: only the faults left after the null retry.

Agent tool calls now also handle fallbacks like `validateToolInput`:

- A `prompt` alias (such as `query`) that comes with a `null` optional field is now accepted. When the alias retry fails, the error lists that retry's faults.
- The stringified-JSON coercion and the null retry build on each other. This applies only where the tool's own Zod schema is validated: models with no compat layer (such as Grok via OpenRouter), tools built with `strict: false` (which get no compat layer on any model), and the approval check. On Anthropic and Gemini, agent tool calls validate a compat schema, which is never coerced.

When a tool's schema answers a call with a promise (for example a Zod schema with an async refinement), the rest of that call's fallbacks run as before this change, so that call accepts nothing it refused before. A fallback input that was already validated is not validated again.

A resumable tool (agent or workflow as a tool) no longer matches the error message to skip a missing `suspendedToolRunId`. When the only fault on a call that is not resuming is that the injected top-level `suspendedToolRunId` is absent, the call is validated again with that field set to `null` and runs with the validated input. A tool whose own schema declares a top-level `suspendedToolRunId`, and a missing `suspendedToolRunId` nested inside the tool's own input, are refused like any other missing field.
