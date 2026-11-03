---
'@mastra/core': patch
---

Fixed structured output on Anthropic models returning empty records or being rejected for complex schemas.

- Anthropic's native output format is strict: free-form objects such as `z.record()` were sent as objects that can hold no keys, so the model always returned `{}` for them. Schemas with a free-form object (including a JSON Schema object with no `properties` and no `additionalProperties`), more than 24 optional parameters or more than 16 union-typed parameters are now put into the prompt instead (as `jsonPromptInjection: 'inline'` does), and the reply is validated against the schema as before. The decision is made once on the schema as you wrote it, before Mastra's schema normalization or provider conversion closes any object, and it is recorded with the run so durable agents (including resumed steps) and input processors that replace the schema apply it consistently, and when Anthropic gets the schema through the prompt (this fallback or `jsonPromptInjection: 'auto'`) the prompt carries that schema, with records left open. Other schemas still use the native output format; `jsonPromptInjection: false`, or `providerOptions.anthropic.structuredOutputMode` set to `'outputFormat'` or `'jsonTool'` (which also wins over `jsonPromptInjection: 'auto'`), keeps the native format.
- Harness `session.message({ output, sync: true })` accepts `jsonPromptInjection` and passes it to the agent's structured output.
