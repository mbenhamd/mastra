---
'@mastra/core': patch
---

Fixed structured output on Anthropic models returning empty records or being rejected for complex schemas.

- Anthropic's native output format is strict: every object is sent closed, so open objects such as `z.record()` could only come back as `{}`, and schemas over Anthropic's limits were rejected. Anthropic now uses the strict format only when it provably carries your schema: every object is closed (a plain `z.object()`, `z.strictObject()`, or a JSON Schema object with `additionalProperties: false`), the root is an object, every array has an `items` schema, there are no tuples, intersections, `$ref`s or other open shapes, the schema comes from Zod or is plain JSON Schema, and the request stays within Anthropic's limits of 24 optional and 16 union-typed parameters, counted across the response schema and every `strict: true` tool in the request. Any other schema, including `z.record()`, `.passthrough()`, `.catchall()`, `z.looseObject()` and JSON Schema objects without `additionalProperties: false`, is put into the prompt instead (as `jsonPromptInjection: 'inline'` does), with its records left open, and the reply is validated against the schema as before.
- The decision is made once on the schema as you wrote it and recorded with the run, so durable agents (including resumed and recovered runs and `structuredOutput.model` structuring passes) and input processors that replace the schema apply it consistently. Durable runs also persist the schema with your objects as open as you wrote them.
- `jsonPromptInjection: false`, or `providerOptions.anthropic.structuredOutputMode` set to `'outputFormat'` or `'jsonTool'` (which also wins over `jsonPromptInjection: 'auto'`), keeps the native format.
- Harness `session.message({ output, sync: true })` accepts `jsonPromptInjection` and passes it to the agent's structured output.
