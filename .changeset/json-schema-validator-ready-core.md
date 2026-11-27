---
'@mastra/core': patch
---

Where code generation from strings is disallowed, so that JSON Schemas are validated by the interpreter in `@mastra/schema-compat`, asynchronous validation now waits for that validator to load. A call right after startup is then not refused as `JSON Schema validator unavailable`. This covers tool execution (input, output, suspend, resume and request context), the AI SDK tool-input hook, tool approval preflight and edited-argument checks, workflow input, resume, suspend, state and request context, structured output, terminal tool results, agent and durable-agent request context, agent tool resume data and adopted tool output, and agent-controller session state. These paths wait only while a validator is loading, including a copy loaded later by a package imported on demand; after that, validation keeps its earlier timing, and the AI SDK tool-input hook stays synchronous. AI SDK v4 does not await that hook, so during the startup window, and while a later copy loads, AI SDK v4 refuses calls to tools whose input schema the hook validates, reporting invalid arguments even when the input is valid. `jsonSchemaValidatorReady` and `jsonSchemaValidatorLoading` are re-exported from `@mastra/core/schema`. Synchronous callers can still see the startup window: the `validateToolInput` and `validateToolOutput` helpers from `@mastra/core/tools` and direct synchronous `validate` calls. If your code calls them right after startup, await `jsonSchemaValidatorReady()` first while `jsonSchemaValidatorLoading()` is true.

In the same runtimes, a tool whose output schema the interpreter refuses is not run. Its call returns a validation error, `Tool output schema is refused for <tool>, so the tool was not run`, before the tool performs any action. This applies to Mastra tools and AI SDK tools, whatever the output validation mode. Elsewhere, output schemas are validated after the tool runs, as before.

```ts
import { jsonSchemaValidatorLoading, jsonSchemaValidatorReady } from '@mastra/core/schema';
import { validateToolInput } from '@mastra/core/tools';

if (jsonSchemaValidatorLoading()) await jsonSchemaValidatorReady();
const { data, error } = validateToolInput(tool.inputSchema, input, tool.id);
```
