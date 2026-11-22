---
'@mastra/schema-compat': patch
'@mastra/core': patch
'@mastra/mcp': patch
'@mastra/memory': patch
'@mastra/claude': patch
'@mastra/openai': patch
'@mastra/factory': patch
---

Fixed JSON Schema tool inputs failing validation in runtimes started with `--disallow-code-generation-from-strings`. The JSON Schema adapter now validates with `@cfworker/json-schema`, which never generates code, instead of Ajv, which compiles each schema with `new Function`. Validation behaves the same: OpenAPI `nullable: true` still allows `null`, and `format` is still not checked. Error messages now use the new validator's wording, for example `5000 is greater than 500.`.

A schema that uses `$dynamicRef` anywhere, including where only a `$ref` or `dependencies` reaches it, is now refused: every value gets one issue saying `$dynamicRef is not supported`. The new validator cannot resolve `$dynamicRef`, and refusing the schema is safer than letting such references pass unchecked. `$ref` is unaffected. `$recursiveRef` is now resolved in every draft; Ajv resolved it only for some draft and anchor combinations.

The validator is imported lazily when the package loads, because `@cfworker/json-schema` reads `self.location` on import and throws in a browser worker created from a blob URL. Importing the package therefore never fails there. Until the validator has loaded, which happens shortly after startup, and permanently where it cannot load (such as those blob workers), every JSON Schema validation fails closed with the issue `JSON Schema validator unavailable (<reason>)`; it never reports success. During that window a schema with `$dynamicRef` gets this issue too, not the `$dynamicRef` one. The new `jsonSchemaValidatorReady()` (exported from `@mastra/schema-compat` and `@mastra/core/schema`) resolves once the import has settled. Mastra's asynchronous validation paths await it before validating: tool execution (input, output, suspend, resume and request context), the AI SDK tool-input hook, tool approval preflight and edited-argument checks, workflow input, resume, suspend, state and request context, structured output (including the Claude and OpenAI agent SDKs), terminal tool results, agent and durable-agent request context, agent tool resume data and adopted tool output, agent-controller session state, MCP server events and resume data, working-memory extraction, and Factory sandbox settings. `@mastra/mcp`, `@mastra/claude` and `@mastra/openai` now depend on `@mastra/schema-compat` directly for it. Synchronous APIs can still see the window: a direct synchronous `validate` call and the `validateToolInput`, `validateToolOutput`, `validateToolSuspendData` and `validateRequestContext` helpers from `@mastra/core/tools`. Await `jsonSchemaValidatorReady()` first if your own code calls them right after startup.

The `ajvOptions` option and the `getAjv()` method are removed, and Ajv customization has no replacement. Drop the option:

```ts
// Before
const schema = toStandardSchema(jsonSchema, { ajvOptions: { strict: false } });
// After
const schema = toStandardSchema(jsonSchema);
```

Code that called `getAjv()` to add keywords or formats should express the rule in the JSON Schema itself, or validate the value separately.
