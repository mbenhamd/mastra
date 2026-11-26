---
'@mastra/schema-compat': major
---

Removed Ajv from the JSON Schema adapter, which now validates with `@cfworker/json-schema`. The `ajvOptions` option and the `getAjv()` method are removed, and Ajv customization has no replacement. Drop the option:

```ts
// Before
const schema = toStandardSchema(jsonSchema, { ajvOptions: { strict: false } });
// After
const schema = toStandardSchema(jsonSchema);
```

Ajv options that changed the value, such as `coerceTypes`, `useDefaults` and `removeAdditional`, are gone too: the validated value is now returned exactly as passed. Convert strings to numbers, fill defaults or strip extra properties before calling `validate`, for example with a Zod schema's `z.coerce` and `.default()`, or by hand.

Code that called `getAjv()` to add keywords or formats should express the rule with a keyword the adapter checks, such as `pattern`, `enum` or `minimum`, or validate the value separately. Do not move a custom format to the `format` keyword: the adapter never checks `format`, on any subschema.

The adapter now accepts only a fixed list of keywords for each dialect and refuses any other schema when it compiles it. A schema without `$schema`, or with draft-07's, is draft-07. A schema with `https://json-schema.org/draft/2020-12/schema` is 2020-12, as `MCPClient` marks every tool schema that names no dialect. The validation result then carries one issue, `Schema validation error: <keyword> at "<pointer>" <reason>, so the schema is refused`. The issue names the keyword and its JSON Pointer, but never the schema's values or the tool's arguments. The list is: `type`, `properties`, `required`, `items`, `additionalProperties`, `propertyNames`, `allOf`, `anyOf`, `oneOf`, `enum`, `const`, the numeric bounds, `multipleOf` (positive integers only), the length, item and property counts, `pattern`, `uniqueItems`, `nullable`, `format` (not checked), `$ref`, `$defs`, `definitions`, the annotations `title`, `description`, `default`, `examples` and `example`, plus `discriminator`, Composio's `human_parameter_*` and `file_*` keys, and any key starting with `x-`. A 2020-12 schema may also use `prefixItems`, `unevaluatedProperties`, `unevaluatedItems`, `contains`, `minContains`, `maxContains`, `dependentSchemas`, `dependentRequired`, `$comment`, and `$id` at the root as an absolute URI. A keyword stays on a dialect's list only if this validator and fork main's Ajv for that dialect gave the same answer in a comparison run. A value that JSON cannot carry (a non-finite number, a `Date` or another non-plain object, a sparse array, a cycle) is refused before validation.

The fixed list refuses these schemas, which fork main's Ajv (`new Ajv({ allErrors: true, strict: false })`, or `Ajv2020` for a `2020-12` `$schema`, in `json-schema.ts:209-217`) accepted and validated:

- Refused here, accepted by fork main: a `$schema` other than draft-07 or 2020-12, such as 2019-09, and an `$id` below the root or in a draft-07 schema.
- Refused here, accepted by fork main: `if`/`then`/`else`, `not`, `patternProperties`, `dependencies`, `additionalItems`, `id`, `$anchor`, `$dynamic*` and `$recursive*`, and any other key not on the list, plus the 2020-12 keywords above in a draft-07 schema. Fork main applied or ignored these under `strict: false`. A draft-07 Composio output schema with a tuple-style `prefixItems` (`GITHUB_GET_THE_HOURLY_COMMIT_COUNT_FOR_EACH_DAY`) is refused for this reason; fork main's draft-07 Ajv ignored the keyword.
- Refused here, accepted by fork main: a `$ref` other than `#` or a member of the root's `$defs` or `definitions`.
- Refused here, accepted by fork main: a decimal `multipleOf`, a boolean `exclusiveMinimum` or `exclusiveMaximum`, and other listed keywords holding a value of the wrong type.
- Refused here, accepted by fork main: a property, `required`, `dependentSchemas` or `dependentRequired` name, or a `const`/`enum` key, named `__proto__`, `constructor`, `toString` or `valueOf`.
- Refused here, accepted by fork main: a value that JSON cannot carry.
- Refused here, while fork main overflowed its stack or recursed: a loop of `$ref`, `allOf`, `anyOf`, `oneOf` or `dependentSchemas` that reads the same schema at the same value.

PapersFlow import facts, checked on PapersFlow origin/main 735f0f2b49. No PapersFlow source imports `@mastra/mcp`. One test, `apps/doxa-runtime/tests/mcpSdk.integration.test.ts`, imports the upstream MCP SDK's `Client` and `StreamableHTTPClientTransport`. That is not `MCPClient` and never reaches this adapter. PapersFlow's run-scoped Parallel MCP surface (`apps/doxa-runtime/src/parallel/mcpRunScopedParallel.ts`) only serves tool schemas, so it adds no schema source.
