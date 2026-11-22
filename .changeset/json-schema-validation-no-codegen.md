---
'@mastra/schema-compat': patch
---

Fixed JSON Schema tool inputs failing validation in runtimes started with `--disallow-code-generation-from-strings`. The JSON Schema adapter now validates with `@cfworker/json-schema`, which never generates code, instead of Ajv, which compiles each schema with `new Function`. Validation behaves the same: OpenAPI `nullable: true` still allows `null`, and `format` is still not checked. Error messages now use the new validator's wording, for example `5000 is greater than 500.`.

A schema that uses `$dynamicRef` anywhere, including where only a `$ref` or `dependencies` reaches it, is now refused: every value gets one issue saying `$dynamicRef is not supported`. The new validator cannot resolve `$dynamicRef`, and refusing the schema is safer than letting such references pass unchecked. `$ref` is unaffected. `$recursiveRef` is now resolved in every draft; Ajv resolved it only for some draft and anchor combinations.

The validator is imported lazily when the package loads, because `@cfworker/json-schema` reads `self.location` on import and throws in a browser worker created from a blob URL. Importing the package therefore never fails there. Until the validator has loaded, which happens shortly after startup, and permanently where it cannot load (such as those blob workers), every JSON Schema validation fails closed with the issue `JSON Schema validator unavailable (<reason>)`; it never reports success. During that window a schema with `$dynamicRef` gets this issue too, not the `$dynamicRef` one. The new `jsonSchemaValidatorReady()` resolves once the import has settled, in every copy of the adapter loaded in the process, including copies bundled into other packages. Await it before a synchronous `validate` call made right after startup.

The `ajvOptions` option and the `getAjv()` method are removed, and Ajv customization has no replacement. Drop the option:

```ts
// Before
const schema = toStandardSchema(jsonSchema, { ajvOptions: { strict: false } });
// After
const schema = toStandardSchema(jsonSchema);
```

Code that called `getAjv()` to add keywords or formats should express the rule in the JSON Schema itself, or validate the value separately.
