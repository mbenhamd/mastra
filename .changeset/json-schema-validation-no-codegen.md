---
'@mastra/schema-compat': patch
---

Fixed JSON Schema tool inputs failing validation in runtimes started with `--disallow-code-generation-from-strings`. The JSON Schema adapter now validates with `@cfworker/json-schema`, which never generates code, instead of Ajv, which compiles each schema with `new Function`. Validation behaves the same: OpenAPI `nullable: true` still allows `null`, and `format` is still not checked. Error messages now use the new validator's wording, for example `5000 is greater than 500.`. The `ajvOptions` option and the `getAjv()` method are removed; neither had a caller.
