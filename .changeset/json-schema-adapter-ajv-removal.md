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

Code that called `getAjv()` to add keywords or formats should express the rule with a keyword the adapter checks, such as `pattern`, `enum` or `minimum`, or validate the value separately. Do not move a custom format to the `format` keyword. The adapter does not check `format` on subschemas reached through schema keywords such as `properties` or `$defs`. On a subschema reached only by a `$ref` to another location, such as `#/components/schemas/User`, it checks the formats the validator knows, such as `email`, and ignores other names, so a custom format is not checked there either.
