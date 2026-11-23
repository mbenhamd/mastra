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

Code that called `getAjv()` to add keywords or formats should express the rule with a keyword the adapter checks, such as `pattern`, `enum` or `minimum`, or validate the value separately. Do not move a custom format to the `format` keyword: the adapter never checks `format`, on any subschema.
