---
'@mastra/schema-compat': patch
---

Fixed JSON Schema tool inputs failing validation in runtimes started with `--disallow-code-generation-from-strings`. The JSON Schema adapter now validates with `@cfworker/json-schema`, which never generates code, instead of Ajv, which compiles each schema with `new Function`. Most schemas validate as before: OpenAPI `nullable: true` still allows `null`, and `format` is still not checked, on every subschema, including one reached only by a `$ref` to another location such as `#/components/schemas/User`. Keywords beside a `$ref` still apply in every draft. Two behaviours change, described below: a schema containing `$dynamicRef` is refused, and `$recursiveRef` now resolves in every draft. Other differences from Ajv are listed after them. Error messages now use the new validator's wording, for example `5000 is greater than 500.`, and an extra property under `additionalProperties: false` is reported once as `Property "x" does not match additional properties schema.`.

A schema with a `$dynamicRef` key anywhere is now refused, including where only a `$ref` or `dependencies` reaches it and inside literal data such as `const`, `enum`, `default` or `examples`: every value gets one issue saying `$dynamicRef is not supported`. The new validator cannot resolve `$dynamicRef`, and refusing the schema is safer than letting such references pass unchecked. `$ref` is unaffected. `$recursiveRef` is now resolved in every draft; Ajv resolved it only for some draft and anchor combinations.

Other differences from Ajv:

- `example` and `examples` are ignored, as annotations; objects in them are no longer read as schemas.
- A schema Ajv refused as invalid is now used as written, and a malformed keyword is ignored. For example, `additionalProperties: "false"` (a string), draft-03 `required: true`, `nullable` without `type`, and an `enum` with repeated members no longer refuse every value. Fix such schemas at their source.
- Every schema is validated with 2020-12 keywords, so `prefixItems`, `dependentRequired` and `unevaluated*` apply even without a `$schema`.
- A root value of `undefined` is refused, even against `{}`.
- `multipleOf` compares decimals exactly, so `0.07` is a multiple of `0.01`; Ajv refused it through floating-point error. `NaN`, which JSON cannot carry, is not refused by `minimum` or `maximum`.
- Every `anyOf` branch is evaluated and its errors kept. An `anyOf` whose branch refers back to the same schema at the same value (`{ anyOf: [{ type: 'null' }, { $ref: '#' }] }`), or a failing branch over about 100,000 items, now ends in `Maximum call stack size exceeded`, so the value is refused; it is never accepted.

A value is validated in its JSON form: an object property whose value is `undefined` counts as absent for every keyword. Ajv counted it as absent only for `properties`, `required` and `dependencies`, so `{ extra: undefined }` against `additionalProperties: false` was refused and is now accepted.

`uniqueItems` compares every item that is not an object or an array (strings, numbers, booleans, `null`) in one pass wherever the keyword appears, including behind a `$ref`, so an array of such items costs time in proportion to its length, even when it also holds objects. Values compare by JSON equality: `"1"` and `1` differ, `1` and `1.0` are equal. Only the object and array items are compared pair by pair, so their cost grows with the square of their number, as with Ajv for arrays of objects.

The one-pass check is a local patch of the third-party `@cfworker/json-schema` 4.1.6 (`patches/@cfworker__json-schema@4.1.6.patch`, listed under `patchedDependencies` in `pnpm-workspace.yaml`), which this package bundles into its build. It replaces the library's pairwise `uniqueItems` loop with the one-pass check above and changes nothing else; results and messages are unchanged. The patch is pinned to 4.1.6: a different version makes `pnpm install` fail with `ERR_PNPM_UNUSED_PATCH` instead of silently dropping it. Drop the patch file and its `patchedDependencies` entry once `@cfworker/json-schema` checks primitive arrays in one pass itself.

The validator is imported lazily when the package loads, because `@cfworker/json-schema` reads `self.location` on import and throws in a browser worker created from a blob URL. Importing the package therefore never fails there. Until the validator has loaded, which happens shortly after startup, and permanently where it cannot load (such as those blob workers), every JSON Schema validation fails closed with the issue `JSON Schema validator unavailable (<reason>)`; it never reports success. During that window a schema with `$dynamicRef` gets this issue too, not the `$dynamicRef` one. The new `jsonSchemaValidatorReady()` resolves once the import has settled, in every copy of the adapter loaded in the process, including copies bundled into other packages. `jsonSchemaValidatorLoading()` reports whether any copy is still loading. Before a synchronous `validate` call made right after startup, await `jsonSchemaValidatorReady()` while `jsonSchemaValidatorLoading()` is true. Where the validator cannot load, as in a worker created from a blob URL, validate in a worker started from a script URL or on the main thread instead.

```ts
import { jsonSchemaValidatorLoading, jsonSchemaValidatorReady, toStandardSchema } from '@mastra/schema-compat';

const schema = toStandardSchema({ type: 'object', properties: { id: { type: 'string' } } });
if (jsonSchemaValidatorLoading()) await jsonSchemaValidatorReady();
const result = schema['~standard'].validate({ id: 'a1' });
```
