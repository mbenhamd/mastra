---
'@mastra/schema-compat': patch
---

Fixed JSON Schema tool inputs failing validation in runtimes started with `--disallow-code-generation-from-strings`. The JSON Schema adapter now validates with `@cfworker/json-schema`, which never generates code, instead of Ajv, which compiles each schema with `new Function`. Most schemas validate as before: OpenAPI `nullable: true` still allows `null`, and `format` is still not checked, on subschemas reached through schema keywords such as `properties` or `$defs`. This differs from Ajv on a subschema reached only by a `$ref` to another location, such as `#/components/schemas/User`: there, formats the validator knows (such as `email`) are now checked and `nullable` is not recognised, so a `null` or badly formatted value that Ajv accepted there is now refused. Keywords beside a `$ref` still apply in every draft. Two behaviours change, described below: a schema containing `$dynamicRef` is refused, and `$recursiveRef` now resolves in every draft. Error messages now use the new validator's wording, for example `5000 is greater than 500.`.

A schema with a `$dynamicRef` key anywhere is now refused, including where only a `$ref` or `dependencies` reaches it and inside literal data such as `const`, `enum`, `default` or `examples`: every value gets one issue saying `$dynamicRef is not supported`. The new validator cannot resolve `$dynamicRef`, and refusing the schema is safer than letting such references pass unchecked. `$ref` is unaffected. `$recursiveRef` is now resolved in every draft; Ajv resolved it only for some draft and anchor combinations.

A value is validated in its JSON form: an object property whose value is `undefined` counts as absent for every keyword. Ajv counted it as absent only for `properties`, `required` and `dependencies`, so `{ extra: undefined }` against `additionalProperties: false` was refused and is now accepted.

`uniqueItems` checks an array of strings, numbers, booleans and `null` in one pass wherever the keyword appears, including behind a `$ref`, so its cost grows linearly with the array, as with Ajv. Values compare by JSON equality: `"1"` and `1` differ, `1` and `1.0` are equal. An array that contains an object or an array is still compared pair by pair, so its cost grows with the square of its length.

The one-pass check is a local patch of the third-party `@cfworker/json-schema` 4.1.6 (`patches/@cfworker__json-schema@4.1.6.patch`, listed under `patchedDependencies` in `pnpm-workspace.yaml`), which this package bundles into its build. It puts the one-pass check in front of the library's pairwise `uniqueItems` loop and changes nothing else; results and messages are unchanged. The patch is pinned to 4.1.6: a different version makes `pnpm install` fail with `ERR_PNPM_UNUSED_PATCH` instead of silently dropping it. Drop the patch file and its `patchedDependencies` entry once `@cfworker/json-schema` checks primitive arrays in one pass itself.

The validator is imported lazily when the package loads, because `@cfworker/json-schema` reads `self.location` on import and throws in a browser worker created from a blob URL. Importing the package therefore never fails there. Until the validator has loaded, which happens shortly after startup, and permanently where it cannot load (such as those blob workers), every JSON Schema validation fails closed with the issue `JSON Schema validator unavailable (<reason>)`; it never reports success. During that window a schema with `$dynamicRef` gets this issue too, not the `$dynamicRef` one. The new `jsonSchemaValidatorReady()` resolves once the import has settled, in every copy of the adapter loaded in the process, including copies bundled into other packages. `jsonSchemaValidatorLoading()` reports whether any copy is still loading. Before a synchronous `validate` call made right after startup, await `jsonSchemaValidatorReady()` while `jsonSchemaValidatorLoading()` is true.

```ts
import { jsonSchemaValidatorLoading, jsonSchemaValidatorReady, toStandardSchema } from '@mastra/schema-compat';

const schema = toStandardSchema({ type: 'object', properties: { id: { type: 'string' } } });
if (jsonSchemaValidatorLoading()) await jsonSchemaValidatorReady();
const result = schema['~standard'].validate({ id: 'a1' });
```
