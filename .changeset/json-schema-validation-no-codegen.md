---
'@mastra/schema-compat': patch
---

Fixed JSON Schema validation in runtimes that disallow code generation from strings. Examples are Node or Bun started with `--disallow-code-generation-from-strings`, and a page whose Content Security Policy lacks `unsafe-eval`. There, the JSON Schema adapter's Ajv cannot compile a schema, so every JSON Schema tool input failed with `Schema validation error: Code generation from strings disallowed for this context`.

**Where code generation is allowed, nothing changes.** The adapter checks once per process whether it can generate code. Where it can, it validates with Ajv exactly as before: the same options, `ajvOptions`, `getAjv()`, results and messages. The new validator is never loaded there.

**Where code generation is disallowed, the adapter uses an interpreter.** This is `@cfworker/json-schema` 4.1.6 with a local patch, bundled into this package's build. This path accepts nothing that Ajv, run where it can compile, would refuse; where the two validators would differ, it refuses. For plain-JSON schemas and values, a seeded comparison test in this package (`json-schema-interpreter-parity.test.ts`) checks this against Ajv, for draft-07 and for 2020-12.

In this path:

- **The schema and each value are first copied once into plain JSON.** Anything JSON cannot carry is refused, with its JSON Pointer and a named reason:
  - a `Proxy`, where the runtime can detect one (Node and Bun can, through `util.types.isProxy`);
  - a getter or setter, or a symbol or non-enumerable key;
  - an object whose prototype is not `Object.prototype`, such as a `Date`, a class instance or an object with a `null` prototype;
  - an array subclass or a sparse array;
  - a cycle;
  - a function, a `bigint`, a symbol or a non-finite number.

  A schema is also refused when it reaches one object through two references, when one of its objects is frozen, sealed or not extensible, or when one of its members is read-only. An object member whose value is `undefined` is kept. In a schema, it counts as an absent keyword, as Ajv read it. In a value, it is handled as Ajv handled it. A schema is copied when it is first used, so later changes to the original object do not reach validation.
- **Each dialect accepts only a fixed list of keywords.** A schema using any other keyword is refused with the issue `Schema validation error: <keyword> at "<pointer>" <reason>, so the schema is refused`. The list:
  - `type`, `properties`, `required`, `items`, `additionalProperties`, `propertyNames`, `allOf`, `anyOf`, `oneOf`, `enum`, `const`, the numeric bounds, the length, item and property counts, `pattern`, `nullable`, `format` (never checked; Ajv here did not check it either), `$defs` and `definitions`, and `$ref` to `#` or to a member of the root's `$defs` or `definitions`;
  - the annotations `title`, `description`, `default`, `examples`, `example` and `discriminator`, Composio's `human_parameter_name`, `human_parameter_description`, `file_uploadable` and `file_downloadable`, and any key that starts with `x-`;
  - in draft-07 only, `additionalItems` and a tuple `items`;
  - in 2020-12 only, `prefixItems`, `dependentRequired`, `$comment`, and a root `$id` in canonical `http` or `https` form: a lower-case host, an optional port, and path segments made of letters, digits, `.`, `_`, `~` and `-`, with no query, fragment or percent-encoding.

  A schema with no `$schema`, or with draft-07's, is draft-07. A schema with `https://json-schema.org/draft/2020-12/schema` is 2020-12. Refused because they are not on the list: `not`, `if`/`then`/`else`, `multipleOf`, `uniqueItems`, `contains`, `minContains`, `maxContains`, `patternProperties`, `dependencies`, `dependentSchemas`, `unevaluatedProperties`, `unevaluatedItems`, `$anchor`, `$dynamic*` and `$recursive*`. Also refused:
  - an `$id`, `$anchor` or `$dynamicAnchor` below the root;
  - a property name or `required` name that `Object.prototype` holds;
  - a loop through `$ref`, `allOf`, `anyOf` or `oneOf` that reads the same schema at the same value;
  - a schema that Ajv refused or threw on while compiling, such as a listed keyword holding a value of the wrong type.

  A corpus of 1,954 real Composio tool definitions, with 3,908 input and output schemas, uses no keyword outside the list.
- **Under `const` or `enum`, more values are refused.** When the schema uses `const` or `enum`, a value that holds an own key `constructor`, `toString`, `valueOf` or `__proto__` anywhere is refused. Ajv's equality check read those names from the prototype, and this validator's does not.
- **A schema given `ajvOptions` is refused**, because those options need Ajv.
- **Messages are the interpreter's own**, for example `5000 is greater than 500.` Issue paths mostly match Ajv's. They differ for `propertyNames`, for a tuple with too many items, and for `additionalProperties` set to a schema.
- **The new `JSON_SCHEMA_REFUSAL` symbol names a method on each wrapped schema.** In this path, the method returns why the schema is refused, or `undefined`. Where Ajv validates, it always returns `undefined`.
- **Large values take longer to check**, because the schema is interpreted rather than compiled. A valid 34 MB value with 800,000 rows took about 1.5 to 8 seconds; Ajv took about 25 to 350 ms. A value nested about 200 levels deep under a recursive schema ends in `Maximum call stack size exceeded`, and the value is refused.

The patch to `@cfworker/json-schema` is `patches/@cfworker__json-schema@4.1.6.patch`, listed under `patchedDependencies` in `pnpm-workspace.yaml`. It changes validation as follows:

- An object property whose value is `undefined` counts as absent for `required`, `dependentRequired` and `properties`. It counts as present for `additionalProperties`, `propertyNames` and the property counts, as with Ajv. An `undefined` value matches no `type`.
- `anyOf` stops at the first branch that passes, as Ajv did. `anyOf` and `oneOf` check their branches with short-circuiting, and collect the full errors only when the keyword fails.
- A schema reached through `$ref` that can refer back to itself is checked against each value location at most twice, and the result is then reused. So time grows with the size of the value, not exponentially with its depth.
- A `#/` reference that the library misses is resolved by following its JSON Pointer.
- `format` is never checked.
- Annotations are kept only from subschemas that pass.
- `not` and `if` stop at their first error.
- `examples` is never read as schemas.
- The draft-04 `id` is ignored, and only a string `$id` is read.
- `uniqueItems` is checked in one pass instead of comparing every pair.

Some of these patched paths serve keywords that the list refuses. They stay in the patch so that it matches the library's structure. The patch is pinned to 4.1.6: with a different version, `pnpm install` fails with `ERR_PNPM_UNUSED_PATCH`.

The interpreter is imported lazily, and only where code generation is disallowed. This is because `@cfworker/json-schema` reads `self.location` on import, which throws in a browser worker created from a blob URL. Until the interpreter has loaded, every validation in this path fails closed with `Schema validation error: JSON Schema validator unavailable (<reason>)`. Where it cannot load, every validation fails this way permanently. Validation never reports success while the interpreter is missing.

Two new functions handle this startup window. `jsonSchemaValidatorReady()` resolves once every copy of the adapter in the process has settled its load. `jsonSchemaValidatorLoading()` reports whether a copy is still loading. Where code generation is allowed, nothing loads, and `jsonSchemaValidatorLoading()` is always `false`.

```ts
import { jsonSchemaValidatorLoading, jsonSchemaValidatorReady, toStandardSchema } from '@mastra/schema-compat';

const schema = toStandardSchema({ type: 'object', properties: { id: { type: 'string' } } });
if (jsonSchemaValidatorLoading()) await jsonSchemaValidatorReady();
const result = schema['~standard'].validate({ id: 'a1' });
```
