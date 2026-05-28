## 11. Migration from current Harness

**Entity ownership:** all entities; cutover order in §11.1 and
[§0 Mental model](../00-mental-model.md). Duplicate traps: §11.6e.

Harness v1 is a clean replacement contract, not a compatibility layer. The
session-first model changes public behavior and must ship at an honest breaking
boundary. The implementation should replace the current Harness surface with the
v1 `Harness` / `Session` API rather than keep two production implementations,
aliases, hidden fallbacks, or adapter shims alive.

```ts
import { Harness } from '@mastra/core/harness';
```

Migration guidance in this section is for humans and implementation planning
only. It maps old concepts to the new session-first contract so code can be
rewritten deliberately. It does not define runtime aliases, compatibility
subpaths, automatic imports, or fallback behavior.

The cutover rules are:

- **One public Harness contract.** `@mastra/core/harness` exports the v1
  contract at the breaking release boundary. There is no supported `/v1` alias
  after cutover and no old `Harness` class kept as a production entry point.
- **Session-first lifecycle.** Create/open, close, delete, rename, clone,
  settings, operations, and inbox responses are normal `Session` concerns.
  Partial-history fork is deferred from v1 instead of preserved as an alias.
  There is no app-facing `harness.threads.*` lifecycle model.
- **Explicit data import only.** Existing thread/message data can be imported or
  read only through explicit owner-defined rules. It must not silently hydrate v1
  runtime state from legacy metadata, process-local state, live callbacks, or
  best-effort memory rows.
- **No hidden help paths.** Unsupported old shapes fail loudly at the boundary
  that observes them. The implementation must not auto-fix, silently coerce, or
  keep compatibility behavior behind flags.
