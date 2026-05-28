### 11.3 Cutover timeline

- **Before the breaking release** — implementation work may happen on branches
  and prerelease builds, but the shipped public contract is not split into
  compatibility subpaths.
- **Breaking release boundary** — `@mastra/core/harness` becomes the v1
  `Harness` / `Session` contract. Removed methods are removed, changed behavior
  is documented as changed behavior, and callers migrate deliberately.
- **After cutover** — no `/v1` compatibility alias, no legacy production export,
  and no hidden fallback flags. If an old shape still appears at runtime, the
  owning boundary rejects it or requires an explicit import path.

In short: break once, cleanly, at the version boundary that allows it.
