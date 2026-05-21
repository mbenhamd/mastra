---
"@mastra/core": patch
---

Added runtime compatibility protection for Harness recovery. When you update agent implementations, prompts, tools, model bindings, MCP bindings, or other runtime dependencies, queued or suspended work from an older generation will be rejected during recovery instead of running against mismatched runtime state.

```ts
const harness = new Harness({
  runtimeCompatibilityGeneration: "agents-2026-05-21",
  // ...other Harness config
});
```

Change the `runtimeCompatibilityGeneration` value whenever existing non-terminal queued or suspended work should not resume on the new runtime surface.
