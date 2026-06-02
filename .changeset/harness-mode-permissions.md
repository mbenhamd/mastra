---
'@mastra/core': minor
---

Harness v1 modes can now carry a base permission policy and a workspace tool profile.

A mode already controls the agent, prompt, and base tools. It now also controls:

**`permissions`** — a base permission policy (per-category and per-tool `allow`/`ask`/`deny`) that is seeded onto the session whenever the mode is entered (session create, `switchMode`, or a plan-approval transition). The mode owns the base; runtime `session.permissions.setPolicy()` and grants overlay it until the next mode entry re-establishes it. Modes that omit `permissions` leave the session's existing rules untouched (opt-in).

**`workspaceTools.expose`** — limits which workspace tool categories (`read` / `edit` / `execute`) the mode exposes from its tool surface (`mode.tools` / `additionalTools` / per-call tools), using `HarnessConfig.toolCategoryResolver` to classify tools. `mcp` / `other` / uncategorized tools and the harness built-ins are never filtered.

```ts
new Harness({
  agents: { writer },
  modes: [
    {
      id: 'review',
      agentId: 'writer',
      // read-only review mode: deny edits/exec, only expose read tools
      permissions: { categories: { edit: 'deny', execute: 'deny' }, tools: {} },
      workspaceTools: { expose: ['read'] },
    },
  ],
  defaultModeId: 'review',
});
```

The workspace itself stays owned by the session/resource model (files, sandbox, browser, provider resume state) and is unchanged across mode switches.
