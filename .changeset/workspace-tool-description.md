---
'@mastra/core': minor
---

Workspace tools accept a `description` option that replaces the description the model sees. Use it when your sandbox changes what a tool acts on, for example computer tools that drive the user's own screen instead of a sandbox desktop.

```ts
new Workspace({
  sandbox,
  tools: {
    mastra_workspace_computer_click: {
      description: "Left-click on the user's screen. The user approves every click.",
    },
  },
});
```
