### 12.7 Permissions

```ts
// Grant a tool for this session only. Tool names are the model-visible /
// Harness-exposed tool IDs after any product remapping. MastraCode, for example,
// exposes the command tool as `execute_command`, not the core workspace
// constant name.
await session.permissions.grantTool({ toolName: 'execute_command' });

// Revoke a previous grant.
await session.permissions.revokeTool({ toolName: 'execute_command' });

// Set a category-level policy.
await session.permissions.setPolicy({
  category: 'destructive',
  policy: 'ask',
});

// Inspect what's currently granted.
const grants = session.permissions.getGrants();
```
