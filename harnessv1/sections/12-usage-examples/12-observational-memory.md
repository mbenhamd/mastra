### 12.12 Observational Memory

```ts
// Configure OM on the Agent Memory instance, not on HarnessConfig or Session.
const memory = new Memory({
  storage: memoryStorage,
  options: {
    observationalMemory: {
      model: 'anthropic/claude-haiku-4-5',
    },
  },
});

const agent = new Agent({
  id: 'assistant',
  name: 'assistant',
  instructions: 'Help the user.',
  model: 'openai/gpt-4o-mini',
  memory,
});

const harness = new Harness({
  agents: { assistant: agent },
  modes: [{ id: 'default', agentId: 'assistant' }],
  defaultModeId: 'default',
  // Omit observationalMemory here. Enabled Harness OM currently rejects.
});

// Recovery only: old Harness builds could persist an override they could not
// honor. This call is idempotent and wakes queued work after its CAS commits.
const recoveredSession = await harness.session({ sessionId, resourceId: 'user-123' });
await recoveredSession.om.clearOverride();
```

`session.om.switchObserverModel(...)` and
`session.om.switchReflectorModel(...)` currently validate and reject without
persisting. Rebuild or reconfigure the Agent Memory instance to change OM
models.
