---
'@mastra/core': minor
---

Added authenticated terminal recovery envelopes for continuous workflows. Supported workflow stores now atomically admit nested runs, retain exact terminal results and final state, bind recursive graph ancestry, and preserve one child run per foreach iteration.

Check the recovery capability before using the new contract:

```ts
const capabilities = workflowsStorage.getWorkflowTerminalizationCapabilities()
if (capabilities.recoveryVersion !== 1) {
  throw new Error('Terminal recovery envelopes are not supported')
}
```

Terminal-state persistence now requires the recovery payload that storage authenticates and returns for dispatch:

```ts
// Before this unreleased terminalization contract extension
await workflowsStorage.persistWorkflowTerminalState({ ...fence, snapshot })
const { snapshot: retained } = await workflowsStorage.getWorkflowTerminalEffectForDispatch(input)

// After
await workflowsStorage.persistWorkflowTerminalState({
  ...fence,
  snapshot,
  recoveryEnvelope,
})
const { recovery } = await workflowsStorage.getWorkflowTerminalEffectForDispatch(input)
```

The recovery envelope is canonical, bounded data. It rejects accessors, proxies, executable values, malformed Unicode, and the framework authentication token instead of retaining provider or runtime objects.
