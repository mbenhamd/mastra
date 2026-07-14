---
'@mastra/core': minor
---

Added authenticated terminal recovery envelopes for continuous workflows. Supported workflow stores now atomically admit nested runs and initialize an absent durable child snapshot without replacing replayed progress, retain exact terminal results and final state, bind recursive graph ancestry, preserve one child run per foreach iteration, and reject late admission after the parent is terminal.

Check the recovery capability before using the new contract:

```ts
const capabilities = workflowsStorage.getWorkflowTerminalizationCapabilities()
if (capabilities.recoveryVersion !== 1) {
  throw new Error('Terminal recovery envelopes are not supported')
}

const terminalStatus = await workflowsStorage.getWorkflowRunTerminalStatus({
  workflowName,
  runId,
})
```

Adapters that advertise `recoveryVersion: 1` must implement this status lookup. It reports durable terminal evidence even after the replaceable workflow row is gone, so replay can stop before publishing a nested start under a terminal parent.

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

The recovery envelope and the atomic initial child snapshot are canonical, bounded data. They reject accessors, proxies, executable values, custom or inherited serialization hooks, malformed Unicode, and the framework authentication token instead of retaining provider or runtime objects. Atomic admission returns `parent_snapshot_conflict` for malformed or incomplete locked parent state without writing ownership, ancestry, or child state. Terminal persistence returns `invalid_snapshot` for incomplete required run state before advancing the journal or retaining evidence.
