---
'@mastra/core': major
---

- Added `listSuspendedRuns()` for discovering and resuming suspended standard or durable agent runs after a process restart.
- Durable recovery now validates the persisted run owner and runtime version before continuing.
- Prepared and recovered runtime bindings now compare regular-expression source, flags, execution position, and custom properties without accepting identity-changing subclasses.
- Built-in `Run.startAsync()` results now expose an optional `execution` promise for observing background execution failures.

**Breaking change:** `DurableAgent.prepare()` no longer returns `registryEntry`.

- Before: read `registryEntry` from the prepared result.
- After: pass the returned `runId` to `stream()` with the same messages and options. The result still exposes the serializable `workflowInput`; call `cleanup()` if the prepared handoff is abandoned.

```ts
const messages = 'Recover this task'
const prepared = await durableAgent.prepare(messages)

try {
  const stream = await durableAgent.stream(messages, { runId: prepared.runId })
  try {
    await stream.output.consumeStream()
  } finally {
    stream.cleanup()
  }
} catch (error) {
  prepared.cleanup()
  throw error
}
```
