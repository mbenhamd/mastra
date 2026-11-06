---
'@mastra/core': patch
---

A parent session is now woken when it cancels a delegated task with reason `delegation_timeout`. The wake records the task's `cancelled` outcome; before this, the wake was refused. `HarnessWakeTerminalPayload.outcome` now accepts `'cancelled'`.

To handle every settlement outcome in a wake payload, check it with the new guard from `@mastra/core/storage`:

```ts
import { isHarnessDelegatedSettlementOutcome } from '@mastra/core/storage';

if (isHarnessDelegatedSettlementOutcome(payload.outcome) && payload.outcome === 'cancelled') {
  // the parent cancelled this delegated task; decide whether to retry it
}
```

`HARNESS_DELEGATED_SETTLEMENT_OUTCOMES` lists all four outcomes, and `HarnessDelegatedSettlementOutcome` is their type.
