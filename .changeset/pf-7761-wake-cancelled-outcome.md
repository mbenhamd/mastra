---
'@mastra/core': patch
---

When a parent cancels its delegated task with reason `delegation_timeout`, the task settles `cancelled` and the parent's settlement wake now runs and records the `cancelled` outcome. Before, the wake was refused. Fixes a typecheck failure on main. `@mastra/core/storage` now exports `HARNESS_DELEGATED_SETTLEMENT_OUTCOMES`, `HarnessDelegatedSettlementOutcome` and `isHarnessDelegatedSettlementOutcome`.
