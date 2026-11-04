---
'@mastra/core': patch
---

Fixed Harness restart recovery re-running a delegated subagent that was killed mid-run. When a process restarts and finds a `task_delegate` child whose run the process that died had already started dispatching, the child is now interrupted instead of run again: the plan task fails with `harness.run_interrupted`, the child is closed, and the parent is no longer held busy by a second run of that work. A delegation that never started still runs once, and one waiting for approval still waits. The interrupted run's completion (`agent_end`, `run_completed` and its run summary) is published before the child closes and the task settles, so a failed or crashed publication is retried by the next recovery pass. Storage adapters that do not enforce the session lease on recovery writes (`supportsDispatchRecovery` is false) keep the previous re-attach behaviour instead of writing an unfenced interruption.
