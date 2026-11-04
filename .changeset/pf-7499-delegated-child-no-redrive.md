---
'@mastra/core': patch
---

Fixed Harness restart recovery re-running a delegated subagent that was killed mid-run. When a process restarts and finds a `task_delegate` child whose run the process that died had already started dispatching, the child is now interrupted instead of run again: the plan task fails with `harness.run_interrupted`, the child is closed, and the parent is no longer held busy by a second run of that work. A delegation that never started still runs once, and one waiting for approval still waits.
