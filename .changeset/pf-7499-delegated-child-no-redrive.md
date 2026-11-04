---
'@mastra/core': patch
---

Fixed Harness restart recovery re-running a delegated subagent that was killed mid-run. When a process restarts and finds a `task_delegate` child whose work had already been sent to the model by the process that died, the child is now interrupted instead of run again: the plan task fails with `harness.run_interrupted`, the child is closed, and the parent is free for its next turn right away. A delegation that never started still runs once, and one waiting for approval still waits.
