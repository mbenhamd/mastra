---
'@mastra/core': patch
---

Fixed `signal()` results that never settled after a crash. A question, plan approval or sandbox-access request that a tool registers during a `signal()` turn now records that signal when it is registered. Before, the signal was added only when the run's suspension was captured, so a process that died between the two left a pending interaction whose answer did not settle the signal.
