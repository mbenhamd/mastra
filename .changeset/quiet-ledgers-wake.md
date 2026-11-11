---
'@mastra/core': patch
---

Moved the harness session's terminal staging state (which runs have staged output, counted usage, or a native admission) and its settlement wake admissions into their own internal modules. There is no change to the public API or to runtime behavior.
