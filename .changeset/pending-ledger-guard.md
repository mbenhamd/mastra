---
'@mastra/core': patch
---

Improved regression coverage for harness session recovery. Tests verify that a recovered run's token usage is counted once and that later saves remove a settled run's usage-tracking entry. There is no change to the public API or to runtime behavior.
