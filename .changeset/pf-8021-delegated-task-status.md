---
'@mastra/core': patch
---

A delegated plan task now keeps its status until its delegation settles. Before, retrying a subtree that already held a failed child stored the delegated task as `failed`, so restart recovery skipped it and the subtree stayed locked. A reopened dependency also no longer changes a delegated task's status while its subagent runs.
