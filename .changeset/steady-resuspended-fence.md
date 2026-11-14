---
'@mastra/core': patch
---

Harness replacement-mode runs keep their tool fence when they suspend again after a resume. A warm or cold resume is capped to the tools the run was admitted with, and a re-suspension records the same fence and per-call tool names. A later resume after a restart is therefore still refused or capped, instead of running with the mode's current tools.
