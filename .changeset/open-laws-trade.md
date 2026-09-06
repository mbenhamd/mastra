---
'@mastra/core': patch
---

Reduce workflow storage reads during ordinary agent execution with the direct engine by omitting unconfigured goal and task-completion stages. Preserve supplied configuration, tool-approval resume, and the evented engine's retained graph. [PF-4025]
