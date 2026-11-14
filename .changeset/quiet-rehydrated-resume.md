---
'@mastra/core': patch
---

Fixed Harness approvals, suspensions and questions for modes that replace the agent's tools: they can now be answered after the session was closed or the process restarted. The reopened session resumes against the tools the mode registers, limited to the tool names the run was admitted with. A run that depended on tools passed only for that call (`additionalTools`), or whose mode no longer registers one of its tools, still fails closed instead of resuming with a different tool surface. [PF-8961](https://linear.app/papersflow/issue/PF-8961)
