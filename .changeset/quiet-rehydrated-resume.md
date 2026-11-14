---
'@mastra/core': patch
'@mastra/server': patch
---

Fixed Harness approvals, suspensions and questions for modes that replace the agent's tools: they can now be answered after the session was closed or the process restarted. The reopened session resumes against the tools the mode registers, limited to the tool names the run was admitted with. A run that was admitted with tools passed only for that call (`additionalTools`), including one that shares a registered tool's name, or whose mode no longer registers one of its tools, still fails closed instead of resuming with a different tool surface. A run that suspends again after any resume (in the same process or after a restart) does not yet keep its admitted tool names; that is tracked separately (PF-8974). [PF-8961]
