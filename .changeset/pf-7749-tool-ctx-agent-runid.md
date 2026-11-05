---
'@mastra/core': patch
---

Tools run by an agent now receive the run id as `ctx.agent.runId`.

`AgentToolExecutionContext` already declared `runId`, but the agent tool builder left it unset, so the value was only available as the top-level `ctx.runId`. Inside a Harness, `ask_user` and `submit_plan` read `ctx.agent.runId` to register their pending question or plan approval before suspending, and `task_delegate` reads it to record the parent run of a delegated subagent. Those registrations now happen, with the same run id the Harness reports on `agent_start`.
