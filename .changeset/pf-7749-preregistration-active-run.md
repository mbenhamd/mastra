---
'@mastra/core': patch
---

`ask_user` and `submit_plan` in a Harness now register their question or plan approval when the tool runs, instead of only when the run pauses.

- They register only for the session's own active run, and only while no other question or approval is waiting. Otherwise the request is recorded when the run pauses, as before.
- A question from an agent used as a tool never appears on the parent session.
- A second question after a resume, or two questions in one step, are asked one after the other.

Fixed questions, plan approvals and sandbox-access requests that could not be answered after a restart. If the process stopped before the run paused for the answer, the Harness now drops the request when it reopens the session. A request that had already started expiring finishes expiring instead.
