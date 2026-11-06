---
'@mastra/core': patch
---

The built-in `ask_user` and `submit_plan` tools now pre-register their question or plan approval with the harness only for the session's own active run, and only while no other question or approval is waiting. Otherwise the call is parked when the run suspends, as before. This means:

- an agent-as-tool child never registers a question on its parent session;
- a second question after a resume, or two questions in one step, are asked one after the other.

After a restart, a question or plan approval whose run never reached its suspension is released, so it no longer stays parked with no way to answer it.
