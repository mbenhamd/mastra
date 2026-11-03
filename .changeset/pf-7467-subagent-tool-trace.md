---
'@mastra/core': minor
---

Improved what a parent agent learns from Harness subagents:

- A subagent result now includes `toolTrace`: the tools the child called, with call and error counts per tool, for example `search_papers` called twice with one failure. The parent can see what the child already covered and what failed, without searching again. Raw tool inputs and outputs are still never copied into the result, and lifecycle events leave the trace out to keep their small size.
- `task_delegate` now lists each durable subagent type with its description, as `spawn_subagent` already does, so the parent picks a type by what it does rather than by its id alone. Its `task` field also says that the subagent does not see the parent conversation.
