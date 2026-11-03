---
'@mastra/core': minor
---

Improved what a parent agent learns from Harness subagents:

- A subagent result (`spawn_subagent` output, and a delegated plan task's `delegation.result` in `plan_task_check`) can now include `toolTrace`. It lists the tools the child called, in the order the framework recorded them, with distinct call and error counts per tool:

  ```json
  {
    "toolTrace": [
      { "toolName": "search_papers", "calls": 2, "errors": 1 },
      { "toolName": "read_paper_content", "calls": 3, "errors": 0 }
    ]
  }
  ```

  The parent can see what the child already covered and what failed, without searching again. The trace comes only from the framework's own tool receipts. Raw tool inputs and outputs are never included, and lifecycle events (`subagent_end`) leave it out.

- `toolTraceTruncated: true` means the trace is incomplete. This happens when tools were dropped (more than 24, over the 2 KiB trace budget, or no room left in a near-limit result), when a long tool name was shortened, or when the receipt ledger overflowed. A missing `toolTrace` does not mean the child called no tools. Check `toolCallCount` and `toolTraceTruncated`.

- `plan_task_check` never fails to read a settled task because of its trace. If a task would not fit the response budget, the trace is removed first, then its truncation flag.

- `task_delegate` now lists each durable subagent type with its description, as `spawn_subagent` already does, so the parent picks a type by what it does rather than by its id alone. Its `task` field also says that the subagent does not see the parent conversation.
