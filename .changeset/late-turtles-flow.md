---
'mastracode': minor
---

Improved MastraCode task tracking so agents keep stable task IDs in prompts and update one task at a time while working.

MastraCode now preserves Harness task IDs in state, includes those IDs in the current task list prompt, and replays structured task snapshots from full thread history when a thread reloads. The TUI keeps successful task updates quiet, shows task-tool failures inline, and avoids duplicate completed-task summaries.
