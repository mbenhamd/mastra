---
'@mastra/libsql': patch
---

A Harness message result can no longer be written for a deleted session. Each result, dispatch stamp or settlement write now checks in the same statement that the session still exists, and is refused when it is gone, so a delete cannot be undone by a turn that finishes afterwards.
