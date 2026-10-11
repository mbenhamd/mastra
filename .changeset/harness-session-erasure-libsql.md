---
'@mastra/libsql': patch
---

Deleting a Harness session now erases the content of its message results kept under another resource or thread scope, and an erased result stays marked as erased when read back. Deleting a session's channel and wakeup rows is also faster, because those tables gain session indexes.
