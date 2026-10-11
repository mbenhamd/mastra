---
'@mastra/libsql': patch
---

The Harness channel and wakeup tables gain session indexes, so deleting a session's channel and wakeup rows no longer scans them. Deleting a session also erases the content of its message results kept under another resource or thread scope, and an erased result stays marked as erased when it is read back.
