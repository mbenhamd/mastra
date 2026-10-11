---
'@mastra/libsql': patch
---

The Harness channel and wakeup tables gain session indexes, so deleting a session's channel and wakeup rows no longer scans them. An erased message result stays marked as erased when it is read back.
