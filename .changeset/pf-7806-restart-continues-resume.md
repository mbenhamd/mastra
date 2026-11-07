---
'@mastra/core': patch
---

Fixed `run.restart()` losing a resume answer after a crash. When a process died after `run.resume()` was accepted but before the resumed step finished, a restart re-ran that step without its resume data, so it suspended again and asked for the same answer. The accepted resume data is now stored with the resume claim, and `run.restart()` (including the boot-time recovery sweep) continues the resume with it once. This needs a workflows store with concurrent updates, such as `@mastra/pg` or `@mastra/libsql`.
