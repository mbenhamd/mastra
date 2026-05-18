---
'@mastra/core': patch
---

Fixed agent resume APIs so `resumeStream()`, `resumeStreamUntilIdle()`, and `resumeGenerate()` enforce request-context validation and `AGENTS_EXECUTE` FGA checks before loading persisted snapshots or resuming a run. Resume calls now fail closed when FGA is configured but no authenticated user is present.
