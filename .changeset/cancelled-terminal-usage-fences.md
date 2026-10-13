---
'@mastra/core': patch
---

A deliberately cancelled terminal turn now counts its consumed token usage exactly once. This holds even when its completed output is still staged across a restart, including for a resumed turn that stale-resume recovery cancels. Recovered completion is no longer published after another process deletes the session. The retry is rejected with the session-deleted error or the result-erased error instead.
