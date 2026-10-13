---
'@mastra/core': patch
---

A deliberately cancelled terminal turn now counts its consumed token usage exactly once. This holds even when its completed output is still staged across a restart, and when its cancellation settlement fails and a later process recovers it. It includes a resumed turn that is aborted explicitly or cancelled by stale-resume recovery. Recovered completion is no longer published after another process deletes the session, and a live owner finishing the same run afterwards does not emit a second `agent_end`. A failed terminal admission read no longer fails an ordinary turn that was not admitted under terminal handoff. A parked turn whose admitted finalizer version is not registered is refused before the provider resumes, and the same response can resume it once that version is registered.
