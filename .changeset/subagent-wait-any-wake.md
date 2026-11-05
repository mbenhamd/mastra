---
'@mastra/core': patch
---

`subagent` `wait` takes `until: 'any' | 'all'` (default `'all'`). With `'any'` it returns as soon as one target has settled. A wait now wakes when this process settles a delegated task, instead of polling storage every second. A slower 5 s storage poll still finds a task settled by another process. Child sessions load concurrently, and each task version's delegation metadata is parsed once.
