---
'@mastra/core': patch
---

Fixed workflow commits that could run out of memory when `.map()` uses a live workflow as an `initData` or step source. Serialized map configurations now store compact workflow and step references, keeping workflow setup reliable.
