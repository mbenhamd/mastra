---
'@mastra/core': patch
---

Fixed workflow commits that could run out of memory when `.map()` uses `mapVariable({ initData: workflow })`. Map configurations now store a compact workflow reference, keeping workflow setup reliable.
