---
'@mastra/core': patch
---

A subtree delegation settled as `blocked` no longer leaves its parent tasks stuck at `blocked`. The settled parents above unfinished work stay rollup-owned, so they complete once the blocked work is finished.
