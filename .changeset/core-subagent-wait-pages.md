---
'@mastra/core': patch
---

`subagent wait` now returns a page when a burst of settlements is too large for one result: it keeps the children that woke the wait first and hints at the offset of the rest, instead of returning an empty page. A wait with `until: 'any'` also counts a child that settles while its first read is in flight, so it no longer times out on a settlement it already saw.
