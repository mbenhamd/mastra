---
'@mastra/core': patch
---

Fixed `ToolResultSupersedeProcessor` so a group name and a key can never combine into the same internal key as a different group and key. Before, values containing a NUL character could collide.
