---
'@mastra/redis': patch
---

Fixed best-effort workflow state guards to reject mismatched execution generations and lifecycle resume attempts instead of checking only status. Concurrent workflow updates remain unsupported.
