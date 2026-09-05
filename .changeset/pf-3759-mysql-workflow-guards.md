---
'@mastra/mysql': patch
---

Fixed workflow state compare-and-set updates to reject mismatched execution generations and lifecycle resume attempts instead of checking only status.
