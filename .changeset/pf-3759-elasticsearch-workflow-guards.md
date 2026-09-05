---
'@mastra/elasticsearch': patch
---

Fixed best-effort workflow state guards to reject mismatched execution generations and lifecycle resume attempts instead of checking only status. Workflow snapshot keys now use namespace, workflow name, and run ID, while resource metadata is preserved when re-persistence omits it. Concurrent workflow updates remain unsupported.
