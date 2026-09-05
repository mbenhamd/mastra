---
'@mastra/upstash': patch
---

Fixed atomic workflow state updates to guard execution generations and lifecycle resume attempts, including an explicitly empty expected generation. Workflow snapshot keys now use namespace, workflow name, and run ID, while resource metadata is preserved when re-persistence omits it.
