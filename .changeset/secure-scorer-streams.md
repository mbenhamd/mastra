---
'@mastra/core': patch
---

- Redacted authentication tokens from scorer traces so secrets don't leak into observability data.
- Fixed scorer stream callbacks so callback failures don't disrupt scoring.
- Sanitized malformed streamed tool arguments without slow token processing.
