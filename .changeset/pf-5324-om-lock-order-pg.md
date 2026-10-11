---
'@mastra/pg': patch
---

[PF-5324] Guarded Observational Memory working-memory writes now lock the memory record only when it may need its not-managed marker stamped, so other memory updates no longer wait behind every observer write.
