---
'@mastra/core': patch
---

Improved transient workflow performance by skipping guaranteed-miss storage reads for fresh built-in run IDs while retaining collision checks for explicit and custom-generated IDs.
