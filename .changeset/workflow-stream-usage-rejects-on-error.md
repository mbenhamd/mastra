---
'@mastra/core': patch
---

Fixed `run.stream()` and `run.resumeStream()` reporting zero token usage when the workflow stream failed. When the stream fails before the run finishes, awaiting `usage` now rejects with the stream error instead of resolving to all-zero token counts, so a failed run can no longer be billed or recorded as a successful run that used no tokens.
