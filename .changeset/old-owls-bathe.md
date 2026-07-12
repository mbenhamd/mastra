---
'@mastra/core': patch
---

Fixed SystemPromptScrubber streaming blocks so detected system-prompt leakage stops the stream instead of failing open.
