---
'@mastra/inngest': patch
---

Fixed Inngest workflow runs to preserve raw schema input (including transformed input with explicit factory generics), validate request context before dispatch, retain `disableScorers` across worker replacement and resume, roll back rejected start events, and explicitly reject unsupported per-run PubSub and Core schedule options.
