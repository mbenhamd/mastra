---
'@mastra/inngest': patch
---

Fixed Inngest workflow runs to preserve raw schema input and request-context validation, honor `disableScorers` across remote execution, and explicitly reject unsupported per-run PubSub and Core schedule options while keeping the original explicit factory generics source-compatible.
