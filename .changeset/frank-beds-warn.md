---
"@mastra/core": patch
---

Fixed evented workflows retaining an authenticated organization selection in stored request context. Live steps keep the admitted selection, while resume and replay use the current authenticated request.
