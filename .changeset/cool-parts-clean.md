---
"@mastra/core": patch
---

Fixed structured output retaining a rejected attempt when an output-step processor retries the response.

Fixed background queue cleanup rejecting after session lease eviction while preserving durable queued work for recovery.
