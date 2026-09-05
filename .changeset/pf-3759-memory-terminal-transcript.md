---
'@mastra/memory': patch
---

Fixed observational memory finalization to use the final output transcript, including partial assistant text materialized after cancellation, instead of the earlier input-step transcript.
