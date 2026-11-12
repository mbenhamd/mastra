---
'@mastra/core': patch
---

When a subagent ends by direct delivery, its evidence now lists the terminal call first, followed by its other framework-observed tool receipts in call order. Receipts are added only while the report stays within its item limit (24, including the terminal call) and its byte limit. A direct delivery that was valid before stays valid, and a call left out stays unproven for the caller. Run tool-result fallbacks are not appended, ambiguous earlier call ids are skipped, and failed calls keep an error status.
