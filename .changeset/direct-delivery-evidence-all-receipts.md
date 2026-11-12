---
'@mastra/core': patch
---

When a subagent ends by direct delivery, its evidence now lists the terminal call first, followed by its other framework-observed tool receipts in call order. Every receipt that fits the report's item limit (24, including the terminal call) and byte limit is added. The delegated result summary then keeps only the receipts that fit the envelope it already reserves for plan-task reads, so a settled task stays readable. A receipt that would exceed a limit is skipped, and later receipts that fit are still added. A direct delivery that was valid before stays valid. A call left out stays unproven for the caller. Run tool-result fallbacks are not appended, ambiguous earlier call ids are skipped, and failed calls keep an error status.
