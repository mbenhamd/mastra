---
'@mastra/inngest': minor
---

Add durable, replay-safe Inngest workflow resume delivery with deterministic dispatch identities, exact result receipts, and fenced nested resume handling. Ambiguous send acknowledgements retain their admitted checkpoint for an identical retry. Resume-capable custom storage adapters must implement both atomic resume version 1 and fenced step-update version 1; ordinary runs on adapters without those capabilities keep the existing snapshot persistence path.
