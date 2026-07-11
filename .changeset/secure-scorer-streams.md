---
'@mastra/core': patch
---

Scorer traces now redact authentication tokens, scorer stream callbacks continue to receive results without disrupting scoring, and malformed streamed tool arguments are sanitized without pathological delays.
