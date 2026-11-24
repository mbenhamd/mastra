---
'@mastra/core': patch
---

Fixed approval-gated tool calls that the user approved and execute then refused for their input. On models with a schema compat layer (Anthropic, Gemini), the approval check validated the tool's own schema while execute validated the compat schema, so a call such as `{ tags: '["x"]' }` passed the check and failed after approval. The approval check now also runs execute's validator on the same arguments, so it refuses whatever execute would refuse. Its own-schema check still runs, and nothing it refused before is accepted now. Without a compat layer the two schemas are the same and the input is validated once.
