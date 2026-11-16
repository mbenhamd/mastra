---
'@mastra/core': patch
---

Fixed missing prompt-cache write tokens for OpenAI and Azure OpenAI models. Cache writes reported by the provider now appear in usage as `cacheCreationInputTokens`, so tracing and cost estimates see them instead of counting them as plain input.
