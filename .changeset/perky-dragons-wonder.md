---
'@mastra/schema-compat': patch
'@mastra/mongodb': patch
'@mastra/upstash': patch
'@mastra/core': patch
'@mastra/convex': patch
---

Fixed structured output failing against OpenAI strict-mode providers (e.g. openrouter-routed openai/* models): the schema transformer only applied the Anthropic compat layer, so schemas containing z.record(), optional fields, or discriminated unions were rejected upstream. Response validation now also runs the matched compat layer's post-processing, so provider-emitted nulls for optional fields map back correctly — including inside record values.
