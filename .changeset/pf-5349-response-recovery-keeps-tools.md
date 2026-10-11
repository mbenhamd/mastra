---
'@mastra/core': patch
---

Harness v1's response-only recovery step now keeps the tool schemas of the step before it. Before, the step that asks a silent model to report a completed tool result removed every tool from the request, so the provider's cached prompt prefix was lost and a short reply re-sent the whole context uncached.

The step still cannot run tools: the loop refuses every tool call it emits. A forced tool choice (`'required'` or a named tool) is relaxed to `'auto'` so the model can reply, and a surface that includes provider-executed tools falls back to sending no tools, because the provider would run them. If a configured `prepareStep` throws on the recovery call, the step stays tool-free.
