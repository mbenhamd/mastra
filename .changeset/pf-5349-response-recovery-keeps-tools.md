---
'@mastra/core': patch
---

Improved the Harness v1 reply step that runs after a silent tool turn. The step now keeps the configured tools available to the model, so the request matches the one before it and reuses the provider's cache. It never runs a tool call.

If the tool choice forces a tool, it becomes `'auto'` for this step so the model can reply. If any available tool runs on the provider side, the step sends no tools.
