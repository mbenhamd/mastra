---
'@mastra/editor': patch
---

Fixed Composio catalog tools from `resolveCatalogTools` running an external action after the run was stopped. A call now checks the run's abort signal before and after it resolves its identity, and throws without reaching Composio if the run was aborted.
