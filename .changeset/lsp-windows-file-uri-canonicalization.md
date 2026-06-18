---
"@mastra/core": patch
---

Fixed Windows diagnostic lookups in Workspace when `lsp: true` is enabled.
Diagnostics are now returned correctly instead of timing out with empty results.

Fixes #17813
