---
'mastra': patch
---

Hardened CLI dependency installation by passing package specs as process arguments instead of shell command strings. Installs now share native timeout and cancellation handling, and package specs cannot be reinterpreted as package-manager options.
