---
'mastra': patch
'@mastra/codemod': patch
---

Hardened CLI dependency installation and codemod execution by passing untrusted values as process arguments instead of shell command strings. Codemod paths containing spaces now work, quoted custom runner options remain intact, and package specs cannot be reinterpreted as package-manager options.
