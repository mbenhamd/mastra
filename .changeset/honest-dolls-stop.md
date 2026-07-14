---
'@mastra/deployer': patch
---

Fixed deployer commands for paths and package specifications containing spaces or special characters. Stuck commands now stop after a timeout, required package-manager settings continue to work, failures report credential-safe status information, and the `.mastra` directory is created and ignored reliably.
