---
'mastra': patch
---

Create projects more safely when dependency specs contain spaces or special characters. `mastra create --timeout=60000` now applies the timeout to package-manager initialization and dependency installs; when the timeout is reached, the CLI terminates the package-manager process and reports project creation failure. Package specs can no longer be interpreted as package-manager options.
