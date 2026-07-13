---
'@mastra/deployer': patch
---

Run deployer package-manager commands safely when paths or package specs contain spaces or special characters. Commands now fail after a bounded process-tree timeout, preserve required platform bootstrap variables, and propagate only credential-safe failure status.
