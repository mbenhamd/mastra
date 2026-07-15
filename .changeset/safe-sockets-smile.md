---
'@mastra/code-sdk': patch
---

Harden MastraCode Unix-socket routing by sanitizing and bounding resource and thread path components before creating local cross-process PubSub sockets, and construct its in-memory Harness storage with the fork's required database dependency.
