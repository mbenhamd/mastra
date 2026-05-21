---
'@mastra/mcp': patch
---

Fixed OAuth authentication for confidential clients. Token exchange and refresh requests now correctly include client credentials, resolving `invalid_request` errors that occurred when using confidential-client OAuth servers. See [#16854](https://github.com/mastra-ai/mastra/issues/16854).
