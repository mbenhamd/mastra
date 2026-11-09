---
'@mastra/editor': patch
---

The Composio tool provider no longer asks the npm registry for the latest `@composio/core` version. Both Composio clients it builds now pass `disableVersionCheck: true`, so a server using the provider makes no npm request when it starts.
