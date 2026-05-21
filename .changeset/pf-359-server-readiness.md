---
'@mastra/server': patch
---

Updated `MastraServer.init()` to call `Mastra.init()` before route registration, delaying server request handling until Harness readiness completes.

```ts
const server = new MastraServer({ app, mastra });
await server.init();
```
