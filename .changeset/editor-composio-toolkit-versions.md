---
'@mastra/editor': minor
---

Added `toolkitVersions` to `ComposioToolProvider`. It pins the dated version each Composio toolkit's tools are fetched and executed at, so a host no longer follows Composio's schema changes unannounced for the toolkits it maps. The map is passed to both Composio clients and overrides the SDK's `COMPOSIO_TOOLKIT_VERSION_<TOOLKIT>` environment values key by key. A toolkit in neither still runs Composio's `latest` version, and the connection-management session tools are not pinned.

```ts
const provider = new ComposioToolProvider({
  apiKey: process.env.COMPOSIO_API_KEY!,
  toolkitVersions: { gmail: '20261120_00', github: '20261123_00' },
});
```
