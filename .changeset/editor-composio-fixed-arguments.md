---
'@mastra/editor': minor
---

Added `fixedArguments` to `ComposioToolProvider.resolveCatalogTools`. It lets a host set an argument's value for a tool on every call. The model is not offered the argument. A call that sends it anyway fails input validation and never reaches Composio.

Resolving fails when a fixed argument names a tool outside the catalog, or a parameter the tool does not have. So a renamed Composio argument fails loudly instead of being silently left unset.

```ts
const tools = await provider.resolveCatalogTools({
  toolSlugs: ['GMAIL_FETCH_EMAILS'],
  // Searches return headers and text, without each message's base64 MIME payload.
  fixedArguments: { GMAIL_FETCH_EMAILS: { include_payload: false } },
  resolveIdentity,
});
```
