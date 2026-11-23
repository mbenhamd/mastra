---
'@mastra/editor': minor
---

Added `fixedArguments` to `ComposioToolProvider.resolveCatalogTools`. It lets a host set an argument's value for a tool on every call. The model is not offered the argument, and the fixed value replaces whatever a call sends. When the tool's schema does not allow additional properties, a call that sends the argument is refused before it reaches Composio.

Resolving fails when a fixed argument names a tool outside the catalog, or a parameter the tool does not have. So a renamed Composio argument fails loudly instead of being silently left unset.

```ts
const tools = await provider.resolveCatalogTools({
  toolSlugs: ['GMAIL_FETCH_EMAILS'],
  // Searches return headers and text, without each message's base64 MIME payload.
  fixedArguments: { GMAIL_FETCH_EMAILS: { include_payload: false } },
  resolveIdentity,
});
```
