---
'@mastra/pg': minor
---

`claimTerminalIntents` accepts a `finalizerIds` filter. Per-session claim ordering now holds within one finalizer id, so an unsettled intent of one kind never blocks another kind. This holds with or without the filter, so intents under a renamed finalizer id are no longer ordered against intents under the old id in the same session. This is a query change only; the schema is unchanged.

```ts
const intents = await storage.claimTerminalIntents({
  consumerId: 'chat-usage-consumer',
  limit: 10,
  finalizerIds: ['my-app.chat'], // omit to claim every kind
});
```
