### 12.4 Headless script — streaming

Consume the `AgentStream.textStream` facade after awaiting stream admission.
§4.2 owns admission and settlement semantics; §4.8 owns the `AgentStream`
shape. This example is not a full MastraCode headless migration; event-driven
controllers still need pending-inbox responders, lifecycle subscriptions, result
lookup, and their product output-format contract.

```ts
const session = await harness.session({
  resourceId: 'cron:report-builder',
  threadId: { fresh: true },
});

const stream = await session.signal({
  type: 'user-message',
  contents: 'Generate the weekly report',
  stream: true,
});
for await (const chunk of stream.textStream) {
  process.stdout.write(chunk);
}
```

For the clean turn-boundary form, see §12.3 and the canonical §3/§4.2 rules.
