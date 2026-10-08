---
'@mastra/core': minor
'@mastra/libsql': patch
'@mastra/pg': patch
'@mastra/redis': patch
'@mastra/valkey': patch
'@mastra/elasticsearch': patch
'@mastra/mysql': patch
'@mastra/mssql': patch
'@mastra/dynamodb': patch
'@mastra/dsql': patch
'@mastra/spanner': patch
'@mastra/oracledb': patch
'@mastra/mongodb': patch
'@mastra/upstash': patch
'@mastra/convex': patch
---

Retain cancellation requests so later requests for the same execution keep the first request ID and timestamp.

Use the explicit lineage to retain a durable request even when cancellation is immediate or that lineage has already been canceled:

```ts
await run.requestCancel({
  requestId: 'abort-operation',
  expectedExecutionGeneration: generation,
  expectedLifecycleResumeAttempt: attempt,
  retainCancellationRequest: true,
});
```

Retries preserve the first request and its recorded request occurrence. This timestamp does not reconstruct a historical cancellation time absent from an older snapshot. Unsupported stores refuse retention before writing.
