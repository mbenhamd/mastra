---
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

Reject `expectedCancelRequest` in `updateWorkflowState` with a clear error before accessing the adapter. These adapters do not support retained cancellation requests.
