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

Reject unsupported cancellation-request comparison guards before adapter access instead of allowing a guard to enter the persisted snapshot. These adapters do not retain durable cancellation requests and do not advertise retainedCancelRequestVersion1.
