---
'@mastra/google-cloud-pubsub': patch
---

Fixed Google Cloud Pub/Sub workflow delivery to isolate logical lifecycle topics, preserve replay identity, and redeliver rejected async handlers.
