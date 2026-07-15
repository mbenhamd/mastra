---
'@mastra/google-cloud-pubsub': patch
---

Fixed Google Cloud Pub/Sub workflow delivery to isolate logical lifecycle topics, preserve replay identity, and redeliver rejected async handlers. Same-instance terminal and final-unsubscribe cleanup deletes only the generation-scoped logical subscription, never the shared lifecycle broker topic; crashed or remote ungrouped watcher subscriptions expire after a bounded 24-hour orphan lifetime.
