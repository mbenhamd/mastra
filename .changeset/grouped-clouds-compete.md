---
'@mastra/google-cloud-pubsub': patch
---

Fixed grouped subscribers registered on one Google Cloud Pub/Sub adapter instance so each broker message is delivered to one competing callback instead of being fanned out to every callback in the group.
