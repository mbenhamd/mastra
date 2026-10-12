---
'@mastra/pg': patch
---

A Harness message result can no longer be written for a deleted session. A late result, dispatch stamp or settlement locks the session row first and is refused when the session is gone, so a delete cannot be undone by a turn that finishes afterwards.

`deleteMessages` supports the `sessionLeaseFence` option: the deletion and its observational-memory retraction apply only while the caller still holds the Harness session's lease, checked in the same transaction. The Harness session table must be in the same schema as memory.

A store with session-record projections turned off may delete sessions in a schema where another store projects them. The delete still retires and erases that session's projection intents, releases their queue capacity and closes its projection fence, so the deleting store no longer needs projections turned on.
