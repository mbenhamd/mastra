---
'@mastra/core': patch
---

A Harness message result can no longer be written for a deleted session. Storage now refuses a late result, dispatch stamp or settlement for a session that no longer exists, so a delete cannot be undone by a turn that finishes afterwards, and the session reports such a turn as deleted.

The error message kept on a deleted session's terminal result, and the reason kept on a grant tombstone, now read "The error message is not retained" instead of being empty, so the kept result is still a valid terminal result.

Memory storage adds an optional ownership check to `deleteMessages`: with `sessionLeaseFence`, the deletion and its observational-memory retraction apply only while the caller still holds the Harness session's lease, and are refused otherwise. Check `supportsSessionLeaseFencedMessageDeletion` before relying on it. The in-memory store supports it.
