---
'@mastra/editor': patch
---

Composio `listConnections` now marks each account with `owned` and, when Composio reports it, `accountType`, so callers can tell a user's own account from another user's SHARED account the ACL admits. `authorId` is the requested bucket when one bucket owns the account; it no longer reads the raw `user_id`, which the Composio SDK's response schema drops.
