---
'@mastra/editor': patch
---

Composio `listConnections` now marks each account with `owned` and, when Composio reports it, `accountType`, so callers can tell a user's own account from another user's SHARED account the ACL admits. `authorId` is set only for an owned account when exactly one bucket was requested, and is then that bucket; it no longer reads the raw `user_id`, which the Composio SDK's response schema drops.
