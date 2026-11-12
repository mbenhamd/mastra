---
'@mastra/editor': patch
---

Added `owned` and, when Composio reports it, `accountType` to each Composio connection so callers can tell their own accounts from shared accounts they can access. Set `authorId` for an owned account when one user was requested.
