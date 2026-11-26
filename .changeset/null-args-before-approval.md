---
'@mastra/core': patch
---

Fixed approval-gated tool calls with unparseable arguments reaching the approval prompt. On the agent loop, a call whose arguments could not be parsed is now refused before the user is asked to approve it, with the same error text the durable agent returns. Before, a tool whose input fields are all optional asked for approval and then failed with "received invalid arguments"; a tool with a required field returned an input validation error instead of the parse error.
