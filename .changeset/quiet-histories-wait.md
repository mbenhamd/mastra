---
'@mastra/core': patch
---

Prevent MessageHistory from committing failed or aborted turns, including partial assistant output, while continuing to run the rest of the output-processor chain.
