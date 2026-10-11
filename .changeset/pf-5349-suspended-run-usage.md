---
'@mastra/core': patch
---

Fixed token usage for runs that suspend on a tool approval or a tool suspension. The suspended result now includes the model step that suspended the run, also after a cold resume that suspends again. Before, a run that suspended on its first step reported zero tokens. Totals after the run finishes are unchanged. The durable agent engine is not covered yet.
