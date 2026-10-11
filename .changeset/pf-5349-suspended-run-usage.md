---
'@mastra/core': patch
---

Fixed token usage for runs that suspend. A suspended run now reports the usage of the model step whose tool call suspended it. Before, that step was counted only after resume, so a run that suspended on its first step reported zero tokens. Totals after resume are unchanged.
