---
'@mastra/core': patch
---

Improved how `submitPlan` reports a declined plan to the model. A decline without revision notes now tells the model the plan is cancelled and it should stop, and a decline with notes tells it to revise the plan and submit it again. Before, the model only saw `{ approved: false }` and had to guess. The stored result and the host payload are unchanged.
