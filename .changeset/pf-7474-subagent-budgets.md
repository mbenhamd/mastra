---
'@mastra/core': minor
---

Added per-type step and token budgets for Harness subagents.

A subagent type can now set `maxSteps` and `maxTotalTokens`. The Harness counts the child's provider steps and reported tokens, for both `spawn_subagent` and `task_delegate` children, and stops the child at the step where a budget is reached. The parent then gets an error result with the code `harness.subagent_budget_exhausted`. That result keeps the child's partial text, tool trace and usage, and a `budgetExhausted` field that names the budget, its limit and what was used. A stopped child is never reported as successful. Types without a budget behave as before.

```ts
const harness = new Harness({
  // ...
  subagents: {
    types: {
      searcher: {
        agentId: 'search-agent',
        description: 'Search the literature',
        maxSteps: 12,
        maxTotalTokens: 200_000,
      },
    },
  },
});
```

`onIterationComplete` now also receives `usage`, the token usage of that iteration's model call.
