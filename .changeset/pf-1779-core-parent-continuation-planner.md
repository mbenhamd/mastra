---
'@mastra/core': patch
---

Added deterministic nested-workflow continuation-planning contracts and exports:

```ts
import { planWorkflowTerminalParentContinuation } from '@mastra/core/workflows';

const contract = planWorkflowTerminalParentContinuation(plannerInput);
```

This prepares retry-safe recovery for a future release and does not change current runtime execution or retry behavior.
