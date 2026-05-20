---
"@mastra/core": minor
"@mastra/libsql": patch
"@mastra/pg": patch
---

Harness sessions can now be reliably resumed from scheduled or proactive triggers, even after server restarts.

LibSQL and PostgreSQL now preserve the queue admission override flag while recovering scheduled Harness work.

PostgreSQL storage now includes favorites during setup and migrations, so favorites remain available after store initialization and restarts.

```ts
import { Mastra } from '@mastra/core';

new Mastra({
  harnesses: { default: harness },
  storage,
  harnessWakeups: { pollIntervalMs: 1_000 },
});
```
