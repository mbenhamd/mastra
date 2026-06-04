### 12.3 Headless script — typed structured output

A backend job calls the Harness directly without a UI. Uses `signal` with a Zod schema for typed output. This is the clean one-shot case; richer controllers such as MastraCode headless still need subscription, pending-inbox response, output-format, and result-lookup plumbing.

```ts
import { z } from 'zod';
import { Harness } from '@mastra/core/harness';

const harness = new Harness(config);
await harness.init();

const session = await harness.session({
  resourceId: 'cron:nightly-summarizer',
  threadId: { fresh: true },
});

const SummarySchema = z.object({
  title: z.string(),
  bullets: z.array(z.string()).max(5),
  sentiment: z.enum(['positive', 'neutral', 'negative']),
});

// `output` with `sync: true` uses the clean turn-boundary path in §3/§4.2.
const summary = await session.signal({
  type: 'user-message',
  contents: `Summarize this support ticket:\n\n${ticket.body}`,
  output: SummarySchema,
  sync: true,
  model: 'anthropic/claude-haiku-4-5', // per-call override
});

// summary is typed as z.infer<typeof SummarySchema> — no casting.
await db.summaries.insert({
  ticketId: ticket.id,
  title: summary.title,
  bullets: summary.bullets,
  sentiment: summary.sentiment,
});

await session.close();
```
