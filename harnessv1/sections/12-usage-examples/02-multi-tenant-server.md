### 12.2 Multi-tenant server

A web service hosting the same Harness instance for many users. Each request maps to a session.

```ts
import {
  Harness,
  HarnessSessionNotFoundError,
  type Session,
} from '@mastra/core/harness';

const harness = new Harness(config);
await harness.init();

// HTTP handler: admit a user-message signal on behalf of the user.
app.post('/sessions/:sessionId/signals', async (req, res) => {
  const { user } = req.auth;
  const { sessionId } = req.params;

  // Find or create the session for this resource through the §4.1
  // resolution rules.
  const session = await harness.session({
    sessionId,
    resourceId: user.id,
  });

  // `signal({ stream: true })` resolves at the durable admission boundary.
  // Clients observe completion through the SSE event stream or result lookup;
  // this handler must not return accepted until the v1 admission receipt exists.
  const stream = await session.signal({
    type: 'user-message',
    contents: req.body.contents,
    stream: true,
    admissionId: req.body.admissionId,
  });

  res.status(202).json({
    accepted: true,
    sessionId,
    signalId: stream.signalId,
    runId: stream.runId,
  });
});

// SSE handler: stream session events to the client.
app.get('/sessions/:sessionId/events', async (req, res) => {
  const { user } = req.auth;
  let session: Session;
  try {
    session = await harness.session({
      sessionId: req.params.sessionId,
      resourceId: user.id,
    });
  } catch (err) {
    if (err instanceof HarnessSessionNotFoundError) return res.status(404).end();
    throw err;
  }

  res.setHeader('Content-Type', 'text/event-stream');
  const unsubscribe = session.subscribe((event) => {
    res.write(`data: ${JSON.stringify(event)}\n\n`);
  });

  req.on('close', unsubscribe);
});

// Memory eviction is automatic under the §9 residency knobs and §5 lifecycle.
//
// Local-runtime demo only. If you also want to *terminate* sessions that have
// been idle for a long time (e.g., abandoned tabs older than 30 days), run an
// operator/runtime sweeper against storage. Restart-safe scheduled work should
// use wakeup/scheduler rows, not registerHeartbeat.
harness.registerHeartbeat({
  id: 'idle-session-terminator',
  intervalMs: 24 * 60 * 60_000,
  handler: async () => {
    const cutoff = Date.now() - 30 * 24 * 60 * 60_000;
    for (const user of await getActiveUsers()) {
      let cursor: string | undefined;
      do {
        const page = await harness.listSessions({ resourceId: user.id, cursor });
        for (const summary of page.items) {
          if (!summary.closedAt && summary.lastActivityAt < cutoff) {
            await operatorCloseSession({
              sessionId: summary.sessionId,
              resourceId: summary.resourceId,
            });
          }
        }
        cursor = page.nextCursor;
      } while (cursor);
    }
  },
});
```
