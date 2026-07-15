### 12.1 Single-user TUI

The classic Mastra Code shape. One human, one process, one conversation at a
time, but the user can switch between sessions (each bound to durable thread
history). Product code resolves sessions through `harness.session(...)`, not
thread-first lifecycle helpers (§0, §4.1).

```ts
import { Harness } from '@mastra/core/harness';
import { config } from './harness-config';

const harness = new Harness(config);
await harness.init();

// Bootstrap: product code chooses a concrete session first. Harness v1 has no
// resource-only "latest or create" resolver.
const existing = await harness.listSessions({ resourceId: 'local-user', limit: 1 });
let activeSession = existing.items[0]
  ? await harness.session({ sessionId: existing.items[0].sessionId, resourceId: 'local-user' })
  : await harness.session({ resourceId: 'local-user', threadId: { fresh: true } });

// Subscribe to events for live rendering.
const unsubscribe = activeSession.subscribe(event => {
  switch (event.type) {
    case 'text_delta':
      process.stdout.write(event.delta);
      break;
    case 'tool_start':
      console.log(`\n[tool] ${event.toolName} started`);
      break;
    case 'agent_end':
      console.log('\n[idle]');
      break;
  }
});

// User presses Enter. `signal` is busy-independent once admitted; it either
// starts a run or drains into the live run under the §3 rules.
activeSession.signal({ type: 'user-message', contents: 'Refactor the auth middleware' });

// User types again while the agent is still working. With agent signals this
// just drops into the same run as new user input — the model sees both
// messages mid-reasoning.
activeSession.signal({
  type: 'user-message',
  contents: 'Also add rate limiting',
  attributes: { delivery: 'while-active' },
});

// Use `queue` instead when you specifically want sequential, isolated turns
// (one turn finishes fully before the next prompt starts).
activeSession.queue({ contents: 'Now run the test suite' });

// Current MastraCode has the same product split today: Enter is an immediate
// signal, while Ctrl+F uses a process-local FIFO that v1 replaces with the
// durable session queue above. Goal and plan continuations are structured
// `system-reminder` signals instead of a separate message facade.

// Product `/new` can remain a local staged intent. No durable room is created
// until the first admission needs one.
let stagedFresh = true;
async function currentSessionForAdmission() {
  if (!stagedFresh) return activeSession;
  stagedFresh = false;
  activeSession = await harness.session({ resourceId: 'local-user', threadId: { fresh: true } });
  return activeSession;
}

// User switches to a different thread. The current session stays resumable in storage —
// we don't close it, we just stop using it. (Eviction will drop it from memory if idle.)
const otherSession = await harness.session({ sessionId: otherSessionId });

// Shutdown.
await harness.shutdown();
```
