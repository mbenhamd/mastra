### 2.1 Harness vs Session

The Harness is **restartable orchestration infrastructure**. It is the shared
registry, factory, and control plane for live wiring and runtime policy. It may
hold process-local registries, caches, workers, intervals, listeners, and
lifecycle state, but it does not own durable per-conversation state; storage
does. See §1 for the canonical responsibility split.

A Session is **the session-first per-conversation runtime**. It is the active or
reopenable room callers mostly reason about: current run state, queue, pending
decisions, channel binding, memory/context, runtime settings, and execution
ownership. It persists recoverable state through the storage records defined in
§5 and can be hydrated on demand.

```
Harness                    Session
─────────────────────      ────────────────────────────
Infrastructure root        Per-conversation room
Shared across users        One per conversation
Owns composition/policy    Owns runtime state
Restartable front desk     Reopens from storage (§5)
Created once               Created/opened on demand
```

Code should hold references to `Session` objects for lifecycle and runtime work.
The Harness is the thing that hands them out and coordinates the infrastructure
behind them; it should not grow into a broad public lifecycle surface.
