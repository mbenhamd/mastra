---
'@mastra/core': patch
---

Fixed durable agents storing request-context values outside `durableRequestContextKeys`. Every saved copy of the run's request context now keeps only the allowlisted keys plus the run's thread, resource, scopes and versions, so credentials in the live context no longer reach storage. This includes the per-step writes of an `EventedAgent`. Tools in the run still see the full live context. This covers `DurableAgent` and `EventedAgent` from `@mastra/core`; the Inngest durable agent is unchanged.

Fixed a prepared durable run using its original delegation hooks and request context after a resume. A resumed run now passes the resume's context and hooks to the agents it delegates to.
