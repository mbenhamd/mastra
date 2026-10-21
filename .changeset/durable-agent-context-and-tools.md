---
'@mastra/core': patch
---

Fixed durable agents storing request-context values outside `durableRequestContextKeys`. Saved workflow snapshots now keep only the allowlisted keys plus the run's thread, resource, scopes and versions, so credentials in the live context no longer reach storage. Tools in the run still see the full live context.

Fixed a prepared durable run using its original delegation hooks and request context after a resume. A resumed run now passes the resume's context and hooks to the agents it delegates to.
