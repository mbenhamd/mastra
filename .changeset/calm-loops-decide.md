---
'@mastra/core': patch
---

Improved internal groundwork for a future runtime integration that can recover nested `dowhile` and `dountil` condition decisions after a restart. There are no user-visible `@mastra/core` behavior changes in this release.

Durable loop conditions reject attempts to use Mastra agents, tools, PubSub, stream writers, or shared-state mutation. A crash before the parent continuation is committed may run the condition again, so these callbacks must remain side-effect-free.
