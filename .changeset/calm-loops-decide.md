---
'@mastra/core': patch
---

Improved internal groundwork for a future runtime integration that can recover nested `dowhile` and `dountil` condition decisions after a restart. This release does not change user-visible `@mastra/core` behavior or expose recovery support.

Durable loop conditions reject attempts to use Mastra agents, tools, PubSub, stream writers, or shared-state mutation. A crash before the parent continuation is committed may run the condition again, so these callbacks must remain side-effect-free.
