---
'@mastra/core': minor
---

Added internal recovery support for nested `dowhile` and `dountil` condition decisions. Receipt-backed workflow processing can restore the exact condition inputs after a restart and bind the result to one parent revision. This change does not switch the existing evented runtime yet.

Durable loop conditions reject attempts to use Mastra agents, tools, PubSub, stream writers, or shared-state mutation. A crash before the parent continuation is committed may run the condition again, so these callbacks must remain side-effect-free.
