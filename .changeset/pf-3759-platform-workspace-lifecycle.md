---
'@mastra/platform-workspace': patch
---

Fixed platform sandbox lifecycle races by binding teardown, exec leases, checkpoint capture, and direct execution to the exact sandbox generation. Stop and destroy now coordinate checkpoint handling and keep command execution and checkpoint capture closed when an in-flight start finishes during teardown. Clones retain the provider, template, working directory, and environment defaults they need to reproduce the parent configuration.

Restart requests now wait for pending teardown before the base lifecycle can report an already-running sandbox, then provision the replacement when required.
