---
'@mastra/core': patch
---

Fixed the Harness v1 reply step that runs after a silent tool turn advertising tools the step before it had withdrawn. The reply step now offers at most the tools of the previous request, so a step that sent no tools keeps sending none. Tool calls on the reply step still never run.
