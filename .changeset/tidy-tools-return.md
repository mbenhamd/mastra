---
'@mastra/core': minor
'@mastra/inngest': patch
'@mastra/memory': patch
---

Tools can now opt into returning a successful, schema-validated projection as the terminal agent result. Mastra stops before a redundant follow-up model call, exposes the bounded result through `FullOutput.terminalToolResult`, and streams a `data-terminal-tool-result` part across regular, evented, durable, and Inngest execution.

Terminal delivery requires an explicit schema-validated projection and fails closed for mixed tool batches, failed or denied calls, approval-gated calls, resumed suspensions, provider-executed and background tools, signals queued before terminal arbitration on engines that support active-run signals, pending background work, output processors that are not explicitly pass-through, and transcript-changing payload transforms. Explicit display-only pass-through transforms remain eligible because they do not alter the model transcript. Signals accepted after terminal arbitration remain thread follow-up turns. Durable runs persist the result until finalization and preserve `tool-result` → terminal result → `step-finish` ordering across replay.

Recoverability-critical message saves now retain newer merged content and remain retryable when racing best-effort and strict storage writes both fail.
