---
'@mastra/core': patch
---

Fix `Cannot get workflow run. Mastra storage is not initialized` debug log spam on agents that use memory or any input/output processors.

#17344 fixed this for the internal `execution-workflow`, but agents with memory/processors also build an internal _processor_ workflow (`Agent.combineProcessorsIntoWorkflow`, run by `ProcessorRunner.executeWorkflowAsProcessor`) that never received the parent `Mastra` instance. The processor workflow now receives the parent runtime context and opts out of snapshot persistence (`shouldPersistSnapshot: () => false`), mirroring the execution-workflow fix. Fresh built-in run IDs skip the guaranteed-miss storage lookup entirely; explicit or custom-generated IDs retain the lookup for collision and status synchronization. Follow-up to #17137 / #17344.
