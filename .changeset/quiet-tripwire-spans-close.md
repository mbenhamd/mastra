---
'@mastra/core': patch
---

Agent traces now close when a processor tripwire ends a step. Previously, when a processor called `abort()` mid-run (for example in `processInputStep`, or Observational Memory after an observation failure), the stream ended without a `finish` chunk and the `AGENT_RUN` and `MODEL_GENERATION` spans were never ended, so exporters dropped the whole trace. The agent span now ends with the tripwire reason in its output and `tripwireAbort` attributes, and its open child spans are closed.
