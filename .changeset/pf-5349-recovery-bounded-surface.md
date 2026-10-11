---
'@mastra/core': patch
---

Fixed a follow-up reply offering tools that earlier requests had removed. When an agent asks the model for one more reply after a turn with no answer, that reply now offers at most the tools of the latest request sent to the model, minus any tool that a later step stopped offering without calling the model (for example a step answered from a cache, or one that failed before reaching the model). This holds on regular and durable agents, also after a tool approval resumes the run. If no request sent tools, or none was recorded, the follow-up reply sends none. Tool calls in the follow-up reply still never run.
