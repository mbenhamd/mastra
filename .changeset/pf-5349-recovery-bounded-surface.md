---
'@mastra/core': patch
---

Fixed a follow-up reply offering tools that the previous request had removed. When an agent asks the model for one more reply after a turn with no answer, that reply now offers at most the tools the previous request sent, on regular and durable agents, also after a tool approval resumes the run. If the previous request sent no tools, the follow-up reply sends none. Tool calls in the follow-up reply still never run.
