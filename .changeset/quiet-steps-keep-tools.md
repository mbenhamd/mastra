---
'@mastra/core': patch
---

Fixed saved history losing a tool call and its result when an answer was rejected after iteration feedback. When `onIterationComplete` added feedback after a tool step and an output processor then rejected the next answer, the retry removed the whole tool step message instead of only the rejected answer. Only the rejected answer is removed now, so the tool call and result stay in history.

Fixed a step's text including earlier history that an input processor rewrote. If an input processor replaced an earlier assistant part with a changed copy (for example a redacted one), that copy was counted as the current step's output. A step's content now starts after input processors have run.

Recording where each step writes its content no longer copies every text, reasoning and file payload in the response at every step, which kept long runs and runs with large files from growing slower and using more memory.
