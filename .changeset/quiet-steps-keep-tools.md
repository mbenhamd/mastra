---
'@mastra/core': patch
---

Fixed saved history losing a tool call and its result when an answer was rejected after iteration feedback. When `onIterationComplete` added feedback after a tool step and an output processor then rejected the next answer, the retry removed the whole tool step message instead of only the rejected answer. Only the rejected answer is removed now, so the tool call and result stay in history.

Fixed a step's text including earlier history that an input processor rewrote. If an input processor replaced an earlier assistant part with a changed copy (for example a redacted one), that copy was counted as the current step's output. A step's content now leaves out what input processors rewrite, and keeps what the step itself wrote.

Long runs, and runs that produce large files such as images, are faster and use less memory.
