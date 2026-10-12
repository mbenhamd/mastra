---
'@mastra/core': patch
---

Fixed `steps[].content` from `agent.stream()` sometimes holding another step's parts. Each step in `out.steps` (and the step passed to `onStepFinish`) now holds only what that step wrote:

- After a tool call was approved and the run resumed, the answer step listed the approved tool call and its result instead of its own text.
- A retry attempt that an output processor rejected listed the tool call and result of the step before it. It now has no content, as its text was already empty.
- Feedback added by `onIterationComplete` no longer appears in the content of the step it follows.
