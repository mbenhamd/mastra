---
'@mastra/core': minor
---

Added `subagent_answer_delta` Harness events, so a delegated subagent's answer streams while it is being written. A subagent answers through the `summary` of its `report_subagent_outcome` call. `spawn_subagent` and task delegation now emit that summary's text in order on the parent session, and each event is tagged with the report call's `innerToolCallId`.

The deltas of one report call form a prefix of the final direct answer's text, and equal it when the report completes as a direct answer. Streaming stops early, leaving a strict prefix that may be empty, in these cases:

- the model streams no tool input;
- a display payload policy applies to the report input;
- the answer exceeds the direct-answer size bound.

It also stops at a second top-level `summary` key. The parsed report keeps that later value, so the streamed first value is then not a prefix of it. Interim subagent prose stays in `subagent_text_delta`, and `subagent_end` remains the authoritative result.

```ts
const answers = new Map<string, { innerToolCallId: string; text: string }>();

session.subscribe(event => {
  if (event.type === 'subagent_answer_delta') {
    const current = answers.get(event.toolCallId);
    // A retried report starts over under a new innerToolCallId.
    if (current?.innerToolCallId !== event.innerToolCallId) {
      answers.set(event.toolCallId, { innerToolCallId: event.innerToolCallId, text: event.delta });
    } else {
      current.text += event.delta;
    }
    render(event.toolCallId, answers.get(event.toolCallId)!.text);
  }
  if (event.type === 'subagent_end') {
    // subagent_end is authoritative: replace the streamed text with the final result.
    answers.delete(event.toolCallId);
    renderFinal(event.toolCallId, event.output, event.isError);
  }
});
```
