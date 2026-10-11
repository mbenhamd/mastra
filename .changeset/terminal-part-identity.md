---
'@mastra/core': patch
---

A terminal tool result is now delivered when a final output processor gives the response message a new id. Before, the terminal commit looked for the terminal part only in the message whose id matched the step's message id. A memory processor that stamps a deterministic storage id on the assistant message (a `MessageHistory` subclass, for example) made the run fail with `Final output processors did not retain exactly one terminal tool result`, and the caller got no answer.

The check now finds the terminal part by its own framework part id across the response messages. Terminal parts from earlier runs, which a signal boundary can bring into the response with recalled history, no longer count against the check. Both the streaming and the durable paths still require exactly one terminal part with this run's id and unchanged data, and they compare against a snapshot taken before the final output processors run, so a processor that drops, duplicates, edits, or copies the terminal result under another id is rejected.

```ts
class StampingMessageHistory extends MessageHistory {
  async processOutputResult(args) {
    const assistant = args.messageList.get.response.db().findLast(m => m.role === 'assistant');
    if (assistant) assistant.id = 'assistant-turn-42';
    return super.processOutputResult(args);
  }
}
```
