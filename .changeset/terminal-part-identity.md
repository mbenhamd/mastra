---
'@mastra/core': patch
---

A terminal tool result is now delivered when a final output processor gives the response message a new id. Before, the terminal commit looked for the terminal part only in the message whose id matched the step's message id. A memory processor that stamps a deterministic storage id on the assistant message (a `MessageHistory` subclass, for example) made the run fail with `Final output processors did not retain exactly one terminal tool result`, and the caller got no answer.

The check now finds the terminal part by its own framework part id across the response messages. Both the streaming and the durable paths still require exactly one terminal part with that id and unchanged data, so a processor that drops, duplicates, or edits the terminal result is still rejected.

```ts
class StampingMessageHistory extends MessageHistory {
  async processOutputResult(args) {
    const assistant = args.messageList.get.response.db().findLast(m => m.role === 'assistant');
    if (assistant) assistant.id = 'assistant-turn-42';
    return super.processOutputResult(args);
  }
}
```
