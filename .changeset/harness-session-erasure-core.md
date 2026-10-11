---
'@mastra/core': patch
---

Deleting a Harness session no longer keeps any of its content. The rows it keeps to refuse or resolve replays (terminal admissions and intents, projection intents, committed message results) now hold only identity, status and hashes, and grant tombstones keep only a reason code. A late admission is still refused, and a committed retry or a late acknowledgement still resolves.

A duplicate or waiting caller that asks for the result of a turn whose session was deleted now fails with `HarnessMessageResultErasedError` instead of resolving with an empty result:

```ts
import { HarnessMessageResultErasedError } from '@mastra/core/harness/v1';

try {
  await session.message({ content, admissionId });
} catch (error) {
  if (error instanceof HarnessMessageResultErasedError) {
    // The turn completed, but its session was deleted and its result erased.
  } else {
    throw error;
  }
}
```
