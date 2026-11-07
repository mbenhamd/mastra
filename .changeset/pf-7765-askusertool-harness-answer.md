---
'@mastra/core': patch
---

Fixed `askUserTool` questions inside a Harness: answering with `session.respondToQuestion({ answer })` now resumes the run instead of failing with a resume-schema error. Both free-text and multi-select answers work, and the bare answer passed to `agent.resumeStream(answer)` is still accepted.

```ts
await session.respondToQuestion({ answer: 'Blue' });
await session.respondToQuestion({ answer: ['A', 'C'] }); // multi_select
```

An answer that is neither text nor a list of labels is still refused, and the question stays open for a valid answer.
