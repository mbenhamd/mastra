---
'@mastra/core': patch
---

Tools can now mark a returned failure as a failed call. Set `resultStatus` on `createTool` to classify each returned output as `'success'` or `'error'`.

Use it when a tool returns errors as data, so the model can read them and retry. The model, the stream and the transcript still receive the output unchanged.

Harness v1 records a call classified as `'error'` as failed. A subagent can no longer report `completed` and cite that call as successful evidence. A truthful `failed` or `blocked` report is still accepted.

```ts
const runAnalysis = createTool({
  id: 'run_analysis',
  description: 'Run Python for an analysis.',
  inputSchema: z.object({ code: z.string() }),
  resultStatus: output => (output.success ? 'success' : 'error'),
  execute: async ({ code }) => sandbox.run(code), // returns { success, stdout, stderr }
});
```
