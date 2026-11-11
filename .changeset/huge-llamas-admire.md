---
'@mastra/core': minor
---

Added a `stopOnError` option to `runEvals`. Set it to `false` and an item whose agent call or workflow run throws no longer rejects the whole run: the other items keep running, `runEvals` resolves only after every started item has finished, and each failed item is listed in the new `failures` field with its position in `data`, the item, and the error.

```ts
const result = await runEvals({
  target: agent,
  data,
  scorers: [accuracy],
  concurrency: 4,
  stopOnError: false,
});

for (const { itemIndex, error } of result.failures ?? []) {
  console.log(`Item ${itemIndex} failed:`, error.cause);
}
```

The default stays `true`, so existing runs still reject on the first failed item. Failed items are left out of `summary.totalItems`, scores and the verdict, so a run with some failed items can still pass: check `failures` before trusting the verdict. If every item fails, the result has `failures`, empty `scores` and `summary.totalItems: 0`, and no gate or threshold results and no verdict.

In a `stopOnError: false` run, aborting `targetOptions.abortSignal` stops the run even when the agent returns instead of throwing on abort: no further items start, an item whose agent call or workflow run finishes after the abort is not scored, saved, passed to `onItemComplete` or listed in `failures`, and `runEvals` rejects once the items already running have finished.
