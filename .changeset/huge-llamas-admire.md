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

The default stays `true`, so existing runs still reject on the first failed item. Failed items are left out of `summary.totalItems`, scores and the verdict, so check `failures` before trusting the verdict.
