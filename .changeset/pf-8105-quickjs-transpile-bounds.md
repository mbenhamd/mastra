---
'@mastra/quickjs': patch
---

The `transpile` hook of `QuickJsCodeModeTransport` now runs inside the run's `timeout` and `abortSignal`. A hook that has not settled when either fires yields the usual `TimeoutError` or `AbortError` result instead of holding the run. The option's documentation now states that the hook must return a single JavaScript function expression.
