---
'@mastra/quickjs': minor
---

Added a `transpile` option to `QuickJsCodeModeTransport`. It receives the wrapped TypeScript program and returns the JavaScript that QuickJS evaluates, so callers can use their own TypeScript compiler. A throw or rejection is reported as a `SyntaxError` result. Without the option, the transport still erases types with `ts-blank-space`.
