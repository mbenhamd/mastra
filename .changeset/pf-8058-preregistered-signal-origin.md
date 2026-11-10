---
'@mastra/core': patch
---

Fixed `signal()` results that never settled after a crash. If the process stopped while a `signal()` turn was waiting on a question, plan approval or sandbox-access request, answering that request after restart now completes the original `signal()` result.
