---
'@mastra/core': minor
---

Improved reliability when nested workflows return results to their parent after finishing or suspending. Parent request context is now normalized consistently across storage adapters, and malformed or oversized continuation data is rejected within explicit traversal limits. This change does not modify the public API.
