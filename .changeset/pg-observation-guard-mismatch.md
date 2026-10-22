---
'@mastra/pg': patch
---

Fixed the PostgreSQL store reporting an observational memory write for another resource or thread as an outdated generation. Such a write now fails with the same "guard does not match" error as the libSQL and in-memory stores, including for a thread-scoped record that is still current.
