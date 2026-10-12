---
'@mastra/libsql': patch
---

**Fixed:** A turn that finishes after its Harness session was deleted no longer saves its result in LibSQL. The late result is dropped instead of bringing back data the delete removed.
