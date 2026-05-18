---
'@mastra/convex': patch
---

Convex workflow storage now returns error objects when it receives invalid workflow JSON, so callers get a normal failed storage response instead of an uncaught mutation error.
