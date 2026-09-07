---
'@mastra/core': patch
---

Fixed duplicate workflow resumes when running snapshots are disabled. Competing default and evented `resume()` calls now fail before executing steps on storage that supports concurrent updates.
