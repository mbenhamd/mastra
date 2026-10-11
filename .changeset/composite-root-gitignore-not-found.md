---
'@mastra/core': patch
---

`list_files` and `grep` now work in a multi-mount `CompositeFilesystem` workspace that has no root `.gitignore`. Previously both tools failed with `No mount for path: .gitignore`.
