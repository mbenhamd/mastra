---
'@mastra/core': patch
---

`list_files` and `grep` work again on a workspace whose filesystem is a `CompositeFilesystem`.

Both tools read `.gitignore` from the filesystem root before walking, and only treat a missing file (`ENOENT`) as "no gitignore". A composite has no filesystem at its root, so its `readFile` threw a plain `No mount for path: .gitignore` error, and both tools failed on every multi-mount workspace. `CompositeFilesystem.readFile` now reports a path that no mount holds as `ENOENT` (`exists()` already returned `false` for it). The message is unchanged. Real read failures inside a mount still surface.
