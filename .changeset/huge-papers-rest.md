---
'@mastra/memory': patch
---

Fixed an issue where tool results containing media content blocks (returned via `toModelOutput`) were stringified into the observational memory prompt as raw base64 text. The base64 data overflowed the observer's context, causing token-limit errors and degenerate output.

Image and file blocks (`image`, `image-data`, `image-url`, `file`, `file-data`, `file-url`, and `media`) inside tool results are now hoisted into the observer's input as proper attachments, the same way image and file message parts already are. The text body shows a placeholder like `[Image #1: image/png]` so the observer keeps positional context without seeing the bytes.
