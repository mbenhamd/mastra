---
'@mastra/codemod': patch
---

Run codemods reliably when project paths or runner options contain spaces or special characters. Verbose mode now uses the supported level syntax, and custom runner options can be passed safely by repeating `--jscodeshift`, for example `--jscodeshift=--run-in-band --jscodeshift=--extensions=ts,tsx`.
