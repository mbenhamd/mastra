---
'@mastra/ai-sdk': patch
---

Fixed the AI SDK v6 native approval flow in `handleChatStream` so multiple exact approval responses on the trailing assistant message resume sequentially in one framed response stream. Unsafe earlier-message-only, malformed, and ambiguous approval responses now fail closed instead of resuming the wrong UI message or falling through to a normal agent stream.
