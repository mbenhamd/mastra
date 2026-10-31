---
'@mastra/core': patch
---

Fixed text held by a processor that runs another processor inside its own `processOutputStream` being released unchecked when a model stream ends. Only the processor that holds parts now releases them, so an outer processor's check still decides what is published. Top-level processors that hold parts (`BatchPartsProcessor`, stream buffers) still release them at the end of each model stream, and a failed or aborted stream still clears every hold, nested ones included.

A processor that wraps a `BatchPartsProcessor` only to delegate to it, or a `BatchPartsProcessor` subclass that overrides `processOutputStream`, no longer gets that end-of-stream release: it publishes the batch when it passes on the turn's `finish`, as before the end-of-stream release was added.
