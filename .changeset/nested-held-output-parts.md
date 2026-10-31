---
'@mastra/core': patch
---

Fixed text held by a processor that runs another processor inside its own `processOutputStream` being released unchecked when a model stream ends. Only the processor that holds parts now releases them, so an outer processor's check still decides what is published. Top-level processors that hold parts (`BatchPartsProcessor`, stream buffers) still release them at the end of each model stream.
