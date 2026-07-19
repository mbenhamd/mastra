---
'@mastra/core': patch
---

Fixed durable agents to reject processor-created or mutated executable tools before the model request, including late mutations triggered while processor-returned tool-selection controls are read. Per-step processors can still remove tools, while agent tool hooks remain the supported way to run logic around calls. Tool surface fences now restore protected mutable configuration stored in maps, sets, dates, regular expressions, and binary buffers, and fail closed for opaque internal-slot state without an explicit restoration policy.
