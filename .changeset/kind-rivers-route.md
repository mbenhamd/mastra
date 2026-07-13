---
'@mastra/core': patch
---

Fixed scheduled workflows and their public event streams so they keep working across processes.

Improved internal workflow isolation and prevented internal-only events from entering shared replay history.
