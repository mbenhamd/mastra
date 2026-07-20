---
'@mastra/core': patch
---

Fixed delegated agents that inherit memory so their history, working-memory, and output processors run without mutating the child agent, and so generate and stream project their complete response transcript under the delegation thread without moving child-owned memory records. Delegation bail signals now reach the parent loop across isolated request contexts.

Provider-emitted null resume placeholders now defer to explicit workflow approval or suspension data only when no model-supplied resume identity is present. Identity-bearing resumes remain fail-closed.
