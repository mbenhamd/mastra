---
'@mastra/core': patch
'@mastra/inngest': patch
---

Durable and Inngest agent tool calls now enforce live per-tool permission policy at the action boundary and again after approval or in-tool suspension resume. Runs persist only a policy-required marker; a cold worker that cannot reconstruct the authoritative evaluator denies execution instead of treating a missing function as allow. Inngest agents can configure a trusted worker-local `resolveToolPermission` callback, while `resume()` accepts fresh request context without ever serializing its policy closure. If an in-tool resume newly resolves to `ask`, it fails closed unless the same call carries its prior approval grant, because a second approval stop cannot discard the opaque resume payload.

Durable request-context persistence is now explicit-allowlist only, bounded, and rejects credential-like keys. Inngest durable-agent resume rebuilds that allowlist from fresh context and sanitizes both the active snapshot and rollback checkpoint. Policy-bearing foreach calls stay sequential unless a live immutable per-turn policy snapshot allows every emitted call; each tool still re-evaluates authorization at its side-effect boundary. Generic workflow resume keeps its existing merge behavior.

Inngest durable agents now use protocol-v2 function IDs so pre-policy workers cannot claim new authorization-aware events. Keep the prior worker deployment available until protocol-v1 runs drain; v2 workers intentionally do not resume v1 snapshots under a reused function identity.
