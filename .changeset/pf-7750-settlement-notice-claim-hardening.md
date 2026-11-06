---
'@mastra/core': patch
---

Harness v1: a delegated settlement notice in `wake` mode survives transient storage failures without a second wake.

- **Failed acknowledgement:** an acknowledgement write that fails after the wake ran is retried under the same claim with the reconcile backoff while the session is live and holds the lease, so the same process does not wake the parent again. Each failed attempt is logged. If it keeps failing past the backoff cap, or the lease is lost, the session closes or a Stop lands first, the claim is left dead and an error is logged; the next reconcile pass or lease holder may then wake a second time.
- **Failed claim write:** a claim write that throws (a storage error or a version conflict) leaves the notice owed and is retried with the same backoff in this process. Before, the delivery stopped and the parent was not woken until some unrelated reconcile pass ran.
- **History load across retries:** a run that loads the reminder row between a failed delivery attempt and its retry now counts as the delivery, so the retry does not wake again. The watch on the row lasts until the notice is resolved, the lease is lost, the session starts closing, or a reconcile pass finds the notice no longer owed.
- **Runner:** recording which history rows a run loaded can no longer fail the run. If recording fails, an error is logged and a wake may follow that run (a duplicate).
