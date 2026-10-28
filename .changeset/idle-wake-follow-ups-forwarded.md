---
'@mastra/core': patch
---

Fixed agent input that was lost when an idle thread wake lost its thread lease to another process.

- Input sent to a thread while its idle wake was starting was acknowledged as delivered. If that wake then lost the thread lease, the input was deleted and never ran. It is now handed to the lease holder right after the wake, in send order, the same way the wake itself is handed over, and runs once there.
- On the direct and lease-transfer wake routes, and whenever the wake's own publish fails, input whose hand-over publish fails is dropped, as before.
- On the queued idle wake route, a failed hand-over publish puts that input back in the queue for the next local run on the thread. If the publish failed after it had already delivered, the input can run twice: once at the lease holder and once locally.
- If there is no lease holder, the input stays queued for the next local run on the thread.
- Acknowledgement timing is unchanged: input is still acknowledged as soon as it is queued.
