---
'@mastra/core': patch
---

Fixed agent input that was lost when an idle thread wake lost its thread lease to another process.

- Input sent to a thread while its idle wake was starting was acknowledged as delivered. If that wake then lost the thread lease, the input was deleted and never ran. It is now handed to the lease holder right after the wake, in send order, the same way the wake itself is handed over, and runs once there.
- If that hand-over publish fails, the input is dropped, as before. If there is no lease holder, the input stays queued for the next local run on the thread.
- Acknowledgement timing is unchanged: input is still acknowledged as soon as it is queued.
