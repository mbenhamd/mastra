---
'@mastra/core': patch
---

Fixed `Harness.shutdown()` failing forever when another harness had adopted one of its sessions.

Before, the shutdown save of an adopted session failed with `HarnessSessionLockedError`, or hung until the drain deadline (30 seconds by default) when the session was checkpointing an assistant draft. Shutdown then restored the harness to live, restarted its lease-renewal, channel-recovery and pending-interaction sweep loops, and threw. The harness could never stop, and its loops kept running against storage the caller then closed.

Now a session fenced by another owner's lease, including a child fenced together with its adopted parent, counts as evicted: shutdown skips its remaining drain work, does not restore it, does not release the new owner's lease, and completes once every other session has drained. Idle and pressure evictions still wait for an in-flight assistant-draft checkpoint. Any other storage failure still restores the remaining sessions, restarts the loops and throws, as before. When one session is fenced and another fails, the fenced one is evicted and shutdown throws for the other.
