---
'@mastra/core': patch
---

Fixed an evented workflow restart bringing back a run that was deleted after the restart started. The restarted run's first saved state is now written only if the run is still there under the restart's claim, so a run deleted in the meantime stays deleted. A workflow's first start still saves its run as before.
