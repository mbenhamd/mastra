---
'@mastra/core': patch
---

Reduced idle Harness storage traffic by exponentially backing off durable pending-interaction expiry sweeps to one minute. Discovering due work resets the sweep to one second so overdue backlogs still drain promptly after restart.
