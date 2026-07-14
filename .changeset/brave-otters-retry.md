---
'@mastra/core': patch
'@mastra/redis-streams': patch
---

Fixed workflow event retry budgets resetting when delivery moves to another worker or process, including Redis Stream deliveries reclaimed after a consumer crash.
