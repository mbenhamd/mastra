---
'@mastra/pg': patch
---

Fixed Harness session leases in the Postgres store being judged by each process's own clock. Leases are now stamped and checked with the database clock everywhere the store uses them (acquiring, renewing, saving and creating sessions, plan-task writes, and execution export), so processes with drifting clocks agree on when a lease has expired. The Postgres store also supports recovering admitted `message()` turns whose process stopped mid-run.
