## 5. Session persistence

**Entity ownership:** Storage (Harness logbook), Session (`SessionRecord`),
Thread (transcript rows), Memory (`MemoryStorage` view), and existing
Mastra storage domains where they remain canonical — [§0 Mental model](../00-mental-model.md).

Sessions are durable. The runtime `Session` object is a hydrated cache of a
`SessionRecord` row stored through a new namespace-bound `harness` storage
domain composed with existing `MastraStorage` domains. The harness domain owns
session, admission, result, claim, wakeup, channel-work, and attachment records;
it does not replace the existing memory conversation log, channel
installation/config rows, workflow schedule definitions, or generic background
task storage unless a §5 subsection explicitly says so.

This makes the Harness usable in three deployment shapes without changing the surface:

- **Single-user TUI** — one process, one user, sessions resume across restarts.
- **Multi-tenant server** — many users, many concurrent sessions; clients hold a session ID and reconnect across requests.
- **Mobile/web with intermittent connectivity** — phone disconnects, server flushes, laptop picks up where the phone left off.
