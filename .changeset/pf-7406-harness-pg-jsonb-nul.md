---
'@mastra/pg': patch
---

Fixed Harness storage on PostgreSQL failing a turn when a stored value contains a NUL character or an unpaired surrogate, for example a tool result whose text carries `\u0000`. jsonb refuses both (`22P05 unsupported Unicode escape sequence` and `22P02`), so the message result write rejected and the agent turn failed.

Every Harness jsonb write now serializes through the same `toPgJson` the other PostgreSQL domains use: NUL characters are dropped and unpaired surrogates become U+FFFD, and the stored value stays valid JSON. Replays of the same input compare equal to the stored row. Text columns that already accept these escapes, such as the terminal handoff JSON, are unchanged.
