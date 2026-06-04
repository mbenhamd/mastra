## 12. Usage examples

**Entity ownership:** illustrative only — [§0 Mental model](../00-mental-model.md)
and owning §§2–5, 10, 13, 14 remain authoritative.

The remaining sections are concrete walkthroughs of the v1 API. They are
grouped by application shape (single-user TUI, multi-tenant server, headless
script, subagent author).

These examples are illustrative, not authoritative. Canonical behavior for
resource scoping, concurrency, admission, persistence, events, channels, route
boundaries, API signatures, response shapes, stream types, and error contracts
lives in §§2.3, 3, 4, 5, 10, 13, and 14. If example prose or comments appear to
conflict with those sections, the canonical section wins. Examples must not show
removed thread-first Harness methods (`switchThread`, `sendMessage`, `followUp`,
`harness.memory.*`, or bare thread create/rename helpers) as the normal product
path; use `harness.session(...)` and `Session` methods instead (§0, §11.4).
