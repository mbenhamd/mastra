## 13. Mastra Server integration

**Entity ownership:** Harness front desk projecting Session, Storage, and Events
— implement after §5/§4 per [§0 cutover order](../00-mental-model.md).

This section describes the proposed Harness v1 Mastra Server surface. It is not
a claim that the current `@mastra/core` runtime already exposes `harness`
config, `mastra.getHarness(...)`, or `/harness/*` routes.

Harness v1 server ownership routing is a required new runtime capability, not a
property of current Mastra Server routes or workers. Before §13.2 signal, queue,
inbox, and channel routes can be mounted in a multi-process deployment, the
server/runtime layer must provide a session-owner directory or equivalent
forwarding path keyed by the active `SessionRecord` lease owner. A non-owner
route handler either forwards the request to that owner before durable
acceptance, or fails before admission with the typed locked/worker-unavailable
errors defined in §4.5 and §13.3. It must not acquire a second session owner,
write through a stale owner, or treat generic background-task workers as
session-owner routers.

A `Harness` is registered on a `Mastra` instance the same way agents and
workflows are. The server auto-mounts a stable HTTP surface, and consumers can
talk to the harness either in-process (via `mastra.getHarness(...)`) or remotely
(via the client SDK). Code written against `RemoteSafeSession` can stay portable
across in-process and remote deployments.

§13 is the server/SDK adapter layer for Harness v1. Route authentication,
principal authorization, HTTP wire error envelopes, SSE
authentication/replay/failure behavior, scoped event subscription tokens, SDK
retry/recovery behavior, and deployment lifecycle live here because they depend
on HTTP transport, server topology, or client SDK composition. They are not
additions to the core in-process `Session` contract (§2, §4), the
tool-authoring contract (§6), or the event/replay contract (§10); those sections
define the in-process behavior that §13 projects across the wire.

For error handling, §4.5 owns typed Harness error classes and shared detail
fields; §13.3 owns wire codes, public `details` shapes, status-family/default
mapping guidance, generic server-layer codes, and SDK rehydration. §13.2 route
rows may list the route-specific emitted status/code pairs, but must not
redefine error detail shapes, retryability, or SDK reaction behavior. §13.4 may
describe SDK reactions to §4.5 classes and §13.3 wire codes, but must not
introduce new error codes or detail shapes.
