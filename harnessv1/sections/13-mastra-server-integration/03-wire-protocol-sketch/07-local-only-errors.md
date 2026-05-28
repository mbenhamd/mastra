### 13.3g Local-Only Errors

Not every §4.5 typed Harness error projects to a §13.3f wire code. Some
classes are reachable only inside the host process and therefore have no
public `code` / `details` shape on the discriminated error union above.
Adapters and SDKs must not invent wire codes for them and must not surface
them in HTTP responses, SSE error payloads, result-lookup DTOs, or storage
error events.

The local-only set in v1 is:

- `HarnessConfigError` — a startup-time failure (misconfigured workspace
  provider, missing required field, unresumable provider declared without a
  recovery path, invalid pagination or lease timing relationships, namespace
  conflict between harnesses sharing a storage adapter). It prevents
  `harness.init()` from succeeding and therefore prevents the Mastra Server
  from accepting requests at all. By the time a client could issue an HTTP
  call, the server has already aborted boot; clients see transport-layer
  failure (connection refused, `harness.worker_unavailable`, or upstream
  server error) rather than a typed Harness response.
- `HarnessResourceWorkspaceInUseError` — raised by the explicit
  workspace-admin/operator teardown path when a per-resource workspace still has
  active sessions. It is not a public Harness method and §13.2 does not
  auto-mount it as a client route. No public wire response can emit it; remote
  callers reach the same effective state through normal session lifecycle errors
  (`harness.session_delete_blocked` and `harness.session_closing` for the
  workspace's still-active sessions).

Adding a new local-only error is not a wire change; adding a new typed
Harness error class that can be emitted across an auto-mounted route is, and
must follow the §13.3f stable-code rule for the discriminated union above.
