---
'@mastra/core': minor
---

Enterprise Edition (EE) now automatically captures PostHog telemetry for license checks and feature usage, including license validation status, role-based access control (RBAC) resolution, fine-grained authorization (FGA) calls, and Enterprise feature invocation metadata. Telemetry is enabled by default for Enterprise customers and can be disabled with `MASTRA_TELEMETRY_DISABLED=1`. Community edition users are unaffected.
