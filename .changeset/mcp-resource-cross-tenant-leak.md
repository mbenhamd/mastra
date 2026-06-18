---
"@mastra/mcp": patch
---

Fixed cross-caller resource leakage in `MCPServer`.

- `resources/list`, `resources/read`, and `listResources()` now resolve resources per request using the current caller context.
- `resources/templates/list` now resolves templates per request using the current caller context.

This prevents one caller's resource names, URIs, or templates from being returned to another caller. See https://github.com/mastra-ai/mastra/issues/17609
