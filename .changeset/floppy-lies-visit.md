---
'@mastra/posthog': patch
---

Fixed missing PostHog group assignment for AI events. Events with `metadata.$groups` now include top-level groups, so analytics can be filtered by group.
