---
'@mastra/core': patch
---

Fixed workflow lifecycle fencing so expected generations and resume attempts reject malformed or stale snapshot state, and queued foreach entries can be reset correctly. Fixed workflow processor spans without shared runtime state, bound durable aborts to the execution identity known at call time, and removed an unused MCP SDK dependency.

Preserved canceled-step text for output processors when a late abort closes the step before final response assembly, without replaying earlier completed steps.

Preserved authenticated tool-resume approval decisions without calling the approval-request policy again, while continuing to check current execution permissions.

Preserved the originating agent subscription when a resumed tool suspends again after a mode switch, so later approval can complete the same run without aborting it.

Routed subsequent tool approvals and declines through that originating agent, including the decline used by explicit cancellation after a mode switch.

Preserved array-form foreach iteration progress across concurrent sibling completions, and used engine-owned progress to distinguish failed iterations from successful user data when applying pending resets.

Preserved each remaining tool approval's resume state after a sibling completes, and prevented stale retry progress from overwriting completed foreach results. Successful user outputs that contain a queue-marker field are no longer executed again.

Made default sleep-step identifiers deterministic across workflow replicas so identical scheduled workflows agree on their definition hash, while preserving explicit identifiers and custom identifier generators.

Allowed failed foreach retries to suspend again and preserved queued retry execution when the resumed workflow lowers its concurrency, while retaining completed sibling results and current-attempt failures.
