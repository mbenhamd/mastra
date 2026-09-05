---
'@mastra/convex': patch
---

Fixed workflow state compare-and-set updates to reject mismatched execution generations and lifecycle resume attempts instead of checking only status.

Preserved array-form foreach progress across concurrent completions and prevented pending resets from erasing successful user data shaped like a failure.

Removed completed coordinates from propagated tool-approval suspension maps and prevented stale reset commands from overwriting recovered retry progress.

Accepted explicit suspensions from admitted failed retries without allowing stale sibling suspensions to replace completed results.
