### 13.4l Background-Task Observation

**Background-task observation.** Ordinary Remote Harness SDKs do not expose
background-task list, get, or stream helpers. Client code reads the bounded
`durableWork` projection on `SessionListItem` and `SessionSnapshot`; detailed
task rows stay behind Studio/operator diagnostics because the canonical task
runtime is Mastra's existing BackgroundTasksStorage/manager surface, not a new
Harness client API. Existing generic or admin client helpers that expose
`resourceId`, physical task IDs, or cross-resource streams are not ordinary
Harness client APIs. Unscoped or cross-resource task diagnostics are separate
operator/admin surfaces and require explicit operator authorization.
