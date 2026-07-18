// Symbol used to inject pubsub into step context without exposing it in the public API.
// Steps can access pubsub via context[PUBSUB_SYMBOL] for internal event publishing.
export const PUBSUB_SYMBOL = Symbol('pubsub');

// Symbol used to pass stream format preferences through step context.
export const STREAM_FORMAT_SYMBOL = Symbol('stream_format');

// Symbol used to propagate process-local execution into nested workflow runs.
// It never crosses a storage or transport boundary.
export const TRANSIENT_EXECUTION_SYMBOL = Symbol('transient_execution');

// Symbol used by ProcessorRunner to request process-local execution while
// preserving collision checks for explicit or custom-generated run IDs.
// Unlike TRANSIENT_EXECUTION_SYMBOL, this does not force an inherited nested
// run ID to be transient.
export const PROCESSOR_EXECUTION_SYMBOL = Symbol('processor_execution');

// Symbol used to identify results from nested workflow execution.
//
// When a workflow contains another workflow as a step, the inner workflow's execute()
// returns a result wrapped with this symbol. The step handler (handlers/step.ts) checks
// for this symbol to detect nested workflow results and handle them specially - extracting
// the actual result and nested runId for proper state management.
//
// This Symbol is safe to use (unlike PendingMarker) because it stays in-memory within
// a single execution context - it's never serialized to storage or passed between
// distributed engine instances.
export const NESTED_WORKFLOW_RESULT_SYMBOL = Symbol('nested_workflow_result');
