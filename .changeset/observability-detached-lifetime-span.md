---
'@mastra/core': patch
'@mastra/observability': patch
---

Added a `detachedLifetime` child-span option for work that legitimately outlives its parent span, such as a background run started by a tool call. The span is part of the parent's trace and inherits its sampling decision, trace state and metadata, but the parent's `endTree()` never ends it. Its owner must end it.

```typescript
const background = toolSpan.createChildSpan({
  type: SpanType.GENERIC,
  name: 'background work',
  detachedLifetime: true,
});
try {
  await runBackgroundWork({ tracingContext: { currentSpan: background } });
  background.end({ output: { status: 'done' } });
} catch (error) {
  background.error({ error: error as Error, endSpan: true });
}
```
