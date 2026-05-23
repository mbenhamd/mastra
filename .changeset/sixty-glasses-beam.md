---
'@mastra/posthog': minor
---

Export Mastra operational logs to PostHog for unified observability. Log export is disabled by default so existing tracing setups do not start sending new events unless users opt in.

Enable log export with defaults:

```ts
new PosthogExporter({
  apiKey: process.env.POSTHOG_API_KEY,
  logs: true,
});
```

Customize log export:

```ts
new PosthogExporter({
  apiKey: process.env.POSTHOG_API_KEY,
  logs: {
    eventName: 'mastra_log',
    minLevel: 'warn',
    distinctId: event => event.log.correlationContext?.userId,
    captureExceptions: true,
    dedupe: true,
  },
});
```

Log events include trace/span correlation, user/session identifiers, structured log data, and optional Error Tracking fanout for error and fatal logs. Privacy mode redacts freeform log fields while preserving structural correlation properties.
