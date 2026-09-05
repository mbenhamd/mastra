# @mastra/ai-sdk

The recommended way of using Mastra and AI SDK together is by installing the `@mastra/ai-sdk` package. `@mastra/ai-sdk` provides custom API routes and utilities for streaming Mastra agents in AI SDK-compatible formats. Including chat, workflow, and network route handlers, along with utilities and exported types for UI integrations.

## Installation

```bash
npm install @mastra/ai-sdk
```

## Usage

If you want to use dynamic agents you can use a path with `:agentId`.

```typescript
import { chatRoute } from '@mastra/ai-sdk';

export const mastra = new Mastra({
  server: {
    apiRoutes: [
      chatRoute({
        path: '/chat/:agentId',
      }),
    ],
  },
});
```

### `handleHarnessChatStream`

Use `handleHarnessChatStream` when your runtime already resolved a Harness v1 session and needs to admit an AI SDK UI chat body through `session.message({ stream: true })` instead of raw `agent.stream()`.

```typescript
import { handleHarnessChatStream } from '@mastra/ai-sdk';
import { createUIMessageStreamResponse, type UIMessage } from 'ai';

export async function POST(req: Request) {
  const params = (await req.json()) as { messages: UIMessage[] };
  const session = await harness.session({ resourceId, threadId });
  const stream = await handleHarnessChatStream({
    session,
    params: {
      messages: params.messages,
      requestContext: { app: { requestId } },
    },
    version: 'v6',
  });

  return createUIMessageStreamResponse({ stream });
}
```

Normal submit requests use the trailing user message ID as the Harness `admissionId`, preserving duplicate detection and durable result semantics. `trigger: "regenerate-message"` intentionally omits `admissionId` so Harness admits a fresh turn instead of replaying the original user message result. Per-turn `additionalTools` also require a fresh Harness turn and are rejected on normal submits because Harness does not allow them with `admissionId`.

Direct callers may only provide `requestContext.app`; Harness-owned context keys such as `harness`, `channel`, or `user` are rejected before admission and are validated again by `session.message()`. Tool approvals, suspensions, and other resume-style human-in-the-loop (HITL) responses are not mapped by this helper; route those through native Harness inbox/session APIs.

## Documentation

- [AI SDK UI integration guide](https://mastra.ai/integrations/agentic-ui/ai-sdk-ui)
- [AI SDK reference](https://mastra.ai/reference/ai-sdk/overview)

## Changelog

See the [package changelog](https://github.com/mastra-ai/mastra/blob/main/client-sdks/ai-sdk/CHANGELOG.md) for version history and release notes.

## Support

We have an [open community Discord](https://discord.gg/mastra-ai). Come and say hello and let us know if you have any questions or need any help getting things running.
