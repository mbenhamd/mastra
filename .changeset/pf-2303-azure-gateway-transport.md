---
'@mastra/core': patch
---

Route the OpenAI Responses websocket transport and the `providerOptions` bag selection through gateway capability flags (`transportProviderOptionsKey`, `ownsResponsesWebSocketTransport`) instead of a hardcoded `azure-openai` gateway id. An Azure gateway registered under a custom namespace now keeps its websocket transport and reads `providerOptions.azure` instead of silently downgrading to a plain `fetch` transport against `providerOptions.openai`.
