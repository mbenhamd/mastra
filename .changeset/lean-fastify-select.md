---
'@mastra/fastify': minor
---

Added a selected Fastify entrypoint for applications that expose an explicit set of Mastra routes.

```ts
import Fastify from 'fastify';
import { MastraServer } from '@mastra/fastify/selected';
import { HARNESS_SESSION_CONTROL_ROUTES } from '@mastra/server/server-adapter/routes/harness';
import { mastra } from './mastra';

const server = new MastraServer({
  app: Fastify(),
  mastra,
  routeRegistry: HARNESS_SESSION_CONTROL_ROUTES,
});
```
