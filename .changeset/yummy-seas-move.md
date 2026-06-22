---
'@mastra/server': minor
---

Added a server route selector for registering only selected built-in routes.

Adapters now accept a `routes` option. Leave it out to register all built-in routes, pass a route list to register only those routes, or pass a function to choose routes from `SERVER_ROUTES` in the default order.

```ts
import { SERVER_ROUTES } from '@mastra/server/server-adapter';

const harnessRoutes = SERVER_ROUTES.filter(route => route.path.startsWith('/harness/'));

new MastraServer({
  app,
  mastra,
  routes: harnessRoutes,
});
```
