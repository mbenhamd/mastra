---
'@mastra/server': minor
---

Added explicit server route selection so adapter authors can expose only the route domains their applications use while retaining canonical registry order. The Harness route export includes a fixed three-route session-control allowlist.

```ts
import { MastraServer, type SelectedMastraServerOptions } from '@mastra/server/server-adapter/selected';
import { HARNESS_ROUTES, HARNESS_SESSION_CONTROL_ROUTES } from '@mastra/server/server-adapter/routes/harness';

type HarnessAdapterOptions<TApp> = Omit<SelectedMastraServerOptions<TApp>, 'routeRegistry' | 'routes'>;

abstract class HarnessServerAdapter<TApp, TRequest, TResponse> extends MastraServer<TApp, TRequest, TResponse> {
  constructor(options: HarnessAdapterOptions<TApp>) {
    super({
      ...options,
      routeRegistry: HARNESS_ROUTES,
      routes: HARNESS_SESSION_CONTROL_ROUTES,
    });
  }
}
```
