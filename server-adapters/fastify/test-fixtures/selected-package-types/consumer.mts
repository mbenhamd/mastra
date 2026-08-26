import { MastraServer as SelectedServer } from '@mastra/server/server-adapter/selected';
import type { ServerRoute } from '@mastra/server/server-adapter/selected';
import { MastraServer as SelectedFastifyServer } from '@mastra/fastify/selected';

type SelectedServerOptions = ConstructorParameters<typeof SelectedServer>[0];
type SelectedFastifyOptions = ConstructorParameters<typeof SelectedFastifyServer>[0];

declare const serverApp: SelectedServerOptions['app'];
declare const fastifyApp: SelectedFastifyOptions['app'];
declare const mastra: SelectedServerOptions['mastra'];
declare const routeRegistry: readonly ServerRoute[];

const ConcreteSelectedServer = SelectedServer as unknown as new (options: SelectedServerOptions) => object;

new ConcreteSelectedServer({ app: serverApp, mastra, routeRegistry });
new SelectedFastifyServer({ app: fastifyApp, mastra, routeRegistry });

// @ts-expect-error The selected server subpath requires an explicit route registry.
new ConcreteSelectedServer({ app: serverApp, mastra });
// @ts-expect-error The selected Fastify subpath requires an explicit route registry.
new SelectedFastifyServer({ app: fastifyApp, mastra });
