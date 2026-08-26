import SelectedServerModule = require('@mastra/server/server-adapter/selected');
import SelectedFastifyModule = require('@mastra/fastify/selected');

type ServerRoute = import('@mastra/server/server-adapter/selected').ServerRoute;

const { MastraServer: SelectedServer } = SelectedServerModule;
const { MastraServer: SelectedFastifyServer } = SelectedFastifyModule;

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
