import { Mastra } from '@mastra/core/mastra';
import { describe, expect, it, vi } from 'vitest';

import { MASTRA_RESOURCE_ID_KEY, MASTRA_THREAD_ID_KEY } from '../constants';
import { MastraServer } from './index';

class TestMastraServer extends MastraServer<any, any, any> {
  stream = vi.fn();
  getParams = vi.fn();
  sendResponse = vi.fn();
  registerRoute = vi.fn();
  registerContextMiddleware = vi.fn();
  registerAuthMiddleware = vi.fn();
  registerHttpLoggingMiddleware = vi.fn();

  mergeContextForTest(args: Parameters<MastraServer<any, any, any>['mergeRequestContext']>[0]) {
    return this.mergeRequestContext(args);
  }
}

function createServer() {
  return new TestMastraServer({ app: {}, mastra: new Mastra({}) });
}

describe('server request context merge', () => {
  it('filters reserved context keys from client-provided body and query context', () => {
    const server = createServer();

    const requestContext = server.mergeContextForTest({
      bodyRequestContext: {
        [MASTRA_RESOURCE_ID_KEY]: 'body-resource',
        [MASTRA_THREAD_ID_KEY]: 'body-thread',
        'mastra.flow': { capabilities: ['root-forged'] },
        'mastra.flow.state': { capabilities: ['forged'] },
        'mastra.flow.policy': { allowedTools: ['forged-tool'] },
        'app.locale': 'en',
      },
      paramsRequestContext: {
        [MASTRA_RESOURCE_ID_KEY]: 'query-resource',
        [MASTRA_THREAD_ID_KEY]: 'query-thread',
        'mastra.flow.state': { capabilities: ['query-forged'] },
        'app.locale': 'fr',
      },
    });

    expect(requestContext.get(MASTRA_RESOURCE_ID_KEY)).toBeUndefined();
    expect(requestContext.get(MASTRA_THREAD_ID_KEY)).toBeUndefined();
    expect(requestContext.get('mastra.flow')).toBeUndefined();
    expect(requestContext.get('mastra.flow.state')).toBeUndefined();
    expect(requestContext.get('mastra.flow.policy')).toBeUndefined();
    expect(requestContext.get('app.locale')).toBe('fr');
  });

  it('only reserves the mastra.flow namespace segment', () => {
    const server = createServer();

    const requestContext = server.mergeContextForTest({
      bodyRequestContext: {
        'mastra.flowState': 'app-owned',
      },
    });

    expect(requestContext.get('mastra.flowState')).toBe('app-owned');
  });
});
