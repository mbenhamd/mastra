import { describe, expect, it, vi } from 'vitest';

import { cleanupSharedTestResources } from './durable-agent.test.utils';

describe('durable-agent shared infrastructure cleanup', () => {
  it('retains only a runtime whose stop failed so cleanup can retry it', async () => {
    const server = { id: 'server' };
    const runtime = { id: 'runtime' };
    const closeServer = vi.fn(async () => undefined);
    const stopRuntime = vi
      .fn<(activeRuntime: typeof runtime) => Promise<void>>()
      .mockRejectedValueOnce(new Error('stop failed'))
      .mockResolvedValueOnce(undefined);

    const first = await cleanupSharedTestResources({ server, runtime, closeServer, stopRuntime });

    expect(first).toEqual({ server: null, runtime, errors: [expect.objectContaining({ message: 'stop failed' })] });
    expect(closeServer).toHaveBeenCalledOnce();
    expect(stopRuntime).toHaveBeenCalledOnce();

    const retry = await cleanupSharedTestResources({ ...first, closeServer, stopRuntime });

    expect(retry).toEqual({ server: null, runtime: null, errors: [] });
    expect(closeServer).toHaveBeenCalledOnce();
    expect(stopRuntime).toHaveBeenCalledTimes(2);
  });

  it('retains only a server whose close failed so cleanup can retry it', async () => {
    const server = { id: 'server' };
    const runtime = { id: 'runtime' };
    const closeServer = vi
      .fn<(activeServer: typeof server) => Promise<void>>()
      .mockRejectedValueOnce(new Error('close failed'))
      .mockResolvedValueOnce(undefined);
    const stopRuntime = vi.fn(async () => undefined);

    const first = await cleanupSharedTestResources({ server, runtime, closeServer, stopRuntime });

    expect(first).toEqual({
      server,
      runtime: null,
      errors: [expect.objectContaining({ message: 'close failed' })],
    });
    expect(closeServer).toHaveBeenCalledOnce();
    expect(stopRuntime).toHaveBeenCalledOnce();

    const retry = await cleanupSharedTestResources({ ...first, closeServer, stopRuntime });

    expect(retry).toEqual({ server: null, runtime: null, errors: [] });
    expect(closeServer).toHaveBeenCalledTimes(2);
    expect(stopRuntime).toHaveBeenCalledOnce();
  });
});
