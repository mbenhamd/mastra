import { describe, expect, it, vi } from 'vitest';

vi.mock('@mastra/server/server-adapter', () => {
  throw new Error('selected Fastify adapter loaded the global SERVER_ROUTES registry');
});

describe('selected Fastify adapter import closure', () => {
  it('imports the registry-free server adapter entrypoint', async () => {
    const selectedAdapter = await import('../selected');

    expect(selectedAdapter.MastraServer).toBeTypeOf('function');
  });
});
