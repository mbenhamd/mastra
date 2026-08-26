import { describe, expect, it, vi } from 'vitest';

vi.mock('./routes', () => {
  throw new Error('selected server adapter loaded the global SERVER_ROUTES registry');
});

describe('selected server adapter import closure', () => {
  it('does not import the global built-in route registry', async () => {
    const selectedAdapter = await import('./selected');

    expect(selectedAdapter.MastraServer).toBeTypeOf('function');
  });
});
