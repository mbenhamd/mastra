import { describe, expect, it } from 'vitest';
import { RequestContext } from '../../../../request-context';
import { resolveDurableRequestContext } from './request-context';

describe('resolveDurableRequestContext', () => {
  it('prefers the complete live context over the persisted allowlist', () => {
    const liveRequestContext = new RequestContext();
    liveRequestContext.set('userId', 'user-123');
    liveRequestContext.set('runtimeOnly', () => 'not serializable');

    const resolved = resolveDurableRequestContext(liveRequestContext, {
      persistedRef: 'connection-456',
    });

    expect(resolved.requestContext).toBe(liveRequestContext);
    expect(resolved.customContext).toEqual({
      userId: 'user-123',
      runtimeOnly: expect.any(Function),
    });
    expect(resolved.customContext).not.toHaveProperty('persistedRef');
  });

  it('reconstructs only persisted allowlisted entries when no live context exists', () => {
    const resolved = resolveDurableRequestContext(undefined, {
      persistedRef: 'connection-456',
    });

    expect(Object.fromEntries(resolved.requestContext.entries())).toEqual({
      persistedRef: 'connection-456',
    });
    expect(resolved.customContext).toEqual({ persistedRef: 'connection-456' });
  });
});
