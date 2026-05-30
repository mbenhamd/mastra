import { describe, expect, it } from 'vitest';

import { HarnessValidationError } from './errors';
import { validateCallerRequestContext } from './request-context-input';

const M = 'message';

describe('validateCallerRequestContext — accepted shapes', () => {
  it('returns undefined when no request context is supplied', () => {
    expect(validateCallerRequestContext(undefined, M)).toBeUndefined();
  });

  it('returns undefined for an empty object or an omitted app', () => {
    expect(validateCallerRequestContext({}, M)).toBeUndefined();
    expect(validateCallerRequestContext({ app: undefined }, M)).toBeUndefined();
  });

  it('preserves an explicit empty app bag as data', () => {
    expect(validateCallerRequestContext({ app: {} }, M)).toEqual({ app: {} });
  });

  it('returns the normalized app bag for valid JSON content', () => {
    expect(validateCallerRequestContext({ app: { tenant: 't1', nested: { n: [1, 2] }, flag: true, z: null } }, M)).toEqual(
      { app: { tenant: 't1', nested: { n: [1, 2] }, flag: true, z: null } },
    );
  });

  it('drops undefined app properties (JSON.stringify semantics) but keeps explicit null', () => {
    expect(validateCallerRequestContext({ app: { a: 1, b: undefined, c: null } }, M)).toEqual({ app: { a: 1, c: null } });
  });
});

describe('validateCallerRequestContext — reserved key rejection', () => {
  it.each([
    'harness',
    'channel',
    'MastraMemory',
    'browser',
    'user',
    'userPermissions',
    'userRoles',
  ])('rejects the infrastructure-owned key %s', key => {
    expect(() => validateCallerRequestContext({ [key]: {} }, M)).toThrow(HarnessValidationError);
    expect(() => validateCallerRequestContext({ [key]: {} }, M)).toThrow(/infrastructure-owned/);
  });

  it('rejects mastra__* and __mastra* namespaced keys', () => {
    expect(() => validateCallerRequestContext({ mastra__internal: 1 }, M)).toThrow(/infrastructure-owned/);
    expect(() => validateCallerRequestContext({ __mastraState: 1 }, M)).toThrow(/infrastructure-owned/);
  });

  it('rejects any other unknown top-level key', () => {
    expect(() => validateCallerRequestContext({ foo: 1 }, M)).toThrow(/callers may only supply "app"/);
  });

  it('rejects non-enumerable and symbol top-level keys (Reflect.ownKeys strictness)', () => {
    const nonEnumerable = {};
    Object.defineProperty(nonEnumerable, 'channel', { value: {}, enumerable: false });
    expect(() => validateCallerRequestContext(nonEnumerable, M)).toThrow(HarnessValidationError);

    const symbolKeyed = { [Symbol('harness')]: {} };
    expect(() => validateCallerRequestContext(symbolKeyed, M)).toThrow(/callers may only supply "app"/);
  });

  it('rejects a reserved key even when a valid app is also present', () => {
    expect(() => validateCallerRequestContext({ app: { a: 1 }, harness: {} }, M)).toThrow(/infrastructure-owned/);
  });

  it('roots the validation path at <method>.requestContext', () => {
    expect(() => validateCallerRequestContext({ channel: {} }, 'queue')).toThrow(/queue\.requestContext\.channel/);
  });
});

describe('validateCallerRequestContext — malformed input rejection', () => {
  it.each([
    ['a string', 'x'],
    ['a number', 42],
    ['null', null],
    ['an array', [{ app: {} }]],
  ])('rejects %s as the request context', (_label, value) => {
    expect(() => validateCallerRequestContext(value, M)).toThrow(HarnessValidationError);
  });

  it.each([
    ['a string', 'x'],
    ['a number', 1],
    ['null', null],
    ['an array', [1, 2]],
  ])('rejects a non-object app value (%s)', (_label, value) => {
    expect(() => validateCallerRequestContext({ app: value }, M)).toThrow(/\.app/);
  });

  it('rejects an app containing a non-JSON value', () => {
    expect(() => validateCallerRequestContext({ app: { fn: () => undefined } }, M)).toThrow(HarnessValidationError);
  });

  it('rejects an app containing negative zero (canonicalization profile)', () => {
    expect(() => validateCallerRequestContext({ app: { z: -0 } }, M)).toThrow(HarnessValidationError);
  });
});
