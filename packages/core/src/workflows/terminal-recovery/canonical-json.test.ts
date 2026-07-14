import { describe, expect, it, vi } from 'vitest';
import {
  MAX_WORKFLOW_TERMINAL_RECOVERY_ENVELOPE_BYTES,
  MAX_WORKFLOW_TERMINAL_RECOVERY_ERROR_STACK_BYTES,
  MAX_WORKFLOW_TERMINAL_RECOVERY_VALUE_DEPTH,
  getWorkflowTerminalCanonicalJson,
  materializeWorkflowTerminalCanonicalJson,
  materializeWorkflowTerminalCanonicalJsonObjectPatch,
} from './canonical-json';

describe('workflow terminal recovery canonical JSON', () => {
  it('sorts data keys and applies explicit JSON presence semantics', () => {
    const input = JSON.parse('{"__proto__":{"safe":true},"z":-0,"a":[null,null]}');
    input.a[0] = undefined;
    input.omitted = undefined;
    input.date = new Date('2026-01-01T00:00:00.000Z');

    const canonical = materializeWorkflowTerminalCanonicalJson(input);
    expect(canonical).toEqual(
      JSON.parse('{"__proto__":{"safe":true},"a":[null,null],"date":"2026-01-01T00:00:00.000Z","z":0}'),
    );
    expect(Object.hasOwn(canonical as object, '__proto__')).toBe(true);
    expect(Object.getPrototypeOf(canonical)).toBe(Object.prototype);
    expect(getWorkflowTerminalCanonicalJson(canonical)).toBe(
      '{"__proto__":{"safe":true},"a":[null,null],"date":"2026-01-01T00:00:00.000Z","z":0}',
    );
    expect(materializeWorkflowTerminalCanonicalJsonObjectPatch(undefined)).toEqual({});
    expect(() => materializeWorkflowTerminalCanonicalJsonObjectPatch(null)).toThrow(/must be an object/);
  });

  it('accepts null-prototype records but rejects custom prototypes', () => {
    const record = Object.create(null) as Record<string, unknown>;
    record.value = true;
    expect(materializeWorkflowTerminalCanonicalJson(record)).toEqual({ value: true });
    expect(() => materializeWorkflowTerminalCanonicalJson(Object.create({ inherited: true }))).toThrow(
      /custom prototypes/,
    );
  });

  it.each([
    Number.NaN,
    Number.POSITIVE_INFINITY,
    Number.NEGATIVE_INFINITY,
    1n,
    Symbol('value'),
    () => undefined,
    new Map([['value', 1]]),
    new Set([1]),
    /value/,
    new URL('https://example.com'),
    new Uint8Array([1]),
    new Date('invalid'),
  ])('rejects non-canonical value %#', value => {
    expect(() => materializeWorkflowTerminalCanonicalJson({ value })).toThrow(
      /Invalid workflow terminal recovery data/,
    );
  });

  it('rejects cycles, sparse arrays, array extensions, symbols, and non-enumerable fields', () => {
    const cycle: Record<string, unknown> = {};
    cycle.self = cycle;
    expect(() => materializeWorkflowTerminalCanonicalJson(cycle)).toThrow(/cycle/);

    expect(() => materializeWorkflowTerminalCanonicalJson(new Array(1))).toThrow(/dense/);
    const extended = [1] as number[] & { extra?: boolean };
    extended.extra = true;
    expect(() => materializeWorkflowTerminalCanonicalJson(extended)).toThrow(/indexed data/);

    expect(() => materializeWorkflowTerminalCanonicalJson({ [Symbol('key')]: true })).toThrow(/symbol keys/);
    const hidden = {};
    Object.defineProperty(hidden, 'value', { value: true });
    expect(() => materializeWorkflowTerminalCanonicalJson(hidden)).toThrow(/non-enumerable/);
  });

  it('never invokes getters, toJSON, or proxy traps', () => {
    const getter = vi.fn(() => 'secret');
    const accessor = {};
    Object.defineProperty(accessor, 'value', { enumerable: true, get: getter });
    expect(() => materializeWorkflowTerminalCanonicalJson(accessor)).toThrow(/accessors/);
    expect(getter).not.toHaveBeenCalled();

    const toJSON = vi.fn(() => ({ changed: true }));
    expect(() => materializeWorkflowTerminalCanonicalJson({ toJSON })).toThrow(/non-JSON/);
    expect(toJSON).not.toHaveBeenCalled();

    const trap = vi.fn(() => ({ value: { enumerable: true, configurable: true, value: true } }));
    const proxy = new Proxy({}, { getOwnPropertyDescriptor: trap });
    expect(() => materializeWorkflowTerminalCanonicalJson(proxy)).toThrow(/proxies/);
    expect(trap).not.toHaveBeenCalled();

    const prototypeTrap = vi.fn(() => null);
    const proxyPrototype = new Proxy({}, { getPrototypeOf: prototypeTrap });
    const inheritedProxy = Object.create(proxyPrototype);
    expect(() => materializeWorkflowTerminalCanonicalJson({ nested: inheritedProxy })).toThrow(/custom prototypes/);
    expect(prototypeTrap).not.toHaveBeenCalled();
  });

  it('serializes native Error diagnostics from data descriptors only', () => {
    const cause = Object.assign(new Error('cause'), { code: 'E_CAUSE' });
    const error = Object.assign(new Error('boom', { cause }), {
      code: 'E_CHILD',
      detail: { at: new Date('2026-01-01T00:00:00.000Z'), missing: undefined },
    });
    const canonical = materializeWorkflowTerminalCanonicalJson({ error }) as Record<string, any>;
    expect(canonical.error).toMatchObject({
      name: 'Error',
      message: 'boom',
      code: 'E_CHILD',
      cause: { name: 'Error', message: 'cause', code: 'E_CAUSE' },
      detail: { at: '2026-01-01T00:00:00.000Z' },
    });

    const stackGetter = vi.fn(() => 'must-not-run');
    Object.defineProperty(error, 'stack', { configurable: true, get: stackGetter });
    expect(materializeWorkflowTerminalCanonicalJson({ error })).toMatchObject({
      error: { name: 'Error', message: 'boom', code: 'E_CHILD' },
    });
    expect(stackGetter).not.toHaveBeenCalled();

    const customGetter = vi.fn(() => 'must-not-run');
    Object.defineProperty(error, 'custom', { enumerable: true, get: customGetter });
    expect(() => materializeWorkflowTerminalCanonicalJson({ error })).toThrow(/accessors/);
    expect(customGetter).not.toHaveBeenCalled();
  });

  it('serializes bounded AggregateError members from Promise.any failures', async () => {
    const aggregate = await Promise.any([
      Promise.reject(new Error('first')),
      Promise.reject(Object.assign(new Error('second'), { code: 'E_SECOND' })),
    ]).catch((error: unknown) => error);
    expect(aggregate).toBeInstanceOf(AggregateError);
    const canonical = materializeWorkflowTerminalCanonicalJson({ error: aggregate }) as Record<string, any>;
    expect(canonical.error).toMatchObject({
      name: 'AggregateError',
      errors: [
        { name: 'Error', message: 'first' },
        { name: 'Error', message: 'second', code: 'E_SECOND' },
      ],
    });

    const sparse = new AggregateError([]);
    Object.defineProperty(sparse, 'errors', { configurable: true, value: new Array(1) });
    expect(() => materializeWorkflowTerminalCanonicalJson({ error: sparse })).toThrow(/dense/);
  });

  it('bounds native Error prototype traversal without invoking proxy prototypes', () => {
    const error = new Error('deep');
    let prototype: object = Error.prototype;
    for (let index = 0; index <= MAX_WORKFLOW_TERMINAL_RECOVERY_VALUE_DEPTH; index++) {
      prototype = Object.create(prototype);
    }
    Object.setPrototypeOf(error, prototype);
    expect(() => materializeWorkflowTerminalCanonicalJson({ error })).toThrow(/Error prototype depth limit/);

    const prototypeTrap = vi.fn(() => Error.prototype);
    Object.setPrototypeOf(error, new Proxy({}, { getPrototypeOf: prototypeTrap }));
    expect(() => materializeWorkflowTerminalCanonicalJson({ error })).toThrow(/proxy prototypes/);
    expect(prototypeTrap).not.toHaveBeenCalled();
  });

  it('enforces envelope and Error stack byte limits', () => {
    expect(() =>
      materializeWorkflowTerminalCanonicalJson({
        value: 'x'.repeat(MAX_WORKFLOW_TERMINAL_RECOVERY_ENVELOPE_BYTES + 1),
      }),
    ).toThrow(/byte limit/);
    const error = new Error('boom');
    Object.defineProperty(error, 'stack', {
      configurable: true,
      value: 'x'.repeat(MAX_WORKFLOW_TERMINAL_RECOVERY_ERROR_STACK_BYTES + 1),
    });
    expect(() => materializeWorkflowTerminalCanonicalJson({ error })).toThrow(/byte limit/);
    expect(() =>
      materializeWorkflowTerminalCanonicalJson({
        value: '\u0001'.repeat(Math.floor(MAX_WORKFLOW_TERMINAL_RECOVERY_ENVELOPE_BYTES / 5)),
      }),
    ).toThrow(/byte limit/);
    const escapedMessage = new Error('\u0001'.repeat(Math.floor(MAX_WORKFLOW_TERMINAL_RECOVERY_ENVELOPE_BYTES / 5)));
    Object.defineProperty(escapedMessage, 'stack', { configurable: true, value: undefined });
    expect(() => materializeWorkflowTerminalCanonicalJson({ error: escapedMessage })).toThrow(/byte limit/);
  });

  it('rejects malformed adapter-destructive strings', () => {
    for (const value of ['a\0b', '\ud800', '\udfff']) {
      expect(() => materializeWorkflowTerminalCanonicalJson({ value })).toThrow(/null character|malformed Unicode/);
    }
    expect(materializeWorkflowTerminalCanonicalJson({ value: '😀' })).toEqual({ value: '😀' });
  });

  it('enforces the recursive depth boundary', () => {
    let atLimit: Record<string, unknown> = {};
    for (let index = 0; index < MAX_WORKFLOW_TERMINAL_RECOVERY_VALUE_DEPTH; index++) atLimit = { next: atLimit };
    expect(() => materializeWorkflowTerminalCanonicalJson(atLimit)).not.toThrow();
    expect(() => materializeWorkflowTerminalCanonicalJson({ next: atLimit })).toThrow(/depth limit/);
  });
});
