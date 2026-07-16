import { describe, expect, it, vi } from 'vitest';
import { z } from 'zod/v4';

import { RequestContext } from '../request-context';
import { wrapToolsWithHooks } from '../tools/tool-hooks';
import type { CoreTool } from '../tools/types';
import {
  captureSuspendedToolSurfaceFenceLease,
  claimToolSurfaceFence,
  clearSuspendedToolSurfaceFence,
  clearToolSurfaceFence,
  consumeToolSurfaceFenceRestore,
  createProcessorToolSurfaceView,
  createToolSurfaceFence,
  enforceActiveToolsFence,
  enforceReconstructibleToolSurface,
  enforceToolChoiceFence,
  enforceToolSurfaceFence,
  readToolSurfaceFence,
  stageToolSurfaceFenceRestore,
  stampToolSurfaceFence,
  suspendToolSurfaceFence,
  transferSuspendedToolSurfaceFence,
} from './tool-surface-fence';

describe('replacement tool surface fence', () => {
  it('persists a defensive name set and strips processor-added tools', () => {
    const requestContext = new RequestContext();
    const originalModeTool = {};
    const allowed = stampToolSurfaceFence(requestContext, 'run-1', { modeTool: originalModeTool });
    const tools = { modeTool: {}, injectedTool: {} };
    const logger = { warn: vi.fn() };

    expect(readToolSurfaceFence(requestContext, 'run-1')).toBe(allowed);
    expect(() => (allowed.allowedNames as string[]).push('injectedTool')).toThrow();
    const providerTools = enforceToolSurfaceFence(tools, allowed, logger);

    expect(providerTools).toEqual({ modeTool: originalModeTool });
    expect(providerTools).not.toBe(tools);
    expect(Object.getPrototypeOf(providerTools)).toBe(Object.prototype);
    expect(logger.warn).toHaveBeenCalledTimes(2);
  });

  it('uses the ceiling as the default activeTools set and prevents expansion', () => {
    const allowed = {
      allowedNames: ['modeTool', 'builtin'],
      originalTools: { modeTool: {}, builtin: {} },
      originalToolDescriptors: {},
    };

    expect(enforceActiveToolsFence(undefined, allowed)).toEqual(['modeTool', 'builtin']);
    expect(enforceActiveToolsFence(['modeTool', 'hidden'], allowed)).toEqual(['modeTool']);
  });

  it('rejects shorthand forced choices outside the replacement ceiling', () => {
    const fence = createToolSurfaceFence({ modeTool: {} });

    expect(() => enforceToolChoiceFence({ toolName: 'hiddenTool' }, fence)).toThrow(
      /outside the execution's replacement tool surface/,
    );
  });

  it('materializes processor toolChoice Proxies into a stable plain value', () => {
    const fence = createToolSurfaceFence({ modeTool: {} });
    const choiceTarget = { type: 'tool', toolName: 'modeTool' };
    const get = vi.fn(Reflect.get);
    const choice = new Proxy(choiceTarget, { get });

    const materialized = enforceToolChoiceFence(choice, fence);
    choiceTarget.toolName = 'hiddenTool';

    expect(materialized).toEqual({ type: 'tool', toolName: 'modeTool' });
    expect(Object.getPrototypeOf(materialized!)).toBe(Object.prototype);
    expect(get).toHaveBeenCalledTimes(2);
  });

  it('clears a prior execution fence when the RequestContext is reused', () => {
    const requestContext = new RequestContext();
    stampToolSurfaceFence(requestContext, 'run-1', { modeTool: {} });

    clearToolSurfaceFence(requestContext, 'run-1');

    expect(readToolSurfaceFence(requestContext, 'run-1')).toBeUndefined();
  });

  it('restores an allowed tool whose executable descriptor was mutated in place', () => {
    const requestContext = new RequestContext();
    const originalExecute = vi.fn();
    const injectedExecute = vi.fn();
    const modeTool = { execute: originalExecute, requireApproval: true };
    const fence = stampToolSurfaceFence(requestContext, 'run-1', { modeTool });

    modeTool.execute = injectedExecute;
    modeTool.requireApproval = false;
    const tools = { modeTool };
    const providerTools = enforceToolSurfaceFence(tools, fence);

    expect(providerTools.modeTool).toBe(modeTool);
    expect(modeTool.execute).toBe(originalExecute);
    expect(modeTool.requireApproval).toBe(true);
  });

  it('removes processor-added execution-control fields from an allowed tool', () => {
    const modeTool: Record<string, unknown> = { execute: vi.fn(), requireApproval: true };
    const fence = createToolSurfaceFence({ modeTool });

    modeTool.needsApproval = vi.fn(() => false);
    modeTool.hasSuspendSchema = false;
    modeTool.backgroundConfig = { enabled: false };
    enforceToolSurfaceFence({ modeTool }, fence);

    expect(modeTool).not.toHaveProperty('needsApproval');
    expect(modeTool).not.toHaveProperty('hasSuspendSchema');
    expect(modeTool).not.toHaveProperty('backgroundConfig');
    expect(modeTool.requireApproval).toBe(true);
  });

  it('restores nested schema and configuration state before materializing provider tools', () => {
    const requestContext = new RequestContext();
    const modeTool = {
      inputSchema: { shape: { query: { kind: 'string' } } },
      configuration: { approval: { required: true } },
    };
    const fence = stampToolSurfaceFence(requestContext, 'run-1', { modeTool });

    modeTool.inputSchema.shape.query.kind = 'number';
    modeTool.configuration.approval.required = false;
    const providerTools = enforceToolSurfaceFence({ modeTool }, fence);

    expect(providerTools).toEqual({ modeTool });
    expect(modeTool.inputSchema.shape.query.kind).toBe('string');
    expect(modeTool.configuration.approval.required).toBe(true);
  });

  it('restores mutable built-in state nested under protected tool configuration', () => {
    const config = {
      map: new Map([['mode', 'trusted']]),
      set: new Set(['trusted']),
      date: new Date('2026-01-01T00:00:00.000Z'),
      regexp: /trusted/giu,
      bytes: new Uint8Array([1, 2, 3]),
    };
    const modeTool = { config };
    const fence = createToolSurfaceFence({ modeTool });

    config.map.set('mode', 'mutated');
    config.set.delete('trusted');
    config.set.add('mutated');
    config.date.setUTCFullYear(2030);
    config.regexp.lastIndex = 4;
    config.bytes[1] = 9;
    enforceToolSurfaceFence({ modeTool }, fence);

    expect([...config.map]).toEqual([['mode', 'trusted']]);
    expect([...config.set]).toEqual(['trusted']);
    expect(config.date.toISOString()).toBe('2026-01-01T00:00:00.000Z');
    expect(config.regexp.lastIndex).toBe(0);
    expect([...config.bytes]).toEqual([1, 2, 3]);
  });

  it('rejects and restores a Map mutation that can change durable execution behavior', () => {
    const config = new Map([['mode', 'trusted']]);
    const modeTool = {
      config,
      execute: vi.fn(() => config.get('mode')),
    };
    const fence = createToolSurfaceFence({ modeTool });
    config.set('mode', 'mutated');

    expect(() => enforceReconstructibleToolSurface({ modeTool }, fence)).toThrow(
      /cannot mutate executable tool "modeTool"/,
    );
    expect(config.get('mode')).toBe('trusted');
    expect(modeTool.execute()).toBe('trusted');
  });

  it('restores RegExp.compile mutations to the exact retry baseline', () => {
    const regexp = /trusted/giu;
    regexp.lastIndex = 3;
    const modeTool = {
      config: regexp,
      execute: vi.fn(() => ({ source: regexp.source, flags: regexp.flags, lastIndex: regexp.lastIndex })),
    };
    const fence = createToolSurfaceFence({ modeTool });

    regexp.compile('mutated', 'm');
    regexp.lastIndex = 9;

    expect(() => enforceReconstructibleToolSurface({ modeTool }, fence)).toThrow(
      /cannot mutate executable tool "modeTool"/,
    );
    expect({ source: regexp.source, flags: regexp.flags, lastIndex: regexp.lastIndex }).toEqual({
      source: 'trusted',
      flags: 'giu',
      lastIndex: 3,
    });
    expect(() => enforceReconstructibleToolSurface({ modeTool }, fence)).not.toThrow();
    expect(modeTool.execute()).toEqual({ source: 'trusted', flags: 'giu', lastIndex: 3 });
  });

  it('restores direct executable state retained on a protected function object', () => {
    const execute = Object.assign(vi.fn(), { mode: 'trusted' });
    const modeTool = { execute };
    const fence = createToolSurfaceFence({ modeTool });

    execute.mode = 'mutated';

    expect(() => enforceReconstructibleToolSurface({ modeTool }, fence)).toThrow(
      /cannot mutate executable tool "modeTool"/,
    );
    expect(execute.mode).toBe('trusted');
  });

  it('captures function-owned invocation context by identity without recursively traversing it', () => {
    const lastContext = {
      requestContext: new RequestContext(),
      abortSignal: new AbortController().signal,
    };
    const execute = Object.assign(vi.fn(), {
      lastContext,
      mode: 'trusted',
    });
    const modeTool = { execute };

    const fence = createToolSurfaceFence({ modeTool });

    expect(() => enforceReconstructibleToolSurface({ modeTool }, fence)).not.toThrow();
    expect(execute.lastContext).toBe(lastContext);
    expect(execute.mode).toBe('trusted');
  });

  it('rejects a closure bypass that replaces object-valued state on the registered execute function', () => {
    const execute = Object.assign(vi.fn(), { policy: { mode: 'trusted' } });
    const modeTool = { execute };
    const trustedFence = createToolSurfaceFence({ modeTool });
    const processorView = createProcessorToolSurfaceView(trustedFence);
    const processorClosure = () => {
      execute.policy = { mode: 'poisoned' };
      return processorView.tools;
    };

    const processorTools = processorClosure();

    expect(() => enforceReconstructibleToolSurface(processorTools, trustedFence, processorView.fence)).toThrow(
      /cannot mutate executable tool "modeTool"/,
    );
    expect(execute.policy).toEqual({ mode: 'trusted' });
  });

  it('isolates irreversible processor mutations and gives the next retry a pristine baseline', () => {
    const modeTool = { execute: vi.fn(), config: { mode: 'trusted' } };
    const trustedFence = createToolSurfaceFence({ modeTool });
    const firstAttempt = createProcessorToolSurfaceView(trustedFence);

    Object.freeze(firstAttempt.tools.modeTool);

    expect(() => enforceReconstructibleToolSurface(firstAttempt.tools, trustedFence, firstAttempt.fence)).toThrow(
      /cannot mutate executable tool "modeTool"/,
    );
    expect(Object.isExtensible(modeTool)).toBe(true);
    expect(modeTool.config.mode).toBe('trusted');

    const retryAttempt = createProcessorToolSurfaceView(trustedFence);
    expect(enforceReconstructibleToolSurface(retryAttempt.tools, trustedFence, retryAttempt.fence)).toEqual({
      modeTool,
    });
  });

  it('keeps private class state off the processor view', () => {
    class PrivateConfiguration {
      #mode = 'trusted';

      setMode(mode: string) {
        this.#mode = mode;
      }

      readMode() {
        return this.#mode;
      }
    }
    const configuration = new PrivateConfiguration();
    const modeTool = { config: configuration };
    const trustedFence = createToolSurfaceFence({ modeTool });
    const processorView = createProcessorToolSurfaceView(trustedFence);
    const processorConfiguration = (processorView.tools.modeTool as typeof modeTool).config;

    expect(() => processorConfiguration.setMode('mutated')).toThrow(/private member/);
    expect(configuration.readMode()).toBe('trusted');
    expect(enforceReconstructibleToolSurface(processorView.tools, trustedFence, processorView.fence)).toEqual({
      modeTool,
    });
  });

  it('fails closed while capturing protected built-ins whose internal state cannot be restored safely', () => {
    expect(() => createToolSurfaceFence({ modeTool: { config: new WeakMap() } })).toThrow(
      /cannot be snapshotted safely/,
    );
    expect(() => createToolSurfaceFence({ modeTool: { config: Promise.resolve('pending') } })).toThrow(
      /cannot be snapshotted safely/,
    );
    expect(() => createToolSurfaceFence({ modeTool: { config: new WeakRef({}) } })).toThrow(
      /internal-slot executable state cannot be snapshotted safely/,
    );
    expect(() => createToolSurfaceFence({ modeTool: { config: new URLSearchParams('mode=trusted') } })).toThrow(
      /internal-slot executable state cannot be snapshotted safely/,
    );
    if (typeof SharedArrayBuffer !== 'undefined') {
      expect(() => createToolSurfaceFence({ modeTool: { config: new SharedArrayBuffer(4) } })).toThrow(
        /cannot be retained safely/,
      );
    }
  });

  it('continues to snapshot ordinary instances of user-defined classes', () => {
    class ToolConfiguration {
      mode = 'trusted';
    }
    const config = new ToolConfiguration();
    const modeTool = { config };
    const fence = createToolSurfaceFence({ modeTool });

    config.mode = 'mutated';
    enforceToolSurfaceFence({ modeTool }, fence);

    expect(config.mode).toBe('trusted');
  });

  it('treats Symbol.toStringTag on an ordinary class as cosmetic rather than an opaque brand', () => {
    class TaggedConfiguration {
      mode = 'trusted';

      get [Symbol.toStringTag]() {
        return 'PapersFlowToolConfiguration';
      }
    }
    const config = new TaggedConfiguration();
    const modeTool = { config };
    const fence = createToolSurfaceFence({ modeTool });

    config.mode = 'mutated';
    enforceToolSurfaceFence({ modeTool }, fence);

    expect(config.mode).toBe('trusted');
  });

  it('preserves mutable runtime state that is not part of the tool definition', () => {
    const modeTool = {
      execute: vi.fn(),
      runtimeCount: 0,
      runtimeState: { cacheHits: 0 },
    };
    const fence = createToolSurfaceFence({ modeTool });

    modeTool.runtimeCount++;
    modeTool.runtimeState.cacheHits++;
    enforceToolSurfaceFence({ modeTool }, fence);

    expect(modeTool.runtimeCount).toBe(1);
    expect(modeTool.runtimeState.cacheHits).toBe(1);
  });

  it('restores swapped prototypes on replacement tool state', () => {
    const originalExecute = vi.fn();
    const injectedExecute = vi.fn();
    const originalPrototype = { execute: originalExecute };
    const modeTool = Object.create(originalPrototype) as { execute: typeof originalExecute };
    const fence = createToolSurfaceFence({ modeTool });

    Object.setPrototypeOf(modeTool, { execute: injectedExecute });
    const providerTools = enforceToolSurfaceFence({ modeTool }, fence);

    expect(providerTools.modeTool).toBe(modeTool);
    expect(Object.getPrototypeOf(modeTool)).toBe(originalPrototype);
    expect(modeTool.execute).toBe(originalExecute);
  });

  it('restores protected behavior mutated through the retained prototype chain', () => {
    const originalExecute = vi.fn();
    const injectedExecute = vi.fn();
    const originalPrototype = { execute: originalExecute };
    const modeTool = Object.create(originalPrototype) as { execute: typeof originalExecute };
    const fence = createToolSurfaceFence({ modeTool });

    originalPrototype.execute = injectedExecute;
    enforceToolSurfaceFence({ modeTool }, fence);

    expect(Object.getPrototypeOf(modeTool)).toBe(originalPrototype);
    expect(modeTool.execute).toBe(originalExecute);
  });

  it("restores a hook wrapper's retained receiver and prototype before execution", async () => {
    class ReceiverSensitiveTool {
      readonly parameters = z.object({});
      readonly #secret = 'private';

      get configuration() {
        return { prefix: 'trusted' };
      }

      async execute() {
        return `${this.configuration.prefix}:${this.#secret}`;
      }
    }

    const receiver = new ReceiverSensitiveTool();
    const receiverPrototype = ReceiverSensitiveTool.prototype;
    const originalConfiguration = Object.getOwnPropertyDescriptor(receiverPrototype, 'configuration')!;
    const wrappedTools = wrapToolsWithHooks(
      { modeTool: receiver as unknown as CoreTool },
      { beforeToolCall: vi.fn() },
      { agentId: 'agent-id', agentName: 'Agent name' },
    );
    const fence = createToolSurfaceFence(wrappedTools);

    try {
      Object.defineProperty(receiverPrototype, 'configuration', {
        ...originalConfiguration,
        get: () => ({ prefix: 'mutated' }),
      });

      const restoredTools = enforceToolSurfaceFence(wrappedTools, fence) as Record<string, CoreTool>;

      await expect(restoredTools.modeTool!.execute?.({}, {} as any)).resolves.toBe('trusted:private');
      expect(Object.getOwnPropertyDescriptor(receiverPrototype, 'configuration')).toEqual(originalConfiguration);
    } finally {
      Object.defineProperty(receiverPrototype, 'configuration', originalConfiguration);
    }
  });

  it('restores mutable state returned by an enumerable protected accessor', () => {
    const properties = { query: { kind: 'string' } };
    const parameters = Object.defineProperty({}, 'properties', {
      enumerable: true,
      configurable: true,
      get: () => properties,
    }) as { properties: typeof properties };
    const modeTool = { parameters };
    const fence = createToolSurfaceFence({ modeTool });

    properties.query.kind = 'number';
    enforceToolSurfaceFence({ modeTool }, fence);

    expect(modeTool.parameters.properties.query.kind).toBe('string');
  });

  it('accepts a fresh descriptor-equivalent view from a protected accessor', () => {
    const query = { kind: 'string' };
    const parameters = Object.defineProperty({}, 'properties', {
      enumerable: true,
      configurable: true,
      get: () => ({ query }),
    }) as { properties: { query: typeof query } };
    const modeTool = { parameters };
    const fence = createToolSurfaceFence({ modeTool });

    expect(() => enforceToolSurfaceFence({ modeTool }, fence)).not.toThrow();
    expect(modeTool.parameters.properties.query).toBe(query);
  });

  it('retains a generated Zod definition without traversing library-internal state', () => {
    const shape = Object.fromEntries(
      Array.from({ length: 100 }, (_, index) => [
        `field${index}`,
        z.object({ value: z.string(), confidence: z.number().optional() }),
      ]),
    );
    const modeTool = { inputSchema: z.object(shape) };
    const fence = createToolSurfaceFence({ modeTool });

    expect(() => enforceToolSurfaceFence({ modeTool }, fence)).not.toThrow();
  });

  it('accepts a recursive Zod definition with fresh lazy shape accessors', () => {
    let category: z.ZodType;
    category = z.object({
      name: z.string(),
      get subcategories() {
        return z.array(category);
      },
    });
    const modeTool = { inputSchema: category };
    const fence = createToolSurfaceFence({ modeTool });

    expect(() => enforceToolSurfaceFence({ modeTool }, fence)).not.toThrow();
  });

  it('does not rewrite shared intrinsic prototypes during enforcement', () => {
    const modeTool = { inputSchema: { type: 'object' } };
    const fence = createToolSurfaceFence({ modeTool });
    Object.defineProperty(Object.prototype, '__toolSurfaceFenceTest__', { value: true, configurable: true });

    try {
      enforceToolSurfaceFence({ modeTool }, fence);
      expect(Object.hasOwn(Object.prototype, '__toolSurfaceFenceTest__')).toBe(true);
    } finally {
      Reflect.deleteProperty(Object.prototype, '__toolSurfaceFenceTest__');
    }
  });

  it('fails closed when a protected accessor returns a different identity', () => {
    let properties = { query: { kind: 'string' } };
    const parameters = Object.defineProperty({}, 'properties', {
      enumerable: true,
      configurable: true,
      get: () => properties,
    });
    const modeTool = { parameters };
    const fence = createToolSurfaceFence({ modeTool });

    properties = { query: { kind: 'number' } };

    expect(() => enforceToolSurfaceFence({ modeTool }, fence)).toThrow(
      /Cannot restore replacement tool "modeTool" after a processor mutated its executable descriptor/,
    );
  });

  it('reads the complete processor surface before restoring any retained implementation', () => {
    const originalExecute = vi.fn();
    const injectedExecute = vi.fn();
    const firstTool = { execute: originalExecute };
    const secondTool = {};
    const fence = createToolSurfaceFence({ firstTool, secondTool });
    const processorTools = new Proxy<Record<string, unknown>>(
      {},
      {
        ownKeys: () => ['firstTool', 'secondTool'],
        getOwnPropertyDescriptor: () => ({ enumerable: true, configurable: true }),
        get: (_target, key) => {
          if (key === 'secondTool') firstTool.execute = injectedExecute;
          return key === 'firstTool' ? firstTool : secondTool;
        },
      },
    );

    const providerTools = enforceToolSurfaceFence(processorTools, fence);

    expect(providerTools).toEqual({ firstTool, secondTool });
    expect(firstTool.execute).toBe(originalExecute);
  });

  it('materializes a stable plain snapshot from a stateful processor tool container', () => {
    const modeTool = {};
    const injectedTool = {};
    const fence = createToolSurfaceFence({ modeTool });
    let surface: Record<string, unknown> = { modeTool };
    const processorTools = new Proxy<Record<string, unknown>>(
      {},
      {
        ownKeys: () => Reflect.ownKeys(surface),
        getOwnPropertyDescriptor: (_target, key) => Object.getOwnPropertyDescriptor(surface, key),
        get: (_target, key) => Reflect.get(surface, key),
      },
    );

    const providerTools = enforceToolSurfaceFence(processorTools, fence);
    surface = { injectedTool };

    expect(providerTools).toEqual({ modeTool });
    expect(Object.getPrototypeOf(providerTools)).toBe(Object.prototype);
    expect(providerTools).not.toBe(processorTools);
  });

  it('rejects a stateful processor Proxy without leaving its trapped mutation behind', () => {
    const originalExecute = vi.fn();
    const injectedExecute = vi.fn();
    const modeTool = { execute: originalExecute };
    const fence = createToolSurfaceFence({ modeTool });
    let enumeration = 0;
    const ownKeys = vi.fn(() => {
      enumeration++;
      if (enumeration === 1) return ['modeTool'];
      modeTool.execute = injectedExecute;
      return [];
    });
    const getOwnPropertyDescriptor = vi.fn(() => {
      modeTool.execute = injectedExecute;
      return { value: modeTool, writable: true, enumerable: true, configurable: true };
    });
    const processorTools = new Proxy<Record<string, unknown>>({}, { ownKeys, getOwnPropertyDescriptor });

    expect(() => enforceReconstructibleToolSurface(processorTools, fence)).toThrow(
      /cannot mutate executable tool "modeTool"/,
    );
    expect(modeTool.execute).toBe(originalExecute);
    expect(ownKeys).toHaveBeenCalledOnce();
    expect(getOwnPropertyDescriptor).toHaveBeenCalledOnce();
  });

  it('rejects a persisted name ceiling without every concrete own implementation', () => {
    expect(() => createToolSurfaceFence({ modeTool: {} }, ['modeTool', 'missingTool'])).toThrow(
      /allowed tool "missingTool" has no own concrete implementation/,
    );
    expect(() => createToolSurfaceFence(Object.create({ inheritedTool: {} }), ['inheritedTool'])).toThrow(
      /allowed tool "inheritedTool" has no own concrete implementation/,
    );
    expect(() => createToolSurfaceFence({ missingTool: undefined }, ['missingTool'])).toThrow(
      /allowed tool "missingTool" has no own concrete implementation/,
    );
  });

  it('consumes a staged resume ceiling exactly once', () => {
    const requestContext = new RequestContext();
    stageToolSurfaceFenceRestore(requestContext, 'run-1', ['modeTool']);

    expect([...consumeToolSurfaceFenceRestore(requestContext, 'run-1')!]).toEqual(['modeTool']);
    expect(consumeToolSurfaceFenceRestore(requestContext, 'run-1')).toBeUndefined();
  });

  it('bounds abandoned suspended fences on a caller-retained RequestContext', () => {
    const requestContext = new RequestContext();
    for (let index = 0; index < 64; index++) {
      stampToolSurfaceFence(requestContext, `run-${index}`, { modeTool: {} });
    }

    expect(() => stampToolSurfaceFence(requestContext, 'run-overflow', { modeTool: {} })).toThrow(
      /64 active or suspended runs/,
    );
  });

  it('rejects a duplicate active run id instead of silently replacing its fence', () => {
    const requestContext = new RequestContext();
    const originalFence = stampToolSurfaceFence(requestContext, 'run-1', { originalTool: {} });

    expect(() => stampToolSurfaceFence(requestContext, 'run-1', { replacementTool: {} })).toThrow(
      /Cannot replace the retained tool surface for active run run-1/,
    );
    expect(readToolSurfaceFence(requestContext, 'run-1')).toBe(originalFence);
  });

  it('leases an active fence so colliding and stale executions cannot replace or clear it', () => {
    const requestContext = new RequestContext();
    const first = stampToolSurfaceFence(requestContext, 'shared-run', { modeTool: {} }, 'owner-a');

    expect(() => stampToolSurfaceFence(requestContext, 'shared-run', { hiddenTool: {} }, 'owner-b')).toThrow(
      /concurrent execution/,
    );
    expect(clearToolSurfaceFence(requestContext, 'shared-run', 'owner-b')).toBe(false);
    expect(readToolSurfaceFence(requestContext, 'shared-run')).toBe(first);

    suspendToolSurfaceFence(requestContext, 'shared-run', 'owner-a');
    expect(claimToolSurfaceFence(requestContext, 'shared-run', 'owner-b')).toBe(first);
    expect(clearToolSurfaceFence(requestContext, 'shared-run', 'owner-a')).toBe(false);
    expect(clearToolSurfaceFence(requestContext, 'shared-run', 'owner-b')).toBe(true);
    expect(readToolSurfaceFence(requestContext, 'shared-run')).toBeUndefined();
  });

  it('does not clear a newer suspension after a resume execution claims and re-suspends it', () => {
    const requestContext = new RequestContext();
    const fence = stampToolSurfaceFence(requestContext, 'shared-run', { modeTool: {} }, 'suspend-owner');

    suspendToolSurfaceFence(requestContext, 'shared-run', 'suspend-owner');
    const staleLease = captureSuspendedToolSurfaceFenceLease(requestContext, 'shared-run')!;
    expect(claimToolSurfaceFence(requestContext, 'shared-run', 'resume-owner')).toBe(fence);
    suspendToolSurfaceFence(requestContext, 'shared-run', 'resume-owner');

    expect(clearSuspendedToolSurfaceFence(requestContext, 'shared-run', staleLease)).toBe(false);
    expect(readToolSurfaceFence(requestContext, 'shared-run')).toBe(fence);
    const currentLease = captureSuspendedToolSurfaceFenceLease(requestContext, 'shared-run')!;
    expect(clearSuspendedToolSurfaceFence(requestContext, 'shared-run', currentLease)).toBe(true);
  });

  it('moves the exact suspended fence to a defensively snapshotted request context', () => {
    const sourceContext = new RequestContext();
    const targetContext = new RequestContext();
    const fence = stampToolSurfaceFence(sourceContext, 'shared-run', { modeTool: {} }, 'suspend-owner');
    suspendToolSurfaceFence(sourceContext, 'shared-run', 'suspend-owner');
    const lease = captureSuspendedToolSurfaceFenceLease(sourceContext, 'shared-run')!;

    expect(transferSuspendedToolSurfaceFence(sourceContext, targetContext, 'shared-run', lease)).toBe(true);
    expect(readToolSurfaceFence(sourceContext, 'shared-run')).toBeUndefined();
    expect(claimToolSurfaceFence(targetContext, 'shared-run', 'resume-owner')).toBe(fence);
    expect(transferSuspendedToolSurfaceFence(sourceContext, targetContext, 'shared-run', lease)).toBe(false);
  });
});
