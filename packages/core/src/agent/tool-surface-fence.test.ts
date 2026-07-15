import { describe, expect, it, vi } from 'vitest';
import { z } from 'zod/v4';

import { RequestContext } from '../request-context';
import {
  captureSuspendedToolSurfaceFenceLease,
  claimToolSurfaceFence,
  clearSuspendedToolSurfaceFence,
  clearToolSurfaceFence,
  consumeToolSurfaceFenceRestore,
  createToolSurfaceFence,
  enforceActiveToolsFence,
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
