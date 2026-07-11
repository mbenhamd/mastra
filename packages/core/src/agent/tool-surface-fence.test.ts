import { describe, expect, it, vi } from 'vitest';

import { RequestContext } from '../request-context';
import {
  clearToolSurfaceFence,
  consumeToolSurfaceFenceRestore,
  enforceActiveToolsFence,
  enforceToolSurfaceFence,
  readToolSurfaceFence,
  stageToolSurfaceFenceRestore,
  stampToolSurfaceFence,
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
    enforceToolSurfaceFence(tools, allowed, logger);

    expect(tools).toEqual({ modeTool: originalModeTool });
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
    enforceToolSurfaceFence(tools, fence);

    expect(tools.modeTool.execute).toBe(originalExecute);
    expect(tools.modeTool.requireApproval).toBe(true);
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
});
