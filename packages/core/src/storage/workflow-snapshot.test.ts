import { describe, expect, it } from 'vitest';

import { createEmptyWorkflowSnapshot, mergeWorkflowStepResult } from './workflow-snapshot';

describe('mergeWorkflowStepResult', () => {
  it('merges forEach array outputs without clobbering completed iterations', () => {
    const snapshot = createEmptyWorkflowSnapshot('run-1');
    snapshot.context.foreach = {
      status: 'success',
      output: ['done', null, 'tail'],
      payload: ['a', 'b', 'c'],
      startedAt: 1,
    } as any;
    snapshot.requestContext = { existing: true };

    const context = mergeWorkflowStepResult({
      snapshot,
      stepId: 'foreach',
      result: {
        status: 'success',
        output: [null, 'resumed', null],
        payload: ['a', 'b', 'c'],
        startedAt: 2,
        endedAt: 3,
      } as any,
      requestContext: { incoming: true },
    });

    expect(context.foreach).toEqual({
      status: 'success',
      output: ['done', 'resumed', 'tail'],
      payload: ['a', 'b', 'c'],
      startedAt: 2,
      endedAt: 3,
    });
    expect(snapshot.requestContext).toEqual({ existing: true, incoming: true });
  });

  it('keeps existing values for null updates and fills trailing nulls without sparse arrays', () => {
    const snapshot = createEmptyWorkflowSnapshot('run-1');
    snapshot.context.foreach = {
      status: 'success',
      output: [1, 2],
    } as any;
    const output = Array(3);
    output[1] = 3;

    mergeWorkflowStepResult({
      snapshot,
      stepId: 'foreach',
      result: {
        status: 'success',
        output,
      } as any,
      requestContext: {},
    });

    expect(snapshot.context.foreach?.output).toEqual([1, 3, null]);
    expect(2 in (snapshot.context.foreach?.output as unknown[])).toBe(true);
  });

  it('merges array-form foreach progress without losing completed coordinates to stale holes or nulls', () => {
    const snapshot = createEmptyWorkflowSnapshot('run-1');
    const failed = { status: 'failed', error: 'child failed', endedAt: 2 };
    const succeeded = { status: 'success', output: 'sibling' };
    const earlier = { status: 'success', output: 'earlier' };
    const existingProgress = Array(4);
    existingProgress[0] = failed;
    existingProgress[2] = earlier;
    snapshot.context.foreach = {
      status: 'failed',
      output: [failed, null, 'earlier', null],
      suspendPayload: { __workflow_meta: { foreachOutput: existingProgress } },
    } as any;
    const incomingProgress = Array(4);
    incomingProgress[1] = succeeded;
    incomingProgress[2] = null;

    const context = mergeWorkflowStepResult({
      snapshot,
      stepId: 'foreach',
      result: {
        status: 'success',
        output: [null, 'sibling', null, null],
        suspendPayload: { __workflow_meta: { foreachOutput: incomingProgress } },
      } as any,
      requestContext: {},
    });

    const progress = (snapshot.context.foreach as any).suspendPayload.__workflow_meta.foreachOutput;
    expect(Array.isArray(progress)).toBe(true);
    expect(progress.length).toBe(4);
    expect(Object.hasOwn(progress, 3)).toBe(false);
    expect(context.foreach.output).toEqual([failed, 'sibling', 'earlier', null]);
    expect((context.foreach as any).suspendPayload.__workflow_meta.foreachOutput).toEqual([
      failed,
      succeeded,
      earlier,
      null,
    ]);
  });

  it('removes completed coordinates from a propagated sparse suspension map', () => {
    const snapshot = createEmptyWorkflowSnapshot('run-1');
    const first = { status: 'suspended', suspendPayload: { toolCallId: 'first' } };
    const second = { status: 'suspended', suspendPayload: { toolCallId: 'second' } };
    snapshot.context.foreach = {
      status: 'suspended',
      output: [first, second],
      suspendPayload: { __workflow_meta: { foreachOutput: { 0: first, 1: second } } },
    } as any;

    const context = mergeWorkflowStepResult({
      snapshot,
      stepId: 'foreach',
      result: {
        status: 'suspended',
        output: ['approved-first', second],
        suspendPayload: {
          __workflow_meta: {
            foreachOutput: { 1: second },
            resumeLabels: { second: { stepId: 'foreach', foreachIndex: 1 } },
          },
        },
      } as any,
      requestContext: {},
    });

    expect(context.foreach.output).toEqual(['approved-first', second]);
    expect((context.foreach as any).suspendPayload.__workflow_meta).toEqual({
      foreachOutput: { 1: second },
      resumeLabels: { second: { stepId: 'foreach', foreachIndex: 1 } },
    });
  });

  it('keeps recovered progress when a stale pending reset carries copied failures', () => {
    const snapshot = createEmptyWorkflowSnapshot('run-1');
    const failed = { status: 'failed', error: 'old failure', endedAt: 2 };
    const succeeded = { status: 'success', output: 'recovered', endedAt: 3 };
    snapshot.context.foreach = {
      status: 'success',
      output: ['recovered', failed],
      suspendPayload: { __workflow_meta: { foreachOutput: [succeeded, failed] } },
    } as any;

    const context = mergeWorkflowStepResult({
      snapshot,
      stepId: 'foreach',
      result: {
        status: 'running',
        output: [failed, { __mastra_pending__: true }],
        suspendPayload: { __workflow_meta: { foreachOutput: [failed, failed] } },
      } as any,
      requestContext: {},
    });

    expect(context.foreach.output).toEqual(['recovered', null]);
    expect((context.foreach as any).suspendPayload.__workflow_meta.foreachOutput).toEqual([succeeded, failed]);
  });

  it('persists a fresh failed coordinate without overwriting a successful sibling', () => {
    const snapshot = createEmptyWorkflowSnapshot('run-1');
    const succeeded = { status: 'success', output: 'previous success' };
    const failed = { status: 'failed', error: 'new attempt failed', endedAt: 3 };
    snapshot.context.foreach = {
      status: 'success',
      output: ['previous success', 'sibling success'],
      suspendPayload: {
        __workflow_meta: { foreachOutput: [succeeded, { status: 'success', output: 'sibling success' }] },
      },
    } as any;

    const context = mergeWorkflowStepResult({
      snapshot,
      stepId: 'foreach',
      result: {
        status: 'failed',
        output: [failed, null],
        suspendPayload: { __workflow_meta: { foreachOutput: { 0: failed } } },
      } as any,
      requestContext: {},
    });

    expect(context.foreach.status).toBe('failed');
    expect(context.foreach.output).toEqual([failed, 'sibling success']);
    expect((context.foreach as any).suspendPayload.__workflow_meta.foreachOutput).toEqual([
      failed,
      { status: 'success', output: 'sibling success' },
    ]);
  });

  it('accepts a suspension from an admitted failed retry while protecting a completed sibling', () => {
    const snapshot = createEmptyWorkflowSnapshot('run-1');
    const succeeded = { status: 'success', output: 'sibling success' };
    const failed = { status: 'failed', error: 'previous attempt failed' };
    const staleSuspension = { status: 'suspended', suspendPayload: { token: 'stale sibling' } };
    const freshSuspension = { status: 'suspended', suspendPayload: { token: 'retry approval' } };
    snapshot.context.foreach = {
      status: 'failed',
      output: ['sibling success', failed],
      suspendPayload: { __workflow_meta: { foreachOutput: [succeeded, failed] } },
    } as any;

    mergeWorkflowStepResult({
      snapshot,
      stepId: 'foreach',
      result: { status: 'running', output: [null, { __mastra_pending__: true }] } as any,
      requestContext: {},
    });
    expect(snapshot.context.foreach?.output).toEqual(['sibling success', null]);

    const context = mergeWorkflowStepResult({
      snapshot,
      stepId: 'foreach',
      result: {
        status: 'suspended',
        output: [null, freshSuspension],
        suspendPayload: { __workflow_meta: { foreachOutput: { 0: staleSuspension, 1: freshSuspension } } },
      } as any,
      requestContext: {},
    });

    expect(context.foreach.output).toEqual(['sibling success', freshSuspension]);
    expect((context.foreach as any).suspendPayload.__workflow_meta.foreachOutput).toEqual([succeeded, freshSuspension]);
  });

  it('applies pending marker resets without trusting stale sibling values or status', () => {
    const snapshot = createEmptyWorkflowSnapshot('run-1');
    const failed = { status: 'failed', error: 'engine failure', endedAt: 2 };
    const suspendPayload = {
      __workflow_meta: { foreachOutput: { 15: failed, 16: { status: 'success', output: failed } } },
    };
    snapshot.context.foreach = {
      status: 'success',
      startedAt: 1,
      endedAt: 2,
      suspendPayload,
      output: [
        { status: 'suspended', startedAt: 1, suspendedAt: 2, suspendPayload: { __workflow_meta: {} } },
        {
          status: 'suspended',
          payload: 'payload',
          suspendedAt: 3,
          suspendPayload: { token: 'tok', __workflow_meta: {} },
        },
        { status: 'suspended', suspendPayload: { token: 'tok' }, suspendedAt: 4 },
        { status: 'suspended', startedAt: 5, suspendedAt: 6 },
        { status: 'success', output: 'done-4' },
        { status: 'failed', error: 'failed-5' },
        { status: 'waiting' },
        { status: 'suspended', output: 'user-data' },
        { __mastra_pending__: true },
        { status: 'success', output: 'newer-tail' },
        { status: 'suspended', payload: { type: 'user-status' } },
        { status: 'suspended', startedAt: 10 },
        { __mastra_foreach_queued__: true },
        { __mastra_foreach_queued__: true, value: 'user-data' },
        { status: 'failed', message: 'domain result' },
        failed,
        failed,
      ],
    } as any;
    snapshot.requestContext = { existing: true, shared: 'old' };

    mergeWorkflowStepResult({
      snapshot,
      stepId: 'foreach',
      result: {
        status: 'running',
        startedAt: 3,
        output: [
          { __mastra_pending__: true },
          { __mastra_pending__: true },
          { __mastra_pending__: true },
          { __mastra_pending__: true },
          { __mastra_pending__: true },
          { status: 'suspended', startedAt: 8, suspendedAt: 9 },
          { __mastra_pending__: true },
          { __mastra_pending__: true },
          { __mastra_pending__: true },
          { __mastra_pending__: true },
          { __mastra_pending__: true },
          { __mastra_pending__: true },
          { __mastra_pending__: true },
          { __mastra_pending__: true },
          { __mastra_pending__: true },
          { __mastra_pending__: true },
          { __mastra_pending__: true },
        ],
      } as any,
      requestContext: { incoming: true, shared: 'new' },
    });

    expect(snapshot.context.foreach).toEqual({
      status: 'success',
      startedAt: 1,
      endedAt: 2,
      suspendPayload,
      output: [
        null,
        null,
        null,
        null,
        { status: 'success', output: 'done-4' },
        { status: 'failed', error: 'failed-5' },
        { status: 'waiting' },
        { status: 'suspended', output: 'user-data' },
        null,
        { status: 'success', output: 'newer-tail' },
        { status: 'suspended', payload: { type: 'user-status' } },
        { status: 'suspended', startedAt: 10 },
        null,
        { __mastra_foreach_queued__: true, value: 'user-data' },
        { status: 'failed', message: 'domain result' },
        null,
        failed,
      ],
    });
    expect(snapshot.requestContext).toEqual({ existing: true, incoming: true, shared: 'new' });
  });

  it('ignores fresh-looking sibling values in pending marker reset writes', () => {
    const snapshot = createEmptyWorkflowSnapshot('run-1');
    snapshot.context.foreach = {
      status: 'success',
      startedAt: 1,
      endedAt: 2,
      output: [{ status: 'suspended', startedAt: 1, suspendedAt: 2, suspendPayload: { __workflow_meta: {} } }],
    } as any;

    const context = mergeWorkflowStepResult({
      snapshot,
      stepId: 'foreach',
      result: {
        status: 'running',
        startedAt: 3,
        output: [{ __mastra_pending__: true }, { status: 'success', output: 'stale-new-value' }],
      } as any,
      requestContext: {},
    });

    expect(context.foreach).toEqual({
      status: 'success',
      startedAt: 1,
      endedAt: 2,
      output: [null, null],
    });
  });

  it('does not treat user values with pending-like fields as internal markers', () => {
    const snapshot = createEmptyWorkflowSnapshot('run-1');
    snapshot.context.foreach = {
      status: 'success',
      output: [null],
    } as any;

    const context = mergeWorkflowStepResult({
      snapshot,
      stepId: 'foreach',
      result: {
        status: 'success',
        output: [{ __mastra_pending__: true, value: 'user-data' }],
      } as any,
      requestContext: {},
    });

    expect(context.foreach.output).toEqual([{ __mastra_pending__: true, value: 'user-data' }]);
  });

  it('stores __proto__ as an own step result without mutating the context prototype', () => {
    const snapshot = createEmptyWorkflowSnapshot('run-1');
    const prototype = Object.getPrototypeOf(snapshot.context);
    const result = { status: 'running', payload: { safe: true } } as any;

    const context = mergeWorkflowStepResult({
      snapshot,
      stepId: '__proto__',
      result,
      requestContext: {},
    });

    expect(Object.getPrototypeOf(snapshot.context)).toBe(prototype);
    expect(Object.hasOwn(snapshot.context, '__proto__')).toBe(true);
    expect(Object.getOwnPropertyDescriptor(snapshot.context, '__proto__')?.value).toEqual(result);
    expect(Object.hasOwn(context, '__proto__')).toBe(true);
  });
});
